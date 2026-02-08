"""
Legal documents loader - loads PDF files into RAG system on startup.

Runs as a background task during backend startup.
Skips documents that are already loaded (idempotent).
"""
import logging
import asyncio
from pathlib import Path
from typing import List, Optional

import asyncpg
import google.generativeai as genai

from ..config import settings

logger = logging.getLogger(__name__)

# Path to legal_docs folder (relative to backend root)
LEGAL_DOCS_DIR = Path(__file__).parent.parent.parent / "legal_docs"

# Chunk settings
CHUNK_SIZE = 500  # words
CHUNK_OVERLAP = 50  # words overlap between chunks


def _extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract all text from PDF file."""
    from pypdf import PdfReader

    reader = PdfReader(pdf_path)
    text_parts = []

    for page_num, page in enumerate(reader.pages, 1):
        text = page.extract_text()
        if text:
            text_parts.append(f"[Страница {page_num}]\n{text}")

    return "\n\n".join(text_parts)


def _chunk_text(text: str) -> list[dict]:
    """Split text into overlapping chunks."""
    words = text.split()
    chunks = []

    start = 0
    chunk_num = 1

    while start < len(words):
        end = start + CHUNK_SIZE
        chunk_words = words[start:end]
        chunk_text = " ".join(chunk_words)

        # Try to find article/section markers
        article_match = None
        for marker in ["Статья", "Глава", "Раздел", "Пункт"]:
            if marker in chunk_text:
                idx = chunk_text.find(marker)
                snippet = chunk_text[idx:idx + 50]
                article_match = snippet.split("\n")[0].strip()
                break

        chunks.append({
            "chunk_num": chunk_num,
            "text": chunk_text,
            "word_count": len(chunk_words),
            "article_reference": article_match,
        })

        start = end - CHUNK_OVERLAP
        chunk_num += 1

    return chunks


async def _get_embedding(text: str) -> list[float]:
    """Get embedding from Gemini API."""
    result = genai.embed_content(
        model=f"models/{settings.gemini_embedding_model}",
        content=text,
        task_type="retrieval_document",
    )
    return result["embedding"]


async def _process_pdf(pdf_path: Path, conn: asyncpg.Connection, has_embedding: bool) -> dict:
    """Process single PDF and insert into database."""
    filename = pdf_path.name

    # Extract text
    full_text = _extract_text_from_pdf(pdf_path)
    word_count = len(full_text.split())

    if word_count < 50:
        logger.warning("[LEGAL_DOCS] %s: too short (%d words), skipping", filename, word_count)
        return {"filename": filename, "status": "skipped", "chunks": 0}

    # Check if already exists
    existing = await conn.fetchval(
        "SELECT id FROM legal_documents WHERE title = $1", filename
    )
    if existing:
        logger.info("[LEGAL_DOCS] %s: already loaded, skipping", filename)
        return {"filename": filename, "status": "skipped", "chunks": 0}

    # Insert main document
    doc_id = await conn.fetchval(
        """
        INSERT INTO legal_documents (title, document_type, full_text, source_url)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        filename,
        "Законодательство РК",
        full_text,
        f"file://{pdf_path}",
    )

    # Chunk text
    chunks = _chunk_text(full_text)
    logger.info("[LEGAL_DOCS] %s: %d words, %d chunks", filename, word_count, len(chunks))

    # Process chunks
    for i, chunk in enumerate(chunks):
        if has_embedding:
            embedding = await _get_embedding(chunk["text"])
            await conn.execute(
                """
                INSERT INTO legal_articles (document_id, article_number, title, content, embedding)
                VALUES ($1, $2, $3, $4, $5)
                """,
                doc_id,
                str(chunk["chunk_num"]),
                chunk["article_reference"] or f"Чанк {chunk['chunk_num']}",
                chunk["text"],
                embedding,
            )
        else:
            await conn.execute(
                """
                INSERT INTO legal_articles (document_id, article_number, title, content)
                VALUES ($1, $2, $3, $4)
                """,
                doc_id,
                str(chunk["chunk_num"]),
                chunk["article_reference"] or f"Чанк {chunk['chunk_num']}",
                chunk["text"],
            )

        # Rate limiting for Gemini embeddings API
        if has_embedding and (i + 1) % 5 == 0:
            await asyncio.sleep(0.2)

    return {"filename": filename, "status": "success", "chunks": len(chunks)}


async def load_legal_docs_background(pool: asyncpg.Pool):
    """
    Load legal documents into RAG system.

    Runs as background task on startup. Idempotent - skips already loaded docs.
    """
    try:
        # Check if Gemini is configured
        if not settings.gemini_api_key:
            logger.warning("[LEGAL_DOCS] Gemini API key not configured, skipping")
            return

        # Check if legal_docs folder exists
        if not LEGAL_DOCS_DIR.exists():
            logger.info("[LEGAL_DOCS] No legal_docs/ folder found, skipping")
            return

        # Find PDF files
        pdf_files = list(LEGAL_DOCS_DIR.glob("*.pdf"))
        if not pdf_files:
            logger.info("[LEGAL_DOCS] No PDF files found in legal_docs/")
            return

        logger.info("[LEGAL_DOCS] Found %d PDF files, checking...", len(pdf_files))

        # Configure Gemini
        genai.configure(api_key=settings.gemini_api_key)

        async with pool.acquire() as conn:
            # Check if tables exist
            table_exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'legal_documents'
                )
                """
            )
            if not table_exists:
                logger.warning("[LEGAL_DOCS] legal_documents table not found, skipping")
                return

            # Check if embedding column exists (pgvector)
            has_embedding = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'legal_articles' AND column_name = 'embedding'
                )
                """
            )

            if not has_embedding:
                logger.info("[LEGAL_DOCS] No embedding column (pgvector not installed), using text-only mode")

            # Process each PDF
            loaded = 0
            skipped = 0
            for pdf_path in pdf_files:
                try:
                    result = await _process_pdf(pdf_path, conn, has_embedding)
                    if result["status"] == "success":
                        loaded += 1
                    else:
                        skipped += 1
                except Exception as e:
                    logger.error("[LEGAL_DOCS] Error processing %s: %s", pdf_path.name, e)

            if loaded > 0:
                logger.info("[LEGAL_DOCS] Loaded %d new documents, %d skipped", loaded, skipped)
            else:
                logger.info("[LEGAL_DOCS] All %d documents already loaded", skipped)

    except Exception as e:
        logger.error("[LEGAL_DOCS] Failed to load legal docs: %s", e)
