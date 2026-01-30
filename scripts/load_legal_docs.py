"""
Скрипт для загрузки PDF документов в RAG систему ИИ-Юриста.

Использование:
    python scripts/load_legal_docs.py /path/to/pdf/folder

Требования:
    pip install pypdf google-generativeai asyncpg python-dotenv

Структура:
    - Разбивает PDF на чанки по ~500 слов
    - Генерирует эмбеддинги через Gemini text-embedding-004
    - Сохраняет в таблицы legal_documents и legal_articles
"""

import asyncio
import sys
import os
from pathlib import Path
from typing import Optional
import hashlib

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncpg
from dotenv import load_dotenv

# Load .env from backend root
load_dotenv(Path(__file__).parent.parent / ".env")

# Lazy imports for optional deps
try:
    from pypdf import PdfReader
except ImportError:
    print("❌ Установите pypdf: pip install pypdf")
    sys.exit(1)

try:
    import google.generativeai as genai
except ImportError:
    print("❌ Установите google-generativeai: pip install google-generativeai")
    sys.exit(1)


# Config from env
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "cube_demper")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "text-embedding-004")

# Chunk settings
CHUNK_SIZE = 500  # words
CHUNK_OVERLAP = 50  # words overlap between chunks


def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract all text from PDF file."""
    reader = PdfReader(pdf_path)
    text_parts = []
    
    for page_num, page in enumerate(reader.pages, 1):
        text = page.extract_text()
        if text:
            text_parts.append(f"[Страница {page_num}]\n{text}")
    
    return "\n\n".join(text_parts)


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[dict]:
    """Split text into overlapping chunks."""
    words = text.split()
    chunks = []
    
    start = 0
    chunk_num = 1
    
    while start < len(words):
        end = start + chunk_size
        chunk_words = words[start:end]
        chunk_text = " ".join(chunk_words)
        
        # Try to find article/section markers
        article_match = None
        for marker in ["Статья", "Глава", "Раздел", "Пункт"]:
            if marker in chunk_text:
                # Extract article number
                idx = chunk_text.find(marker)
                snippet = chunk_text[idx:idx+50]
                article_match = snippet.split("\n")[0].strip()
                break
        
        chunks.append({
            "chunk_num": chunk_num,
            "text": chunk_text,
            "word_count": len(chunk_words),
            "article_reference": article_match
        })
        
        start = end - overlap
        chunk_num += 1
    
    return chunks


async def get_embedding(text: str) -> list[float]:
    """Get embedding from Gemini API."""
    result = genai.embed_content(
        model=f"models/{EMBEDDING_MODEL}",
        content=text,
        task_type="retrieval_document"
    )
    return result['embedding']


async def process_pdf(pdf_path: Path, conn: asyncpg.Connection) -> dict:
    """Process single PDF and insert into database."""
    filename = pdf_path.name
    print(f"\n📄 Обработка: {filename}")
    
    # Extract text
    print("   📖 Извлечение текста...")
    full_text = extract_text_from_pdf(pdf_path)
    word_count = len(full_text.split())
    print(f"   📊 Слов: {word_count:,}")
    
    # Check if already exists
    existing = await conn.fetchval(
        "SELECT id FROM legal_documents WHERE title = $1",
        filename
    )
    
    if existing:
        print(f"   ⚠️  Документ уже загружен (id={existing}), пропускаем")
        return {"filename": filename, "status": "skipped", "chunks": 0}
    
    # Insert main document
    doc_id = await conn.fetchval("""
        INSERT INTO legal_documents (title, category, full_text, source_url)
        VALUES ($1, $2, $3, $4)
        RETURNING id
    """, filename, "Законодательство РК", full_text, f"file://{pdf_path}")
    
    print(f"   ✅ Документ сохранён (id={doc_id})")
    
    # Chunk text
    chunks = chunk_text(full_text)
    print(f"   🔪 Разбито на {len(chunks)} чанков")
    
    # Process chunks with embeddings
    print("   🧠 Генерация эмбеддингов...")
    
    for i, chunk in enumerate(chunks):
        # Get embedding
        embedding = await get_embedding(chunk["text"])
        
        # Insert article/chunk
        await conn.execute("""
            INSERT INTO legal_articles 
            (document_id, article_number, title, content, embedding, keywords)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, 
            doc_id,
            str(chunk["chunk_num"]),
            chunk["article_reference"] or f"Чанк {chunk['chunk_num']}",
            chunk["text"],
            embedding,
            []  # keywords можно добавить позже
        )
        
        # Progress
        if (i + 1) % 10 == 0 or i == len(chunks) - 1:
            print(f"   📤 Загружено: {i + 1}/{len(chunks)} чанков")
        
        # Rate limiting (Gemini has limits)
        await asyncio.sleep(0.1)
    
    print(f"   ✅ Готово: {len(chunks)} чанков с эмбеддингами")
    
    return {"filename": filename, "status": "success", "chunks": len(chunks)}


async def main():
    if len(sys.argv) < 2:
        print("Использование: python scripts/load_legal_docs.py <путь_к_папке_с_pdf>")
        print("\nПример:")
        print("  python scripts/load_legal_docs.py ./legal_docs/")
        sys.exit(1)
    
    pdf_folder = Path(sys.argv[1])
    
    if not pdf_folder.exists():
        print(f"❌ Папка не найдена: {pdf_folder}")
        sys.exit(1)
    
    # Find all PDFs
    pdf_files = list(pdf_folder.glob("*.pdf"))
    
    if not pdf_files:
        print(f"❌ PDF файлы не найдены в: {pdf_folder}")
        sys.exit(1)
    
    print(f"🔍 Найдено PDF файлов: {len(pdf_files)}")
    for f in pdf_files:
        print(f"   - {f.name}")
    
    # Check Gemini API key
    if not GEMINI_API_KEY:
        print("\n❌ GEMINI_API_KEY не установлен в .env")
        sys.exit(1)
    
    # Configure Gemini
    genai.configure(api_key=GEMINI_API_KEY)
    print(f"\n✅ Gemini API настроен (модель: {EMBEDDING_MODEL})")
    
    # Connect to database
    print(f"\n🔌 Подключение к базе данных...")
    conn = await asyncpg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    print(f"✅ Подключено к {POSTGRES_DB}@{POSTGRES_HOST}")
    
    # Check if tables exist
    table_exists = await conn.fetchval("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'legal_documents'
        )
    """)
    
    if not table_exists:
        print("\n❌ Таблица legal_documents не найдена!")
        print("   Запустите миграцию: alembic upgrade head")
        await conn.close()
        sys.exit(1)
    
    # Process each PDF
    results = []
    for pdf_path in pdf_files:
        try:
            result = await process_pdf(pdf_path, conn)
            results.append(result)
        except Exception as e:
            print(f"\n❌ Ошибка при обработке {pdf_path.name}: {e}")
            results.append({"filename": pdf_path.name, "status": "error", "error": str(e)})
    
    await conn.close()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 ИТОГИ")
    print("=" * 50)
    
    success = [r for r in results if r["status"] == "success"]
    skipped = [r for r in results if r["status"] == "skipped"]
    errors = [r for r in results if r["status"] == "error"]
    
    total_chunks = sum(r.get("chunks", 0) for r in success)
    
    print(f"✅ Успешно: {len(success)} файлов ({total_chunks} чанков)")
    print(f"⚠️  Пропущено: {len(skipped)} файлов")
    print(f"❌ Ошибки: {len(errors)} файлов")
    
    if errors:
        print("\nОшибки:")
        for e in errors:
            print(f"   - {e['filename']}: {e.get('error', 'Unknown')}")
    
    print("\n🎉 Готово! RAG система заполнена.")


if __name__ == "__main__":
    asyncio.run(main())
