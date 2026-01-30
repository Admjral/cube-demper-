"""Add AI Lawyer tables: legal documents, articles with RAG, documents generation

Revision ID: 20260129100000
Revises: 20260128100000
Create Date: 2026-01-29 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260129100000'
down_revision: Union[str, None] = '20260128100000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Enable pgvector extension if not exists
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    
    # Add lawyer_language to users table
    op.add_column('users', sa.Column('lawyer_language', sa.VARCHAR(10), nullable=False, server_default='ru'))
    
    # Create legal_documents table - stores full law texts
    op.execute("""
        CREATE TABLE legal_documents (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            title VARCHAR(500) NOT NULL,
            code VARCHAR(100),
            document_type VARCHAR(50) NOT NULL,
            language VARCHAR(10) NOT NULL DEFAULT 'ru',
            source_url TEXT,
            effective_date DATE,
            last_updated TIMESTAMPTZ,
            full_text TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_legal_documents_code', 'legal_documents', ['code'])
    op.create_index('idx_legal_documents_type', 'legal_documents', ['document_type'])
    op.create_index('idx_legal_documents_language', 'legal_documents', ['language'])
    
    # Create legal_articles table - stores law articles with embeddings for RAG
    op.execute("""
        CREATE TABLE legal_articles (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            document_id UUID REFERENCES legal_documents(id) ON DELETE CASCADE,
            article_number VARCHAR(50),
            title VARCHAR(500),
            content TEXT NOT NULL,
            embedding VECTOR(768),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_legal_articles_document', 'legal_articles', ['document_id'])
    op.create_index('idx_legal_articles_number', 'legal_articles', ['article_number'])
    
    # Create vector index for semantic search (IVFFlat for performance)
    op.execute("""
        CREATE INDEX idx_legal_articles_embedding 
        ON legal_articles 
        USING ivfflat (embedding vector_cosine_ops)
        WITH (lists = 100)
    """)
    
    # Create lawyer_documents table - stores generated documents (contracts, claims, etc.)
    op.execute("""
        CREATE TABLE lawyer_documents (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            document_type VARCHAR(50) NOT NULL,
            title VARCHAR(500) NOT NULL,
            language VARCHAR(10) NOT NULL DEFAULT 'ru',
            input_data JSONB,
            content TEXT NOT NULL,
            pdf_url TEXT,
            docx_url TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_lawyer_documents_user', 'lawyer_documents', ['user_id'])
    op.create_index('idx_lawyer_documents_type', 'lawyer_documents', ['document_type'])
    
    # Create lawyer_chat_feedback table - stores user feedback on AI responses
    op.execute("""
        CREATE TABLE lawyer_chat_feedback (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            chat_message_id UUID REFERENCES ai_chat_history(id) ON DELETE CASCADE,
            rating SMALLINT CHECK (rating IN (-1, 1)),
            comment TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_lawyer_feedback_user', 'lawyer_chat_feedback', ['user_id'])
    op.create_index('idx_lawyer_feedback_message', 'lawyer_chat_feedback', ['chat_message_id'])
    
    # Add updated_at triggers
    op.execute("""
        CREATE TRIGGER update_legal_documents_updated_at
        BEFORE UPDATE ON legal_documents
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """)
    
    op.execute("""
        CREATE TRIGGER update_legal_articles_updated_at
        BEFORE UPDATE ON legal_articles
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS update_legal_articles_updated_at ON legal_articles")
    op.execute("DROP TRIGGER IF EXISTS update_legal_documents_updated_at ON legal_documents")
    op.drop_table('lawyer_chat_feedback')
    op.drop_table('lawyer_documents')
    op.drop_table('legal_articles')
    op.drop_table('legal_documents')
    op.drop_column('users', 'lawyer_language')
