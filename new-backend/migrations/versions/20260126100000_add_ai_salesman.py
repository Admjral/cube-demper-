"""Add AI Salesman tables and settings

Revision ID: 20260126100000
Revises: 20260121120000
Create Date: 2026-01-26 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260126100000'
down_revision: Union[str, None] = '20260121120000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # AI Salesman messages table - stores generated and sent upsell messages
    op.execute("""
        CREATE TABLE IF NOT EXISTS ai_salesman_messages (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            order_id UUID REFERENCES orders(id) ON DELETE SET NULL,
            store_id UUID NOT NULL REFERENCES kaspi_stores(id) ON DELETE CASCADE,
            customer_phone VARCHAR(50) NOT NULL,
            trigger_type VARCHAR(50) NOT NULL,
            message_text TEXT NOT NULL,
            products_suggested TEXT[],
            sent_at TIMESTAMPTZ,
            delivered_at TIMESTAMPTZ,
            read_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_ai_salesman_messages_store_id', 'ai_salesman_messages', ['store_id'])
    op.create_index('idx_ai_salesman_messages_order_id', 'ai_salesman_messages', ['order_id'])
    op.create_index('idx_ai_salesman_messages_created_at', 'ai_salesman_messages', ['created_at'])

    # Add AI settings columns to kaspi_stores
    op.add_column('kaspi_stores', sa.Column('ai_enabled', sa.Boolean(), nullable=True, server_default='true'))
    op.add_column('kaspi_stores', sa.Column('ai_tone', sa.String(100), nullable=True))
    op.add_column('kaspi_stores', sa.Column('ai_discount_percent', sa.Integer(), nullable=True))
    op.add_column('kaspi_stores', sa.Column('ai_promo_code', sa.String(50), nullable=True))
    op.add_column('kaspi_stores', sa.Column('ai_review_bonus', sa.String(255), nullable=True))
    op.add_column('kaspi_stores', sa.Column('ai_send_delay_minutes', sa.Integer(), nullable=True, server_default='10'))
    op.add_column('kaspi_stores', sa.Column('ai_max_messages_per_day', sa.Integer(), nullable=True, server_default='50'))
    
    # Add sales_count to products if not exists (for sorting by popularity)
    op.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'products' AND column_name = 'sales_count'
            ) THEN
                ALTER TABLE products ADD COLUMN sales_count INTEGER DEFAULT 0;
            END IF;
        END $$;
    """)
    
    # Add category to products if not exists
    op.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'products' AND column_name = 'category'
            ) THEN
                ALTER TABLE products ADD COLUMN category VARCHAR(255);
            END IF;
        END $$;
    """)


def downgrade() -> None:
    # Remove AI settings columns from kaspi_stores
    op.drop_column('kaspi_stores', 'ai_max_messages_per_day')
    op.drop_column('kaspi_stores', 'ai_send_delay_minutes')
    op.drop_column('kaspi_stores', 'ai_review_bonus')
    op.drop_column('kaspi_stores', 'ai_promo_code')
    op.drop_column('kaspi_stores', 'ai_discount_percent')
    op.drop_column('kaspi_stores', 'ai_tone')
    op.drop_column('kaspi_stores', 'ai_enabled')
    
    # Drop ai_salesman_messages table
    op.drop_index('idx_ai_salesman_messages_created_at', table_name='ai_salesman_messages')
    op.drop_index('idx_ai_salesman_messages_order_id', table_name='ai_salesman_messages')
    op.drop_index('idx_ai_salesman_messages_store_id', table_name='ai_salesman_messages')
    op.drop_table('ai_salesman_messages')
