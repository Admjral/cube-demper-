"""Add WhatsApp tracking fields to orders

Tracks which WhatsApp messages were sent for each order status change.

Revision ID: 20260127140000
Revises: 20260127130000
Create Date: 2026-01-27 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260127140000'
down_revision: Union[str, None] = '20260127130000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add WhatsApp tracking fields to orders table
    op.execute("""
        ALTER TABLE orders
        ADD COLUMN IF NOT EXISTS previous_status VARCHAR(50),
        ADD COLUMN IF NOT EXISTS status_changed_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS wa_approved_sent_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS wa_shipped_sent_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS wa_delivered_sent_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS wa_completed_sent_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS wa_review_sent_at TIMESTAMPTZ
    """)

    # Create order_status_history table for detailed tracking
    op.execute("""
        CREATE TABLE IF NOT EXISTS order_status_history (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            order_id UUID REFERENCES orders(id) ON DELETE CASCADE,
            old_status VARCHAR(50),
            new_status VARCHAR(50) NOT NULL,
            wa_message_sent BOOLEAN DEFAULT FALSE,
            wa_message_id UUID REFERENCES whatsapp_messages(id) ON DELETE SET NULL,
            changed_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_order_status_history_order_id
        ON order_status_history(order_id)
    """)

    # Add orders_polling_enabled to kaspi_stores
    op.execute("""
        ALTER TABLE kaspi_stores
        ADD COLUMN IF NOT EXISTS orders_polling_enabled BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS orders_polling_interval_seconds INTEGER DEFAULT 60
    """)


def downgrade() -> None:
    op.drop_table('order_status_history')
    op.execute("""
        ALTER TABLE orders
        DROP COLUMN IF EXISTS previous_status,
        DROP COLUMN IF EXISTS status_changed_at,
        DROP COLUMN IF EXISTS wa_approved_sent_at,
        DROP COLUMN IF EXISTS wa_shipped_sent_at,
        DROP COLUMN IF EXISTS wa_delivered_sent_at,
        DROP COLUMN IF EXISTS wa_completed_sent_at,
        DROP COLUMN IF EXISTS wa_review_sent_at
    """)
    op.execute("""
        ALTER TABLE kaspi_stores
        DROP COLUMN IF EXISTS orders_polling_enabled,
        DROP COLUMN IF EXISTS orders_polling_interval_seconds
    """)
