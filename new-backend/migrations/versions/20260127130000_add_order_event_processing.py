"""Add order event processing for WhatsApp

Adds fields for order-triggered WhatsApp messages:
- trigger_event and order_code in whatsapp_messages
- scheduled_messages table for delayed review requests

Revision ID: 20260127130000
Revises: 20260127120000
Create Date: 2026-01-27 13:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260127130000'
down_revision: Union[str, None] = '20260127120000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add trigger_event and order_code columns to whatsapp_messages
    op.execute("""
        ALTER TABLE whatsapp_messages
        ADD COLUMN IF NOT EXISTS trigger_event VARCHAR(50),
        ADD COLUMN IF NOT EXISTS order_code VARCHAR(50)
    """)

    # Index for finding messages by order
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_whatsapp_messages_order_code
        ON whatsapp_messages(order_code)
        WHERE order_code IS NOT NULL
    """)

    # Index for finding messages by trigger event
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_whatsapp_messages_trigger_event
        ON whatsapp_messages(trigger_event)
        WHERE trigger_event IS NOT NULL
    """)

    # Scheduled messages table for delayed sending (review requests, etc.)
    op.execute("""
        CREATE TABLE IF NOT EXISTS scheduled_messages (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            store_id UUID REFERENCES kaspi_stores(id) ON DELETE CASCADE,
            order_code VARCHAR(50),
            event_type VARCHAR(50) NOT NULL,
            template_id UUID REFERENCES whatsapp_templates(id) ON DELETE SET NULL,
            scheduled_at TIMESTAMPTZ NOT NULL,
            processed_at TIMESTAMPTZ,
            status VARCHAR(20) DEFAULT 'pending',
            result JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_scheduled_messages_user_id
        ON scheduled_messages(user_id)
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_scheduled_messages_scheduled_at
        ON scheduled_messages(scheduled_at)
        WHERE status = 'pending'
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_scheduled_messages_status
        ON scheduled_messages(status)
    """)


def downgrade() -> None:
    op.drop_table('scheduled_messages')
    op.execute("DROP INDEX IF EXISTS idx_whatsapp_messages_trigger_event")
    op.execute("DROP INDEX IF EXISTS idx_whatsapp_messages_order_code")
    op.execute("""
        ALTER TABLE whatsapp_messages
        DROP COLUMN IF EXISTS trigger_event,
        DROP COLUMN IF EXISTS order_code
    """)
