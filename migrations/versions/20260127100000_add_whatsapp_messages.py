"""Add whatsapp_messages table for message history

Revision ID: 20260127100000
Revises: 20260126100000
Create Date: 2026-01-27 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260127100000'
down_revision: Union[str, None] = '20260126100000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # WhatsApp Messages table - stores all sent messages
    op.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_messages (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            session_id UUID REFERENCES whatsapp_sessions(id) ON DELETE SET NULL,
            template_id UUID REFERENCES whatsapp_templates(id) ON DELETE SET NULL,
            recipient_phone VARCHAR(50) NOT NULL,
            recipient_name VARCHAR(255),
            message_content TEXT NOT NULL,
            message_type VARCHAR(50) DEFAULT 'text',
            status VARCHAR(50) DEFAULT 'pending',
            waha_message_id VARCHAR(255),
            error_message TEXT,
            sent_at TIMESTAMPTZ,
            delivered_at TIMESTAMPTZ,
            read_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_whatsapp_messages_user_id', 'whatsapp_messages', ['user_id'])
    op.create_index('idx_whatsapp_messages_status', 'whatsapp_messages', ['status'])
    op.create_index('idx_whatsapp_messages_created_at', 'whatsapp_messages', ['created_at'])
    op.create_index('idx_whatsapp_messages_recipient', 'whatsapp_messages', ['recipient_phone'])


def downgrade() -> None:
    op.drop_index('idx_whatsapp_messages_recipient', table_name='whatsapp_messages')
    op.drop_index('idx_whatsapp_messages_created_at', table_name='whatsapp_messages')
    op.drop_index('idx_whatsapp_messages_status', table_name='whatsapp_messages')
    op.drop_index('idx_whatsapp_messages_user_id', table_name='whatsapp_messages')
    op.drop_table('whatsapp_messages')
