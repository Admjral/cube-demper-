"""Add whatsapp_settings table

Revision ID: 20260127120000
Revises: 20260127110000
Create Date: 2026-01-27 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260127120000'
down_revision: Union[str, None] = '20260127110000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # WhatsApp Settings table - user-specific WhatsApp configuration
    op.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_settings (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            daily_limit INTEGER DEFAULT 100,
            interval_seconds INTEGER DEFAULT 30,
            work_hours_start VARCHAR(10) DEFAULT '09:00',
            work_hours_end VARCHAR(10) DEFAULT '21:00',
            work_days INTEGER[] DEFAULT ARRAY[1,2,3,4,5],
            auto_reply_enabled BOOLEAN DEFAULT false,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id)
        )
    """)
    op.create_index('idx_whatsapp_settings_user_id', 'whatsapp_settings', ['user_id'])


def downgrade() -> None:
    op.drop_index('idx_whatsapp_settings_user_id', table_name='whatsapp_settings')
    op.drop_table('whatsapp_settings')
