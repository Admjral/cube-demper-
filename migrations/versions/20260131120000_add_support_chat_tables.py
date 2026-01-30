"""Add support chat tables

Revision ID: 20260131120000
Revises: 20260131110000
Create Date: 2026-01-31 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260131120000'
down_revision: Union[str, None] = '20260131110000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create support_chats table
    op.execute("""
        CREATE TABLE support_chats (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('open', 'closed', 'pending')),
            assigned_to UUID REFERENCES users(id) ON DELETE SET NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # Create support_messages table
    op.execute("""
        CREATE TABLE support_messages (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            chat_id UUID NOT NULL REFERENCES support_chats(id) ON DELETE CASCADE,
            sender_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            sender_type VARCHAR(10) NOT NULL CHECK (sender_type IN ('user', 'support')),
            content TEXT NOT NULL,
            is_read BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # Create indexes for better query performance
    op.execute("CREATE INDEX idx_support_chats_user_id ON support_chats(user_id)")
    op.execute("CREATE INDEX idx_support_chats_status ON support_chats(status)")
    op.execute("CREATE INDEX idx_support_chats_assigned_to ON support_chats(assigned_to)")
    op.execute("CREATE INDEX idx_support_messages_chat_id ON support_messages(chat_id)")
    op.execute("CREATE INDEX idx_support_messages_created_at ON support_messages(created_at)")
    op.execute("CREATE INDEX idx_support_messages_is_read ON support_messages(is_read) WHERE is_read = FALSE")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS support_messages")
    op.execute("DROP TABLE IF EXISTS support_chats")
