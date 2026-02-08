"""Add notifications table

Revision ID: 20260131150000
Revises: 20260131140000
Create Date: 2026-01-31 15:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

# revision identifiers, used by Alembic.
revision = '20260131150000'
down_revision = '20260131140000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'notifications',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('user_id', UUID(as_uuid=True), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
        sa.Column('type', sa.String(50), nullable=False),
        sa.Column('title', sa.String(255), nullable=False),
        sa.Column('message', sa.Text, nullable=True),
        sa.Column('data', JSONB, server_default='{}'),
        sa.Column('is_read', sa.Boolean, server_default='false'),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('NOW()')),
    )

    # Index for fetching unread notifications
    op.create_index(
        'idx_notifications_user_unread',
        'notifications',
        ['user_id', 'is_read'],
        postgresql_where=sa.text('is_read = FALSE')
    )

    # Index for fetching notifications sorted by date
    op.create_index(
        'idx_notifications_user_created',
        'notifications',
        ['user_id', sa.text('created_at DESC')]
    )


def downgrade() -> None:
    op.drop_index('idx_notifications_user_created', table_name='notifications')
    op.drop_index('idx_notifications_user_unread', table_name='notifications')
    op.drop_table('notifications')
