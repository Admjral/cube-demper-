"""Add notification_settings JSONB to users table

Revision ID: 20260212120000
Revises: 20260211140000
Create Date: 2026-02-12
"""
from alembic import op
import sqlalchemy as sa

revision = '20260212120000'
down_revision = '20260211140000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'users',
        sa.Column(
            'notification_settings',
            sa.JSON(),
            server_default='{"orders": true, "price_changes": true, "support": true}',
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column('users', 'notification_settings')
