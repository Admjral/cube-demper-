"""Add pre_order_days column to products table

Revision ID: 20260210140000
Revises: 20260210120000
Create Date: 2026-02-10

"""
from alembic import op

# revision identifiers
revision = '20260210140000'
down_revision = '20260210120000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE products ADD COLUMN IF NOT EXISTS pre_order_days INTEGER DEFAULT 0;
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE products DROP COLUMN IF EXISTS pre_order_days;")
