"""Add api_key_valid column to kaspi_stores

Revision ID: 20260211140000
Revises: 20260211100000
Create Date: 2026-02-11 14:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '20260211140000'
down_revision = '20260211100000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE kaspi_stores
        ADD COLUMN IF NOT EXISTS api_key_valid BOOLEAN DEFAULT TRUE
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE kaspi_stores
        DROP COLUMN IF EXISTS api_key_valid
    """)
