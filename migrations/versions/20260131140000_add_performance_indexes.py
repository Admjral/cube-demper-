"""Add performance indexes for demper worker queries

Revision ID: 20260131140000
Revises: 20260131130000
Create Date: 2026-01-31 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '20260131140000'
down_revision: Union[str, None] = '20260131130000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Use raw SQL with IF NOT EXISTS for safety
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_products_external_kaspi_id
        ON products(external_kaspi_id)
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_kaspi_stores_needs_reauth_true
        ON kaspi_stores(needs_reauth)
        WHERE needs_reauth = TRUE
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_demping_settings_store_id
        ON demping_settings(store_id)
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_users_role_created
        ON users(role, created_at)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_users_role_created")
    op.execute("DROP INDEX IF EXISTS idx_demping_settings_store_id")
    op.execute("DROP INDEX IF EXISTS idx_kaspi_stores_needs_reauth_true")
    op.execute("DROP INDEX IF EXISTS idx_products_external_kaspi_id")
