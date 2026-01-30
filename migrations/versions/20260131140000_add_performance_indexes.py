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
    # Index for products.external_kaspi_id - used in demper worker product lookup
    op.create_index(
        'idx_products_external_kaspi_id',
        'products',
        ['external_kaspi_id'],
        if_not_exists=True
    )

    # Partial index for kaspi_stores.needs_reauth - filters stores needing reauth
    op.create_index(
        'idx_kaspi_stores_needs_reauth_true',
        'kaspi_stores',
        ['needs_reauth'],
        postgresql_where='needs_reauth = TRUE',
        if_not_exists=True
    )

    # Index for demping_settings.store_id - used in LEFT JOIN in demper worker
    op.create_index(
        'idx_demping_settings_store_id',
        'demping_settings',
        ['store_id'],
        if_not_exists=True
    )

    # Composite index for common admin query pattern
    op.create_index(
        'idx_users_role_created',
        'users',
        ['role', 'created_at'],
        if_not_exists=True
    )


def downgrade() -> None:
    op.drop_index('idx_users_role_created', table_name='users', if_exists=True)
    op.drop_index('idx_demping_settings_store_id', table_name='demping_settings', if_exists=True)
    op.drop_index('idx_kaspi_stores_needs_reauth_true', table_name='kaspi_stores', if_exists=True)
    op.drop_index('idx_products_external_kaspi_id', table_name='products', if_exists=True)
