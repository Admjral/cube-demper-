"""Add unique constraint to products table

Revision ID: add_products_unique
Revises: 6ce6a0fa5853
Create Date: 2026-01-18 01:20:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_products_unique'
down_revision = '6ce6a0fa5853'
branch_labels = None
depends_on = None


def upgrade():
    # Add unique constraint for store_id + kaspi_product_id
    op.execute("""
        ALTER TABLE products
        ADD CONSTRAINT products_store_kaspi_product_unique
        UNIQUE (store_id, kaspi_product_id)
    """)


def downgrade():
    op.execute("""
        ALTER TABLE products
        DROP CONSTRAINT IF EXISTS products_store_kaspi_product_unique
    """)
