"""Add product-level demping fields

Revision ID: 20260118140000
Revises: 20260118120000
Create Date: 2026-01-18 14:00:00
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '20260118140000'
down_revision = '20260118120000'
branch_labels = None
depends_on = None


def upgrade():
    # Add new columns for product-level demping settings
    op.add_column('products', sa.Column('max_price', sa.Integer(), nullable=True))
    op.add_column('products', sa.Column('min_price', sa.Integer(), nullable=True))
    op.add_column('products', sa.Column('price_step_override', sa.Integer(), nullable=True))
    op.add_column('products', sa.Column('demping_strategy', sa.String(50), nullable=False, server_default='standard'))
    op.add_column('products', sa.Column('strategy_params', postgresql.JSONB(), nullable=True))

    # Create index on demping_strategy for faster filtering
    op.create_index('idx_products_demping_strategy', 'products', ['demping_strategy'])


def downgrade():
    # Drop index
    op.drop_index('idx_products_demping_strategy', table_name='products')

    # Remove columns
    op.drop_column('products', 'strategy_params')
    op.drop_column('products', 'demping_strategy')
    op.drop_column('products', 'price_step_override')
    op.drop_column('products', 'min_price')
    op.drop_column('products', 'max_price')
