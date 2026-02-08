"""Add niche analysis tables for Kaspi product research

Revision ID: 20260207200000
Revises: 20260131200000
Create Date: 2026-02-07 20:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import UUID

# revision identifiers, used by Alembic.
revision = '20260207200000'
down_revision = '20260131200000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    existing_tables = inspector.get_table_names()

    # 1. Create niche_categories table
    if 'niche_categories' not in existing_tables:
        op.create_table(
            'niche_categories',
            sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('name', sa.String(255), nullable=False),
            sa.Column('parent_id', UUID(as_uuid=True), nullable=True),
            sa.Column('kaspi_category_id', sa.String(100), nullable=True),
            sa.Column('coefficient', sa.Float, server_default='15.0'),
            sa.Column('total_products', sa.Integer, server_default='0'),
            sa.Column('total_sellers', sa.Integer, server_default='0'),
            sa.Column('avg_price', sa.BigInteger, server_default='0'),
            sa.Column('total_revenue', sa.BigInteger, server_default='0'),
            sa.Column('status', sa.String(20), server_default="'open'"),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()')),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()')),
            sa.ForeignKeyConstraint(['parent_id'], ['niche_categories.id'], ondelete='SET NULL'),
        )

        op.create_index('idx_niche_categories_parent', 'niche_categories', ['parent_id'])
        op.create_index('idx_niche_categories_kaspi_id', 'niche_categories', ['kaspi_category_id'], unique=True)
        op.create_index('idx_niche_categories_revenue', 'niche_categories', ['total_revenue'])

    # 2. Create niche_products table
    if 'niche_products' not in existing_tables:
        op.create_table(
            'niche_products',
            sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('category_id', UUID(as_uuid=True), nullable=False),
            sa.Column('kaspi_product_id', sa.String(50), nullable=False, unique=True),
            sa.Column('name', sa.String(500), nullable=False),
            sa.Column('brand', sa.String(255), nullable=True),
            sa.Column('price', sa.BigInteger, server_default='0'),
            sa.Column('reviews_count', sa.Integer, server_default='0'),
            sa.Column('rating', sa.Float, server_default='0'),
            sa.Column('sellers_count', sa.Integer, server_default='0'),
            sa.Column('estimated_sales', sa.Integer, server_default='0'),
            sa.Column('estimated_revenue', sa.BigInteger, server_default='0'),
            sa.Column('image_url', sa.String(1000), nullable=True),
            sa.Column('kaspi_url', sa.String(1000), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()')),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()')),
            sa.ForeignKeyConstraint(['category_id'], ['niche_categories.id'], ondelete='CASCADE'),
        )

        op.create_index('idx_niche_products_category', 'niche_products', ['category_id'])
        op.create_index('idx_niche_products_kaspi_id', 'niche_products', ['kaspi_product_id'])
        op.create_index('idx_niche_products_revenue', 'niche_products', ['estimated_revenue'])
        op.create_index('idx_niche_products_sales', 'niche_products', ['estimated_sales'])
        op.create_index('idx_niche_products_reviews', 'niche_products', ['reviews_count'])
        op.create_index('idx_niche_products_brand', 'niche_products', ['brand'])

    # 3. Create niche_product_history table for monthly snapshots
    if 'niche_product_history' not in existing_tables:
        op.create_table(
            'niche_product_history',
            sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('product_id', UUID(as_uuid=True), nullable=False),
            sa.Column('year', sa.Integer, nullable=False),
            sa.Column('month', sa.Integer, nullable=False),
            sa.Column('reviews_count', sa.Integer, server_default='0'),
            sa.Column('estimated_sales', sa.Integer, server_default='0'),
            sa.Column('estimated_revenue', sa.BigInteger, server_default='0'),
            sa.Column('price', sa.BigInteger, server_default='0'),
            sa.Column('sellers_count', sa.Integer, server_default='0'),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()')),
            sa.ForeignKeyConstraint(['product_id'], ['niche_products.id'], ondelete='CASCADE'),
            sa.UniqueConstraint('product_id', 'year', 'month', name='uq_niche_product_history_period'),
        )

        op.create_index('idx_niche_product_history_product', 'niche_product_history', ['product_id'])
        op.create_index('idx_niche_product_history_period', 'niche_product_history', ['year', 'month'])


def downgrade() -> None:
    op.drop_table('niche_product_history')
    op.drop_table('niche_products')
    op.drop_table('niche_categories')
