"""Add product_city_prices table for city-based pricing

Revision ID: 20260121100000
Revises: 20260118140000
Create Date: 2026-01-21 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '20260121100000'
down_revision: Union[str, None] = '20260118180000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create product_city_prices table
    op.create_table(
        'product_city_prices',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('product_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('city_id', sa.String(20), nullable=False),
        sa.Column('city_name', sa.String(50), nullable=False),
        
        # Price settings for this city
        sa.Column('price', sa.Integer(), nullable=True),
        sa.Column('min_price', sa.Integer(), nullable=True),
        sa.Column('max_price', sa.Integer(), nullable=True),
        sa.Column('bot_active', sa.Boolean(), server_default='true', nullable=False),
        
        # Tracking
        sa.Column('last_check_time', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('competitor_price', sa.Integer(), nullable=True),
        sa.Column('our_position', sa.Integer(), nullable=True),
        
        # Timestamps
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()'), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()'), nullable=False),
        
        # Constraints
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['product_id'], ['products.id'], ondelete='CASCADE'),
        sa.UniqueConstraint('product_id', 'city_id', name='uq_product_city')
    )
    
    # Create indexes for faster queries
    op.create_index('ix_product_city_prices_product_id', 'product_city_prices', ['product_id'])
    op.create_index('ix_product_city_prices_city_id', 'product_city_prices', ['city_id'])
    op.create_index('ix_product_city_prices_bot_active', 'product_city_prices', ['bot_active'])


def downgrade() -> None:
    op.drop_index('ix_product_city_prices_bot_active', table_name='product_city_prices')
    op.drop_index('ix_product_city_prices_city_id', table_name='product_city_prices')
    op.drop_index('ix_product_city_prices_product_id', table_name='product_city_prices')
    op.drop_table('product_city_prices')
