"""Add saved calculations table for unit economics

Revision ID: 20260131130000
Revises: 20260131120000
Create Date: 2026-01-31 13:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '20260131130000'
down_revision: Union[str, None] = '20260131120000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'saved_calculations',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('kaspi_url', sa.Text, nullable=True),
        sa.Column('image_url', sa.Text, nullable=True),
        sa.Column('selling_price', sa.Numeric(12, 2), nullable=False),
        sa.Column('purchase_price', sa.Numeric(12, 2), nullable=False),
        sa.Column('category', sa.String(100), nullable=False),
        sa.Column('subcategory', sa.String(100), nullable=True),
        sa.Column('weight_kg', sa.Numeric(8, 3), nullable=False, server_default='1.0'),
        sa.Column('packaging_cost', sa.Numeric(12, 2), nullable=False, server_default='0'),
        sa.Column('other_costs', sa.Numeric(12, 2), nullable=False, server_default='0'),
        sa.Column('tax_regime', sa.String(50), nullable=False, server_default='ip_simplified'),
        sa.Column('use_vat', sa.Boolean, nullable=False, server_default='false'),
        # Calculated results (cached)
        sa.Column('commission_rate', sa.Numeric(5, 2), nullable=True),
        sa.Column('commission_amount', sa.Numeric(12, 2), nullable=True),
        sa.Column('tax_amount', sa.Numeric(12, 2), nullable=True),
        sa.Column('best_scenario', sa.String(50), nullable=True),
        sa.Column('best_profit', sa.Numeric(12, 2), nullable=True),
        sa.Column('best_margin', sa.Numeric(8, 2), nullable=True),
        # Metadata
        sa.Column('notes', sa.Text, nullable=True),
        sa.Column('is_favorite', sa.Boolean, nullable=False, server_default='false'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()'), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()'), nullable=False),
    )

    # Index for faster queries
    op.create_index('ix_saved_calculations_user_id', 'saved_calculations', ['user_id'])
    op.create_index('ix_saved_calculations_created_at', 'saved_calculations', ['created_at'])
    op.create_index('ix_saved_calculations_is_favorite', 'saved_calculations', ['is_favorite'])


def downgrade() -> None:
    op.drop_index('ix_saved_calculations_is_favorite')
    op.drop_index('ix_saved_calculations_created_at')
    op.drop_index('ix_saved_calculations_user_id')
    op.drop_table('saved_calculations')
