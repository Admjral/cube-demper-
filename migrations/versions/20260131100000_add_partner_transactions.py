"""Add partner transactions and extended partner fields

Revision ID: 20260131100000
Revises: 20260131000000
Create Date: 2026-01-31 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260131100000'
down_revision: Union[str, None] = '20260131000000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add new fields to partners table
    op.add_column('partners', sa.Column('promo_code', sa.String(50), nullable=True))
    op.add_column('partners', sa.Column('referral_link', sa.String(255), nullable=True))
    op.add_column('partners', sa.Column('clicks_count', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('partners', sa.Column('commission_percent', sa.Integer(), nullable=False, server_default='20'))

    op.create_index('idx_partners_promo_code', 'partners', ['promo_code'], unique=True)

    # Create partner_transactions table
    op.execute("""
        CREATE TABLE partner_transactions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            partner_id UUID NOT NULL REFERENCES partners(id) ON DELETE CASCADE,
            user_id UUID REFERENCES users(id) ON DELETE SET NULL,
            type VARCHAR(20) NOT NULL CHECK (type IN ('income', 'payout')),
            amount INTEGER NOT NULL,
            description TEXT,
            status VARCHAR(20) NOT NULL DEFAULT 'completed' CHECK (status IN ('pending', 'completed', 'rejected')),
            requisites TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    op.create_index('idx_partner_transactions_partner_id', 'partner_transactions', ['partner_id'])
    op.create_index('idx_partner_transactions_created_at', 'partner_transactions', ['created_at'])
    op.create_index('idx_partner_transactions_type', 'partner_transactions', ['type'])


def downgrade() -> None:
    op.drop_table('partner_transactions')
    op.drop_index('idx_partners_promo_code', 'partners')
    op.drop_column('partners', 'commission_percent')
    op.drop_column('partners', 'clicks_count')
    op.drop_column('partners', 'referral_link')
    op.drop_column('partners', 'promo_code')
