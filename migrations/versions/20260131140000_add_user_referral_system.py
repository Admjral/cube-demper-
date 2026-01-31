"""Add user referral system tables and columns

Revision ID: 20260131140000
Revises: 20260131130000
Create Date: 2026-01-31 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260131140000'
down_revision: Union[str, None] = '20260131130000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add referral columns to users table
    op.add_column('users', sa.Column('referred_by', sa.UUID(), nullable=True))
    op.add_column('users', sa.Column('referral_code', sa.String(20), nullable=True))
    op.add_column('users', sa.Column('referral_clicks', sa.Integer(), nullable=False, server_default='0'))

    # Create foreign key for referred_by
    op.create_foreign_key(
        'fk_users_referred_by',
        'users', 'users',
        ['referred_by'], ['id'],
        ondelete='SET NULL'
    )

    # Create unique index for referral_code
    op.create_index('idx_users_referral_code', 'users', ['referral_code'], unique=True)

    # Create referral_transactions table
    op.execute("""
        CREATE TABLE IF NOT EXISTS referral_transactions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            referred_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
            type VARCHAR(20) NOT NULL CHECK (type IN ('income', 'payout')),
            amount INTEGER NOT NULL,
            description TEXT,
            status VARCHAR(20) NOT NULL DEFAULT 'completed' CHECK (status IN ('pending', 'completed', 'rejected')),
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    op.create_index('idx_referral_transactions_user_id', 'referral_transactions', ['user_id'])
    op.create_index('idx_referral_transactions_created_at', 'referral_transactions', ['created_at'])
    op.create_index('idx_referral_transactions_type', 'referral_transactions', ['type'])


def downgrade() -> None:
    op.drop_table('referral_transactions')
    op.drop_index('idx_users_referral_code', 'users')
    op.drop_constraint('fk_users_referred_by', 'users', type_='foreignkey')
    op.drop_column('users', 'referral_clicks')
    op.drop_column('users', 'referral_code')
    op.drop_column('users', 'referred_by')
