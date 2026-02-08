"""Add admin features: blocked users, partners table

Revision ID: 20260128100000
Revises: 20260127140000
Create Date: 2026-01-28 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260128100000'
down_revision: Union[str, None] = '20260127140000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add is_blocked field to users table
    op.add_column('users', sa.Column('is_blocked', sa.Boolean(), nullable=False, server_default='false'))
    
    # Create partners table
    op.execute("""
        CREATE TABLE partners (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            full_name VARCHAR(255),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_partners_email', 'partners', ['email'])
    
    # Add partner_id to users table
    op.add_column('users', sa.Column('partner_id', sa.UUID(), nullable=True))
    op.create_foreign_key(
        'fk_users_partner_id',
        'users', 'partners',
        ['partner_id'], ['id'],
        ondelete='SET NULL'
    )
    op.create_index('idx_users_partner_id', 'users', ['partner_id'])
    
    # Add updated_at trigger for partners
    op.execute("""
        CREATE TRIGGER update_partners_updated_at
        BEFORE UPDATE ON partners
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS update_partners_updated_at ON partners")
    op.drop_index('idx_users_partner_id', 'users')
    op.drop_constraint('fk_users_partner_id', 'users', type_='foreignkey')
    op.drop_column('users', 'partner_id')
    op.drop_table('partners')
    op.drop_column('users', 'is_blocked')
