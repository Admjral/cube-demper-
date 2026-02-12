"""add user profile fields

Revision ID: 20260212150000
Revises: 20260212120000
Create Date: 2026-02-12 15:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20260212150000'
down_revision = '20260212130000'
branch_labels = None
depends_on = None


def upgrade():
    # Add company_name, bin, tax_type to users table
    op.add_column('users', sa.Column('company_name', sa.String(255), nullable=True))
    op.add_column('users', sa.Column('bin', sa.String(12), nullable=True))
    op.add_column('users', sa.Column('tax_type', sa.String(50), nullable=True))


def downgrade():
    op.drop_column('users', 'tax_type')
    op.drop_column('users', 'bin')
    op.drop_column('users', 'company_name')
