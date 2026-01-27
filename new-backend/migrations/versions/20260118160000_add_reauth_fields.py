"""Add needs_reauth and reauth_reason fields to kaspi_stores

Revision ID: 20260118160000
Revises: 20260118140000
Create Date: 2026-01-18 16:00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20260118160000'
down_revision = '20260118140000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add needs_reauth column - indicates if store needs re-authentication
    op.add_column('kaspi_stores', sa.Column('needs_reauth', sa.Boolean(), nullable=False, server_default='false'))

    # Add reauth_reason column - explains why re-auth is needed
    op.add_column('kaspi_stores', sa.Column('reauth_reason', sa.String(255), nullable=True))


def downgrade() -> None:
    op.drop_column('kaspi_stores', 'reauth_reason')
    op.drop_column('kaspi_stores', 'needs_reauth')
