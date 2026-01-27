"""Add kaspi_email and kaspi_password columns to kaspi_stores

Revision ID: 20260118170000
Revises: 20260118160000
Create Date: 2026-01-18 17:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260118170000'
down_revision: Union[str, None] = '20260118160000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add encrypted email and password columns for auto-reauthentication
    op.add_column('kaspi_stores', sa.Column('kaspi_email', sa.String(255), nullable=True))
    op.add_column('kaspi_stores', sa.Column('kaspi_password', sa.String(255), nullable=True))


def downgrade() -> None:
    op.drop_column('kaspi_stores', 'kaspi_password')
    op.drop_column('kaspi_stores', 'kaspi_email')
