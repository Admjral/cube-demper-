"""add_railway_service_columns

Revision ID: 6ce6a0fa5853
Revises: ba10cb14a230
Create Date: 2026-01-18 01:37:53.725970

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6ce6a0fa5853'
down_revision: Union[str, Sequence[str], None] = 'ba10cb14a230'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add Railway service columns to whatsapp_sessions table."""
    # Add railway_service_id column
    op.add_column(
        'whatsapp_sessions',
        sa.Column('railway_service_id', sa.String(255), nullable=True)
    )

    # Add railway_service_url column
    op.add_column(
        'whatsapp_sessions',
        sa.Column('railway_service_url', sa.Text(), nullable=True)
    )

    # Create index on railway_service_id for faster lookups
    op.create_index(
        'idx_whatsapp_sessions_railway_service_id',
        'whatsapp_sessions',
        ['railway_service_id']
    )


def downgrade() -> None:
    """Remove Railway service columns from whatsapp_sessions table."""
    # Drop index first
    op.drop_index('idx_whatsapp_sessions_railway_service_id', 'whatsapp_sessions')

    # Drop columns
    op.drop_column('whatsapp_sessions', 'railway_service_url')
    op.drop_column('whatsapp_sessions', 'railway_service_id')
