"""Add excluded_merchant_ids to demping_settings

Revision ID: 20260121120000
Revises: 20260121100000
Create Date: 2026-01-21 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '20260121120000'
down_revision: Union[str, None] = '20260121100000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add excluded_merchant_ids column to demping_settings table
    # This stores a list of merchant IDs that should not be considered as competitors
    # Used when a user has multiple stores and doesn't want them to compete with each other
    op.add_column(
        'demping_settings',
        sa.Column(
            'excluded_merchant_ids',
            postgresql.ARRAY(sa.String(50)),
            nullable=True,
            server_default='{}',
            comment='List of merchant IDs to exclude from competition (e.g., own stores)'
        )
    )


def downgrade() -> None:
    op.drop_column('demping_settings', 'excluded_merchant_ids')
