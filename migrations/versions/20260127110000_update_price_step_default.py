"""Update default price_step from 100 to 1 KZT

Revision ID: 20260127110000
Revises: 20260127100000
Create Date: 2026-01-27 11:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260127110000'
down_revision: Union[str, None] = '20260127100000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Change default value for price_step from 100 to 1
    # This makes the default demping strategy "undercut by 1 KZT"
    op.alter_column(
        'demping_settings',
        'price_step',
        server_default='1'
    )


def downgrade() -> None:
    # Revert to original default
    op.alter_column(
        'demping_settings',
        'price_step',
        server_default='100'
    )
