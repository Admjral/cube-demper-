"""fix notification timestamps to use timestamptz

Revision ID: 20260212130000
Revises: 20260212120000
Create Date: 2026-02-12 13:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20260212130000'
down_revision = '20260212120000'
branch_labels = None
depends_on = None


def upgrade():
    # Change created_at from timestamp to timestamptz
    # This will preserve existing data and add timezone info (assumed UTC)
    op.execute("""
        ALTER TABLE notifications
        ALTER COLUMN created_at TYPE timestamptz
        USING created_at AT TIME ZONE 'UTC'
    """)


def downgrade():
    # Revert to timestamp without time zone
    op.execute("""
        ALTER TABLE notifications
        ALTER COLUMN created_at TYPE timestamp
        USING created_at AT TIME ZONE 'UTC'
    """)
