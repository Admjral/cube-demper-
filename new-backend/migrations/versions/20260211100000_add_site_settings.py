"""Add site_settings table for configurable referral commission percent

Revision ID: 20260211100000
Revises: 20260210120000
Create Date: 2026-02-11

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = '20260211100000'
down_revision = '20260210120000'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    existing = inspector.get_table_names()

    if 'site_settings' not in existing:
        op.create_table(
            'site_settings',
            sa.Column('key', sa.String(100), primary_key=True),
            sa.Column('value', sa.Text(), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()')),
        )

    # Seed default referral commission percent
    op.execute("""
        INSERT INTO site_settings (key, value)
        VALUES ('referral_commission_percent', '20')
        ON CONFLICT (key) DO NOTHING
    """)


def downgrade():
    op.drop_table('site_settings')
