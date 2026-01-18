"""Add demping_settings table

Revision ID: 20260118120000
Revises: 20260118062046
Create Date: 2026-01-18 12:00:00
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '20260118120000'
down_revision = 'add_products_unique'
branch_labels = None
depends_on = None


def upgrade():
    # Create demping_settings table
    op.create_table(
        'demping_settings',
        sa.Column('id', postgresql.UUID(), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('store_id', postgresql.UUID(), nullable=False),
        sa.Column('min_profit', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('bot_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('price_step', sa.Integer(), nullable=False, server_default='100'),  # in tiyns
        sa.Column('min_margin_percent', sa.Integer(), nullable=False, server_default='5'),
        sa.Column('check_interval_minutes', sa.Integer(), nullable=False, server_default='15'),
        sa.Column('work_hours_start', sa.String(5), nullable=False, server_default='09:00'),
        sa.Column('work_hours_end', sa.String(5), nullable=False, server_default='21:00'),
        sa.Column('is_enabled', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('last_check', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['store_id'], ['kaspi_stores.id'], ondelete='CASCADE'),
        sa.UniqueConstraint('store_id', name='demping_settings_store_unique')
    )

    # Create index on store_id for faster lookups
    op.create_index('idx_demping_settings_store_id', 'demping_settings', ['store_id'])

    # Add trigger for updated_at
    op.execute("""
        CREATE TRIGGER update_demping_settings_updated_at
        BEFORE UPDATE ON demping_settings
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """)


def downgrade():
    # Drop trigger
    op.execute('DROP TRIGGER IF EXISTS update_demping_settings_updated_at ON demping_settings')

    # Drop index
    op.drop_index('idx_demping_settings_store_id', table_name='demping_settings')

    # Drop table
    op.drop_table('demping_settings')
