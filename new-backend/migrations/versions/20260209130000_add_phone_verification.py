"""Add phone verification to users and phone_verifications table

Revision ID: 20260209130000
Revises: 20260209120000
Create Date: 2026-02-09 13:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20260209130000'
down_revision = '20260209120000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add phone columns to users
    op.add_column('users', sa.Column('phone', sa.String(20), nullable=True))
    op.add_column('users', sa.Column('phone_verified', sa.Boolean(), server_default='false', nullable=False))

    # Create phone_verifications table
    op.create_table(
        'phone_verifications',
        sa.Column('id', sa.dialects.postgresql.UUID(), server_default=sa.text('gen_random_uuid()'), primary_key=True),
        sa.Column('user_id', sa.dialects.postgresql.UUID(), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('phone', sa.String(20), nullable=False),
        sa.Column('code', sa.String(6), nullable=False),
        sa.Column('attempts', sa.Integer(), server_default='0', nullable=False),
        sa.Column('expires_at', sa.DateTime(), nullable=False),
        sa.Column('verified_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('NOW()'), nullable=False),
    )
    op.create_index('idx_phone_verifications_user', 'phone_verifications', ['user_id'])


def downgrade() -> None:
    op.drop_index('idx_phone_verifications_user', table_name='phone_verifications')
    op.drop_table('phone_verifications')
    op.drop_column('users', 'phone_verified')
    op.drop_column('users', 'phone')
