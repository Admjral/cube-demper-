"""Add name_en column to whatsapp_templates

Revision ID: 20260209120000
Revises: 20260207200000
Create Date: 2026-02-09 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = '20260209120000'
down_revision = '20260207200000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = [c['name'] for c in inspector.get_columns('whatsapp_templates')]
    if 'name_en' not in columns:
        op.add_column('whatsapp_templates', sa.Column('name_en', sa.String(255), nullable=True))


def downgrade() -> None:
    op.drop_column('whatsapp_templates', 'name_en')
