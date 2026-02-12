"""fix whatsapp_sessions constraints for WAHA Plus

Revision ID: 20260212160000
Revises: 20260212150000
Create Date: 2026-02-12 16:00:00.000000

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '20260212160000'
down_revision = '20260212150000'
branch_labels = None
depends_on = None


def upgrade():
    # All sessions share one WAHA container, so waha_container_name must NOT be unique
    op.drop_constraint('whatsapp_sessions_waha_container_name_key', 'whatsapp_sessions', type_='unique')

    # WAHA Plus allows multiple sessions per user (different session_name).
    # Change unique from (user_id) to (user_id, session_name)
    op.drop_constraint('whatsapp_sessions_user_id_key', 'whatsapp_sessions', type_='unique')
    op.create_unique_constraint('uq_whatsapp_sessions_user_session', 'whatsapp_sessions', ['user_id', 'session_name'])


def downgrade():
    op.drop_constraint('uq_whatsapp_sessions_user_session', 'whatsapp_sessions', type_='unique')
    op.create_unique_constraint('whatsapp_sessions_user_id_key', 'whatsapp_sessions', ['user_id'])
    op.create_unique_constraint('whatsapp_sessions_waha_container_name_key', 'whatsapp_sessions', ['waha_container_name'])
