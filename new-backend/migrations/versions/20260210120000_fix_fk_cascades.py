"""Fix missing ON DELETE CASCADE/SET NULL for phone_verifications and subscriptions.assigned_by

Revision ID: 20260210120000
Revises: 20260209140000
Create Date: 2026-02-10

"""
from alembic import op

# revision identifiers
revision = '20260210120000'
down_revision = '20260209140000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Fix phone_verifications.user_id: add ON DELETE CASCADE
    op.execute("""
        ALTER TABLE phone_verifications
            DROP CONSTRAINT IF EXISTS phone_verifications_user_id_fkey;
    """)
    op.execute("""
        ALTER TABLE phone_verifications
            ADD CONSTRAINT phone_verifications_user_id_fkey
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
    """)

    # Fix subscriptions.assigned_by: change to ON DELETE SET NULL
    op.execute("""
        ALTER TABLE subscriptions
            DROP CONSTRAINT IF EXISTS fk_subscriptions_assigned_by;
    """)
    op.execute("""
        ALTER TABLE subscriptions
            ADD CONSTRAINT fk_subscriptions_assigned_by
            FOREIGN KEY (assigned_by) REFERENCES users(id) ON DELETE SET NULL;
    """)


def downgrade() -> None:
    # Revert subscriptions.assigned_by to NO ACTION
    op.execute("""
        ALTER TABLE subscriptions
            DROP CONSTRAINT IF EXISTS fk_subscriptions_assigned_by;
    """)
    op.execute("""
        ALTER TABLE subscriptions
            ADD CONSTRAINT fk_subscriptions_assigned_by
            FOREIGN KEY (assigned_by) REFERENCES users(id);
    """)

    # Revert phone_verifications.user_id to NO ACTION
    op.execute("""
        ALTER TABLE phone_verifications
            DROP CONSTRAINT IF EXISTS phone_verifications_user_id_fkey;
    """)
    op.execute("""
        ALTER TABLE phone_verifications
            ADD CONSTRAINT phone_verifications_user_id_fkey
            FOREIGN KEY (user_id) REFERENCES users(id);
    """)
