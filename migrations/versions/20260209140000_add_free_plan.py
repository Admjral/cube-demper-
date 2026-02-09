"""Add free plan to plans table and fix existing users mapped to basic

Revision ID: 20260209140000
Revises: 20260209130000
Create Date: 2026-02-09

"""
from alembic import op

# revision identifiers
revision = '20260209140000'
down_revision = '20260209130000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Insert 'free' plan into plans table
    op.execute("""
        INSERT INTO plans (id, code, name, price_tiyns, analytics_limit, demping_limit, features, trial_days, is_active, display_order)
        VALUES (
            gen_random_uuid(),
            'free',
            'Бесплатный',
            0,
            0,
            0,
            '[]'::jsonb,
            3,
            TRUE,
            0
        )
        ON CONFLICT (code) DO NOTHING
    """)

    # 2. Fix existing subscriptions that were incorrectly mapped from 'free' to 'basic'
    #    The tariff migration (20260131200000) mapped plan='free' → plan_id=(basic plan id)
    #    We need to re-map them to the new 'free' plan
    op.execute("""
        UPDATE subscriptions
        SET
            plan_id = (SELECT id FROM plans WHERE code = 'free'),
            analytics_limit = 0,
            demping_limit = 0
        WHERE plan = 'free'
          AND plan_id = (SELECT id FROM plans WHERE code = 'basic')
    """)

    # 3. Also fix subscriptions with plan='free' and plan_id IS NULL (users registered after tariff migration)
    op.execute("""
        UPDATE subscriptions
        SET
            plan_id = (SELECT id FROM plans WHERE code = 'free'),
            analytics_limit = 0,
            demping_limit = 0
        WHERE plan = 'free'
          AND plan_id IS NULL
    """)


def downgrade() -> None:
    # Revert subscriptions back to basic plan
    op.execute("""
        UPDATE subscriptions
        SET
            plan_id = (SELECT id FROM plans WHERE code = 'basic'),
            analytics_limit = 500,
            demping_limit = 50
        WHERE plan = 'free'
          AND plan_id = (SELECT id FROM plans WHERE code = 'free')
    """)

    # Remove free plan
    op.execute("DELETE FROM plans WHERE code = 'free'")
