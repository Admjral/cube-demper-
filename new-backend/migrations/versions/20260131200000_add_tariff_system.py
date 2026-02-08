"""Add tariff system (plans, addons, user_addons)

Revision ID: 20260131200000
Revises: 20260131150000
Create Date: 2026-01-31 20:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

# revision identifiers, used by Alembic.
revision = '20260131200000'
down_revision = '20260131150000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Create plans table
    op.create_table(
        'plans',
        sa.Column('id', UUID(), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('code', sa.String(50), nullable=False, unique=True),
        sa.Column('name', sa.String(100), nullable=False),
        sa.Column('price_tiyns', sa.Integer(), nullable=False),
        sa.Column('analytics_limit', sa.Integer(), nullable=False),
        sa.Column('demping_limit', sa.Integer(), nullable=False),
        sa.Column('features', JSONB(), server_default='[]', nullable=False),
        sa.Column('trial_days', sa.Integer(), server_default='0'),
        sa.Column('is_active', sa.Boolean(), server_default='true'),
        sa.Column('display_order', sa.Integer(), server_default='0'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('id')
    )

    # 2. Insert initial plans
    op.execute("""
        INSERT INTO plans (code, name, price_tiyns, analytics_limit, demping_limit, features, trial_days, display_order) VALUES
        ('basic', 'Базовый', 2199000, 500, 50,
         '["analytics", "demping", "exclude_own_stores", "invoice_glue", "orders_view", "unit_economics", "ai_lawyer", "priority_support"]'::jsonb,
         3, 1),
        ('standard', 'Стандарт', 2799000, 1000, 100,
         '["analytics", "demping", "exclude_own_stores", "invoice_glue", "orders_view", "unit_economics", "ai_lawyer", "priority_support", "preorder", "whatsapp_auto"]'::jsonb,
         0, 2),
        ('premium', 'Премиум', 3399000, -1, 200,
         '["analytics", "demping", "exclude_own_stores", "invoice_glue", "orders_view", "unit_economics", "ai_lawyer", "priority_support", "preorder", "whatsapp_auto", "whatsapp_bulk"]'::jsonb,
         0, 3)
    """)

    # 3. Create addons table
    op.create_table(
        'addons',
        sa.Column('id', UUID(), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('code', sa.String(50), nullable=False, unique=True),
        sa.Column('name', sa.String(100), nullable=False),
        sa.Column('description', sa.Text()),
        sa.Column('price_tiyns', sa.Integer(), nullable=False),
        sa.Column('is_recurring', sa.Boolean(), server_default='true'),
        sa.Column('stackable', sa.Boolean(), server_default='false'),
        sa.Column('features', JSONB(), server_default='[]', nullable=False),
        sa.Column('extra_limits', JSONB()),
        sa.Column('is_active', sa.Boolean(), server_default='true'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('id')
    )

    # 4. Insert initial addons
    op.execute("""
        INSERT INTO addons (code, name, description, price_tiyns, is_recurring, stackable, features, extra_limits) VALUES
        ('ai_salesman', 'ИИ продажник', 'ИИ-ассистент для продаж и общения с клиентами', 1500000, true, false, '["ai_salesman"]'::jsonb, NULL),
        ('demping_100', 'Демпинг +100 товаров', 'Дополнительный пакет демпинга на 100 товаров', 1000000, true, true, '[]'::jsonb, '{"demping_limit": 100}'::jsonb),
        ('preorder', 'Предзаказ', 'Модуль управления предзаказами', 1000000, true, false, '["preorder"]'::jsonb, NULL),
        ('whatsapp', 'WhatsApp рассылка', 'Модуль рассылок и коммуникаций в WhatsApp', 1500000, true, false, '["whatsapp_auto", "whatsapp_bulk"]'::jsonb, NULL),
        ('analytics_unlimited', 'Аналитика безлимит', 'Безлимитная аналитика по всем товарам', 2000000, true, false, '[]'::jsonb, '{"analytics_limit": -1}'::jsonb)
    """)

    # 5. Create user_addons table
    op.create_table(
        'user_addons',
        sa.Column('id', UUID(), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('user_id', UUID(), nullable=False),
        sa.Column('addon_id', UUID(), nullable=False),
        sa.Column('quantity', sa.Integer(), server_default='1'),
        sa.Column('status', sa.String(20), server_default='active'),
        sa.Column('starts_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('expires_at', sa.TIMESTAMP(timezone=True)),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['addon_id'], ['addons.id'], ondelete='CASCADE'),
        sa.UniqueConstraint('user_id', 'addon_id', name='uq_user_addon')
    )
    op.create_index('idx_user_addons_user_id', 'user_addons', ['user_id'])
    op.create_index('idx_user_addons_status', 'user_addons', ['status'])

    # 6. Update subscriptions table with new columns
    op.add_column('subscriptions', sa.Column('plan_id', UUID()))
    op.add_column('subscriptions', sa.Column('analytics_limit', sa.Integer(), server_default='500'))
    op.add_column('subscriptions', sa.Column('demping_limit', sa.Integer(), server_default='50'))
    op.add_column('subscriptions', sa.Column('is_trial', sa.Boolean(), server_default='false'))
    op.add_column('subscriptions', sa.Column('trial_ends_at', sa.TIMESTAMP(timezone=True)))
    op.add_column('subscriptions', sa.Column('assigned_by', UUID()))
    op.add_column('subscriptions', sa.Column('notes', sa.Text()))

    # Add foreign keys
    op.create_foreign_key('fk_subscriptions_plan_id', 'subscriptions', 'plans', ['plan_id'], ['id'])
    op.create_foreign_key('fk_subscriptions_assigned_by', 'subscriptions', 'users', ['assigned_by'], ['id'])
    op.create_index('idx_subscriptions_plan_id', 'subscriptions', ['plan_id'])

    # 7. Migrate existing subscriptions to new plans
    # Map old plans to new: free->basic, basic->standard, pro->premium
    op.execute("""
        UPDATE subscriptions s SET
            plan_id = (
                SELECT id FROM plans WHERE code =
                CASE
                    WHEN s.plan = 'pro' THEN 'premium'
                    WHEN s.plan = 'basic' THEN 'standard'
                    ELSE 'basic'
                END
            ),
            analytics_limit = CASE
                WHEN s.plan = 'pro' THEN -1
                WHEN s.plan = 'basic' THEN 1000
                ELSE 500
            END,
            demping_limit = CASE
                WHEN s.plan = 'pro' THEN 200
                WHEN s.plan = 'basic' THEN 100
                ELSE 50
            END
        WHERE s.status = 'active'
    """)


def downgrade() -> None:
    # Remove foreign keys first
    op.drop_constraint('fk_subscriptions_assigned_by', 'subscriptions', type_='foreignkey')
    op.drop_constraint('fk_subscriptions_plan_id', 'subscriptions', type_='foreignkey')

    # Remove indexes
    op.drop_index('idx_subscriptions_plan_id', table_name='subscriptions')

    # Remove new columns from subscriptions
    op.drop_column('subscriptions', 'notes')
    op.drop_column('subscriptions', 'assigned_by')
    op.drop_column('subscriptions', 'trial_ends_at')
    op.drop_column('subscriptions', 'is_trial')
    op.drop_column('subscriptions', 'demping_limit')
    op.drop_column('subscriptions', 'analytics_limit')
    op.drop_column('subscriptions', 'plan_id')

    # Drop indexes on user_addons
    op.drop_index('idx_user_addons_status', table_name='user_addons')
    op.drop_index('idx_user_addons_user_id', table_name='user_addons')

    # Drop tables
    op.drop_table('user_addons')
    op.drop_table('addons')
    op.drop_table('plans')
