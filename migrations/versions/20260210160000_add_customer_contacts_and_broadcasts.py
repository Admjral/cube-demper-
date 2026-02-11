"""Add customer_contacts and broadcast_campaigns tables

Revision ID: 20260210160000
Revises: 20260210140000
Create Date: 2026-02-10

customer_contacts: накапливает телефоны покупателей из заказов
broadcast_campaigns: маркетинговые рассылки (1 раз в месяц)
"""
from alembic import op

# revision identifiers
revision = '20260210160000'
down_revision = '20260210140000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Customer contacts - accumulated from orders
    op.execute("""
        CREATE TABLE IF NOT EXISTS customer_contacts (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            store_id UUID REFERENCES kaspi_stores(id) ON DELETE SET NULL,
            phone VARCHAR(20) NOT NULL,
            name VARCHAR(255),
            first_order_code VARCHAR(50),
            last_order_code VARCHAR(50),
            orders_count INTEGER DEFAULT 1,
            is_blocked BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, phone)
        );

        CREATE INDEX IF NOT EXISTS idx_customer_contacts_user_id
        ON customer_contacts(user_id);

        CREATE INDEX IF NOT EXISTS idx_customer_contacts_phone
        ON customer_contacts(phone);

        CREATE INDEX IF NOT EXISTS idx_customer_contacts_store_id
        ON customer_contacts(store_id);
    """)

    # Broadcast campaigns - marketing mass sends
    op.execute("""
        CREATE TABLE IF NOT EXISTS broadcast_campaigns (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            store_id UUID REFERENCES kaspi_stores(id) ON DELETE SET NULL,
            template_id UUID REFERENCES whatsapp_templates(id) ON DELETE SET NULL,
            name VARCHAR(255) NOT NULL,
            message_text TEXT NOT NULL,
            status VARCHAR(20) DEFAULT 'draft',
            scheduled_at TIMESTAMPTZ,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            total_recipients INTEGER DEFAULT 0,
            sent_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            filter_min_orders INTEGER DEFAULT 0,
            filter_store_id UUID,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_broadcast_campaigns_user_id
        ON broadcast_campaigns(user_id);

        CREATE INDEX IF NOT EXISTS idx_broadcast_campaigns_status
        ON broadcast_campaigns(status);
    """)

    # Broadcast recipients - individual sends within a campaign
    op.execute("""
        CREATE TABLE IF NOT EXISTS broadcast_recipients (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            campaign_id UUID NOT NULL REFERENCES broadcast_campaigns(id) ON DELETE CASCADE,
            contact_id UUID NOT NULL REFERENCES customer_contacts(id) ON DELETE CASCADE,
            phone VARCHAR(20) NOT NULL,
            status VARCHAR(20) DEFAULT 'pending',
            sent_at TIMESTAMPTZ,
            error_message TEXT,
            waha_message_id VARCHAR(255)
        );

        CREATE INDEX IF NOT EXISTS idx_broadcast_recipients_campaign_id
        ON broadcast_recipients(campaign_id);

        CREATE INDEX IF NOT EXISTS idx_broadcast_recipients_status
        ON broadcast_recipients(status);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS broadcast_recipients;")
    op.execute("DROP TABLE IF EXISTS broadcast_campaigns;")
    op.execute("DROP TABLE IF EXISTS customer_contacts;")
