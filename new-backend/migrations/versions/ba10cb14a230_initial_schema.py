"""initial_schema

Revision ID: ba10cb14a230
Revises:
Create Date: 2026-01-17

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'ba10cb14a230'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Users table
    op.execute("""
        CREATE TABLE users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            full_name VARCHAR(255),
            role VARCHAR(20) DEFAULT 'user',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_users_email', 'users', ['email'])

    # Kaspi Stores table
    op.execute("""
        CREATE TABLE kaspi_stores (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            merchant_id VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            api_key VARCHAR(255),
            guid JSONB,
            products_count INTEGER DEFAULT 0,
            last_sync TIMESTAMPTZ,
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_kaspi_stores_user_id', 'kaspi_stores', ['user_id'])
    op.create_index('idx_kaspi_stores_merchant_id', 'kaspi_stores', ['merchant_id'])

    # Products table - PRICE IN TIYNS!
    op.execute("""
        CREATE TABLE products (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            store_id UUID REFERENCES kaspi_stores(id) ON DELETE CASCADE,
            kaspi_product_id VARCHAR(255) NOT NULL,
            kaspi_sku VARCHAR(255),
            external_kaspi_id VARCHAR(255),
            name VARCHAR(500) NOT NULL,
            price INTEGER NOT NULL,
            min_profit INTEGER DEFAULT 0,
            bot_active BOOLEAN DEFAULT true,
            last_check_time TIMESTAMPTZ,
            availabilities JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    # Composite index for demper performance
    op.execute("""
        CREATE INDEX idx_products_demper
        ON products(bot_active, last_check_time)
        WHERE bot_active = true
    """)
    op.create_index('idx_products_store_id', 'products', ['store_id'])
    op.create_index('idx_products_kaspi_sku', 'products', ['kaspi_sku'])

    # Price History table
    op.execute("""
        CREATE TABLE price_history (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            product_id UUID REFERENCES products(id) ON DELETE CASCADE,
            old_price INTEGER NOT NULL,
            new_price INTEGER NOT NULL,
            competitor_price INTEGER,
            change_reason VARCHAR(50) DEFAULT 'manual',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_price_history_product_id', 'price_history', ['product_id'])
    op.create_index('idx_price_history_created_at', 'price_history', ['created_at'])

    # Preorders table
    op.execute("""
        CREATE TABLE preorders (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            store_id UUID REFERENCES kaspi_stores(id) ON DELETE CASCADE,
            product_id UUID REFERENCES products(id) ON DELETE CASCADE,
            article VARCHAR(255) NOT NULL,
            name VARCHAR(500) NOT NULL,
            price INTEGER NOT NULL,
            warehouses JSONB NOT NULL,
            delivery_days INTEGER DEFAULT 30,
            status VARCHAR(50) DEFAULT 'added',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(store_id, product_id)
        )
    """)
    op.create_index('idx_preorders_store_id', 'preorders', ['store_id'])

    # WhatsApp Sessions table (WAHA Core - one container per user)
    op.execute("""
        CREATE TABLE whatsapp_sessions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE UNIQUE,
            waha_container_name VARCHAR(255) UNIQUE NOT NULL,
            waha_api_key VARCHAR(255) NOT NULL,
            waha_port INTEGER,
            session_name VARCHAR(255) DEFAULT 'default',
            phone_number VARCHAR(20),
            status VARCHAR(20) DEFAULT 'qr_pending',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_whatsapp_sessions_user_id', 'whatsapp_sessions', ['user_id'])

    # WhatsApp Templates table (text only - no media in Core)
    op.execute("""
        CREATE TABLE whatsapp_templates (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            name VARCHAR(255) NOT NULL,
            message TEXT NOT NULL,
            variables JSONB,
            trigger_event VARCHAR(50),
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_whatsapp_templates_user_id', 'whatsapp_templates', ['user_id'])

    # AI Chat History table
    op.execute("""
        CREATE TABLE ai_chat_history (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            assistant_type VARCHAR(50) NOT NULL,
            role VARCHAR(20) NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_ai_chat_history_user_id', 'ai_chat_history', ['user_id'])
    op.create_index('idx_ai_chat_history_created_at', 'ai_chat_history', ['created_at'])

    # Subscriptions table
    op.execute("""
        CREATE TABLE subscriptions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            plan VARCHAR(50) NOT NULL,
            status VARCHAR(20) DEFAULT 'active',
            products_limit INTEGER DEFAULT 500,
            current_period_start TIMESTAMPTZ NOT NULL,
            current_period_end TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_subscriptions_user_id', 'subscriptions', ['user_id'])

    # Payments table
    op.execute("""
        CREATE TABLE payments (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            amount INTEGER NOT NULL,
            status VARCHAR(20) DEFAULT 'pending',
            plan VARCHAR(50),
            tiptoppay_transaction_id VARCHAR(255),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_payments_user_id', 'payments', ['user_id'])
    op.create_index('idx_payments_status', 'payments', ['status'])

    # Create updated_at trigger function
    op.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ language 'plpgsql';
    """)

    # Add updated_at triggers to all tables with updated_at column
    tables_with_updated_at = [
        'users', 'kaspi_stores', 'products', 'preorders',
        'whatsapp_sessions', 'whatsapp_templates', 'subscriptions', 'payments'
    ]

    for table in tables_with_updated_at:
        op.execute(f"""
            CREATE TRIGGER update_{table}_updated_at
            BEFORE UPDATE ON {table}
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """)


def downgrade() -> None:
    # Drop tables (triggers will be dropped automatically with CASCADE)
    op.drop_table('payments')
    op.drop_table('subscriptions')
    op.drop_table('ai_chat_history')
    op.drop_table('whatsapp_templates')
    op.drop_table('whatsapp_sessions')
    op.drop_table('preorders')
    op.drop_table('price_history')
    op.drop_table('products')
    op.drop_table('kaspi_stores')
    op.drop_table('users')

    # Drop trigger function
    op.execute("DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE")
