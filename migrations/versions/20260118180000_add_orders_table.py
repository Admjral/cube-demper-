"""Add orders table for sales analytics

Revision ID: 20260118180000
Revises: 20260118170000
Create Date: 2026-01-18 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260118180000'
down_revision: Union[str, None] = '20260118170000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Orders table - stores Kaspi orders for analytics
    op.execute("""
        CREATE TABLE orders (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            store_id UUID REFERENCES kaspi_stores(id) ON DELETE CASCADE,
            kaspi_order_id VARCHAR(255) NOT NULL,
            kaspi_order_code VARCHAR(255),
            status VARCHAR(50) NOT NULL,
            total_price INTEGER NOT NULL,
            delivery_cost INTEGER DEFAULT 0,
            customer_name VARCHAR(255),
            customer_phone VARCHAR(50),
            delivery_address TEXT,
            delivery_mode VARCHAR(50),
            payment_mode VARCHAR(50),
            order_date TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(store_id, kaspi_order_id)
        )
    """)
    op.create_index('idx_orders_store_id', 'orders', ['store_id'])
    op.create_index('idx_orders_order_date', 'orders', ['order_date'])
    op.create_index('idx_orders_status', 'orders', ['status'])

    # Order items table - individual items in each order
    op.execute("""
        CREATE TABLE order_items (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            order_id UUID REFERENCES orders(id) ON DELETE CASCADE,
            product_id UUID REFERENCES products(id) ON DELETE SET NULL,
            kaspi_product_id VARCHAR(255),
            name VARCHAR(500) NOT NULL,
            sku VARCHAR(255),
            quantity INTEGER NOT NULL DEFAULT 1,
            price INTEGER NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.create_index('idx_order_items_order_id', 'order_items', ['order_id'])
    op.create_index('idx_order_items_product_id', 'order_items', ['product_id'])

    # Add last_orders_sync to kaspi_stores
    op.add_column('kaspi_stores', sa.Column('last_orders_sync', sa.DateTime(timezone=True), nullable=True))

    # Add updated_at trigger
    op.execute("""
        CREATE TRIGGER update_orders_updated_at
        BEFORE UPDATE ON orders
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS update_orders_updated_at ON orders")
    op.drop_column('kaspi_stores', 'last_orders_sync')
    op.drop_table('order_items')
    op.drop_table('orders')
