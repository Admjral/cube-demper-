"""Add proxy pool tables for per-user proxy allocation with module support

Revision ID: 20260131000000
Revises: 20260129100000
Create Date: 2026-01-31 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260131000000'
down_revision: Union[str, None] = '20260129100000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create proxies table with per-module allocation support
    op.execute("""
        CREATE TABLE proxies (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

            -- Proxy connection details
            host VARCHAR(255) NOT NULL,
            port INTEGER NOT NULL,
            protocol VARCHAR(10) DEFAULT 'http',
            username VARCHAR(255),
            password VARCHAR(255),

            -- User allocation
            user_id UUID REFERENCES users(id) ON DELETE SET NULL,
            allocated_at TIMESTAMPTZ,
            status VARCHAR(20) DEFAULT 'available',

            -- Per-module proxy pools (70 demper, 25 orders, 5 catalog, 0 reserve)
            module VARCHAR(20) DEFAULT NULL,

            -- Rotation tracking
            requests_count INTEGER DEFAULT 0,
            max_requests INTEGER DEFAULT 249,
            last_used_at TIMESTAMPTZ,
            available_at TIMESTAMPTZ,

            -- Health monitoring
            success_count INTEGER DEFAULT 0,
            failure_count INTEGER DEFAULT 0,
            last_error TEXT,
            last_health_check TIMESTAMPTZ,

            -- Metadata
            country VARCHAR(10) DEFAULT 'NL',
            provider VARCHAR(50),
            cost_usd DECIMAL(10,4),
            is_residential BOOLEAN DEFAULT FALSE,

            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),

            UNIQUE (host, port)
        )
    """)

    # Create indexes for efficient proxy lookup
    op.create_index(
        'idx_proxies_user_status',
        'proxies',
        ['user_id', 'status', 'available_at']
    )

    op.create_index(
        'idx_proxies_user_module',
        'proxies',
        ['user_id', 'module', 'status', 'available_at']
    )

    # Partial index for fast available proxy lookup
    op.execute("""
        CREATE INDEX idx_proxies_available
        ON proxies (status, user_id)
        WHERE status = 'available'
    """)

    # Create proxy_usage_log table for analytics
    op.execute("""
        CREATE TABLE proxy_usage_log (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            proxy_id UUID REFERENCES proxies(id) ON DELETE CASCADE,
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            module VARCHAR(20),

            requests_made INTEGER,
            success_count INTEGER,
            failure_count INTEGER,

            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ
        )
    """)

    op.create_index(
        'idx_usage_proxy_time',
        'proxy_usage_log',
        [sa.text('proxy_id'), sa.text('started_at DESC')]
    )

    op.create_index(
        'idx_usage_user_time',
        'proxy_usage_log',
        [sa.text('user_id'), sa.text('started_at DESC')]
    )

    # Add updated_at trigger for proxies
    op.execute("""
        CREATE TRIGGER update_proxies_updated_at
        BEFORE UPDATE ON proxies
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS update_proxies_updated_at ON proxies")
    op.drop_table('proxy_usage_log')
    op.drop_table('proxies')
