"""
Proxy Provider - Integration with Proxy6.net API

Handles automatic proxy purchasing from Proxy6.net:
- Netherlands IPv6 proxies (confirmed working with Kaspi!)
- $11 per 100 proxies per 30 days
- HTTP protocol
"""

import httpx
import logging
from typing import List
from datetime import datetime

from ..config import settings
from ..core.database import get_db_pool
from ..models.proxy import ProxyCreate

logger = logging.getLogger(__name__)


class ProxyProviderError(Exception):
    """Base exception for proxy provider errors"""
    pass


class ProxyPurchaseError(ProxyProviderError):
    """Raised when proxy purchase fails"""
    pass


class InsufficientFundsError(ProxyProviderError):
    """Raised when not enough balance"""
    pass


class Proxy6Client:
    """
    Client for Proxy6.net API

    Docs: https://proxy6.net/developers
    """

    def __init__(self):
        if not settings.proxy6_api_key:
            raise ValueError("PROXY6_API_KEY not configured")

        self.api_key = settings.proxy6_api_key
        self.base_url = "https://proxy6.net/api"

    async def purchase_proxies(
        self,
        count: int,
        period_days: int = 30,
        country: str = "nl",  # Netherlands
        version: int = 6  # IPv6
    ) -> List[ProxyCreate]:
        """
        Purchase proxies from Proxy6.net

        Args:
            count: Number of proxies to buy
            period_days: Rental period (default 30 days)
            country: Country code (default 'nl' for Netherlands)
            version: IP version (4 or 6, default 6 for IPv6)

        Returns:
            List of ProxyCreate schemas

        Raises:
            ProxyPurchaseError: If purchase fails
        """
        url = f"{self.base_url}/{self.api_key}/buy/{count}/{period_days}/{country}/{version}"

        logger.info(
            f"Purchasing {count} IPv{version} proxies from {country.upper()} "
            f"for {period_days} days"
        )

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()

                if data.get('status') != 'yes':
                    error_msg = data.get('error', 'Unknown error')
                    raise ProxyPurchaseError(
                        f"Failed to purchase proxies: {error_msg}"
                    )

                # Parse purchased proxies
                proxies = []
                proxy_list = data.get('list', {})

                for proxy_id, proxy_data in proxy_list.items():
                    proxy = ProxyCreate(
                        host=proxy_data['host'],
                        port=proxy_data['port_http'],
                        protocol='http',
                        username=proxy_data['user'],
                        password=proxy_data['pass'],
                        country=country.upper(),
                        provider='proxy6.net',
                        cost_usd=data.get('price', 0) / count,  # Price per proxy
                        is_residential=False  # Proxy6 provides datacenter IPs
                    )
                    proxies.append(proxy)

                logger.info(
                    f"Successfully purchased {len(proxies)} proxies. "
                    f"Total cost: ${data.get('price', 0):.2f}"
                )

                return proxies

            except httpx.HTTPError as e:
                raise ProxyPurchaseError(f"HTTP error during proxy purchase: {e}")

    async def check_balance(self) -> float:
        """
        Check account balance on Proxy6.net

        Returns:
            Balance in USD

        Raises:
            ProxyProviderError: If balance check fails
        """
        url = f"{self.base_url}/{self.api_key}/getbalance"

        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()

                if data.get('status') == 'yes':
                    balance = float(data.get('balance', 0))
                    logger.info(f"Proxy6.net balance: ${balance:.2f}")
                    return balance

                raise ProxyProviderError(
                    f"Failed to get balance: {data.get('error', 'Unknown error')}"
                )

            except httpx.HTTPError as e:
                raise ProxyProviderError(f"HTTP error during balance check: {e}")

    async def get_proxy_list(self) -> dict:
        """
        Get list of all active proxies

        Returns:
            Dict with proxy information

        Raises:
            ProxyProviderError: If request fails
        """
        url = f"{self.base_url}/{self.api_key}/getproxy"

        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()

                if data.get('status') == 'yes':
                    return data.get('list', {})

                raise ProxyProviderError(
                    f"Failed to get proxy list: {data.get('error', 'Unknown error')}"
                )

            except httpx.HTTPError as e:
                raise ProxyProviderError(f"HTTP error during proxy list fetch: {e}")


async def ensure_proxy_pool_sufficient(required_count: int = None) -> int:
    """
    Ensure proxy pool has enough available proxies

    If pool is low, automatically purchase more proxies (if auto-purchase enabled)

    Args:
        required_count: Minimum required proxies (default from settings)

    Returns:
        Number of proxies added to pool

    Raises:
        InsufficientFundsError: Not enough balance to purchase
        ProxyPurchaseError: Purchase failed
    """
    if required_count is None:
        required_count = settings.proxy_pool_min_size

    pool = await get_db_pool()

    # Check current pool status
    async with pool.acquire() as conn:
        result = await conn.fetchrow(
            """
            SELECT COUNT(*) as available
            FROM proxies
            WHERE status = 'available' AND user_id IS NULL
            """
        )
        available_count = result['available']

    if available_count >= required_count:
        logger.info(
            f"Proxy pool sufficient: {available_count}/{required_count} available"
        )
        return 0

    shortage = required_count - available_count
    logger.warning(
        f"Proxy pool low: {available_count}/{required_count}, "
        f"shortage: {shortage} proxies"
    )

    if not settings.proxy_auto_purchase:
        logger.warning("Auto-purchase disabled, skipping proxy purchase")
        return 0

    # Check balance
    client = Proxy6Client()
    balance = await client.check_balance()

    # Estimate cost ($11 per 100 proxies)
    estimated_cost = (shortage / 100) * 11

    if balance < estimated_cost:
        raise InsufficientFundsError(
            f"Insufficient funds: balance ${balance:.2f}, "
            f"need ${estimated_cost:.2f} for {shortage} proxies"
        )

    # Purchase proxies
    logger.info(f"Purchasing {shortage} proxies (estimated cost: ${estimated_cost:.2f})")

    new_proxies = await client.purchase_proxies(count=shortage)

    # Add to database
    async with pool.acquire() as conn:
        for proxy in new_proxies:
            await conn.execute(
                """
                INSERT INTO proxies (
                    host, port, protocol, username, password,
                    country, provider, cost_usd, is_residential,
                    status
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'available')
                ON CONFLICT (host, port) DO NOTHING
                """,
                proxy.host,
                proxy.port,
                proxy.protocol,
                proxy.username,
                proxy.password,
                proxy.country,
                proxy.provider,
                proxy.cost_usd,
                proxy.is_residential
            )

    logger.info(f"✅ Added {len(new_proxies)} new proxies to pool")

    return len(new_proxies)


# Singleton client
proxy6_client = Proxy6Client() if settings.proxy6_api_key else None
