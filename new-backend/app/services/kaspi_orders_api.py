"""
Kaspi Open API (REST) — сервис для работы с заказами через X-Auth-Token.

Отличия от MC GraphQL:
- Авторизация через X-Auth-Token (не session cookies)
- Возвращает реальный customer.cellPhone (не маскированный)
- Geo-restricted: работает только с казахстанских IP
- Формат JSON:API
"""
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

import httpx
import asyncpg

from ..config import settings

logger = logging.getLogger(__name__)


class KaspiTokenInvalidError(Exception):
    """API token is invalid or expired (401/403)"""
    pass


class KaspiOrdersAPIError(Exception):
    """Generic Kaspi Orders API error"""
    pass


class KaspiOrdersAPI:
    BASE_URL = "https://kaspi.kz/shop/api/v2/orders"
    TIMEOUT = 30.0

    def _get_headers(self, api_token: str) -> dict:
        return {
            "X-Auth-Token": api_token,
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
        }

    def _get_client_kwargs(self) -> dict:
        kwargs: dict = {"timeout": self.TIMEOUT}
        proxy = getattr(settings, 'kaspi_api_proxy', None)
        if proxy:
            kwargs["proxy"] = proxy
        return kwargs

    async def fetch_orders(
        self,
        api_token: str,
        date_from: datetime,
        date_to: datetime,
        states: List[str],
        page: int = 0,
        size: int = 100,
    ) -> List[dict]:
        """
        Fetch orders from Kaspi REST API.

        Args:
            api_token: X-Auth-Token from Kaspi MC settings
            date_from: Start date filter
            date_to: End date filter
            states: List of order states to filter
            page: Page number (0-based)
            size: Page size (max 100)

        Returns:
            List of order data dicts (JSON:API format)

        Raises:
            KaspiTokenInvalidError: If token is invalid/expired
            KaspiOrdersAPIError: If API call fails
        """
        headers = self._get_headers(api_token)
        params = {
            "page[number]": page,
            "page[size]": size,
            "filter[orders][creationDate][$ge]": int(date_from.timestamp() * 1000),
            "filter[orders][creationDate][$le]": int(date_to.timestamp() * 1000),
            "filter[orders][state]": ",".join(states),
        }

        all_orders = []
        max_pages = 50

        try:
            async with httpx.AsyncClient(**self._get_client_kwargs()) as client:
                while page < max_pages:
                    params["page[number]"] = page

                    response = await client.get(
                        self.BASE_URL,
                        headers=headers,
                        params=params,
                    )

                    if response.status_code in (401, 403):
                        raise KaspiTokenInvalidError(
                            f"API token invalid or expired (HTTP {response.status_code})"
                        )

                    if response.status_code != 200:
                        logger.warning(f"Kaspi REST API error: {response.status_code}")
                        raise KaspiOrdersAPIError(f"HTTP {response.status_code}")

                    data = response.json()
                    orders = data.get("data", [])

                    if not orders:
                        break

                    all_orders.extend(orders)

                    # Check if there are more pages
                    total_pages = data.get("meta", {}).get("totalPages", 1)
                    page += 1
                    if page >= total_pages:
                        break

        except httpx.TimeoutException:
            logger.warning("Kaspi REST API timeout (likely geo-restricted, need KZ IP)")
            raise KaspiOrdersAPIError("Request timeout — API may be geo-restricted to KZ")
        except httpx.RequestError as e:
            logger.error(f"Kaspi REST API network error: {e}")
            raise KaspiOrdersAPIError(f"Network error: {e}")

        logger.info(f"Fetched {len(all_orders)} orders via REST API")
        return all_orders

    async def fetch_order_detail(
        self,
        api_token: str,
        order_id: str,
    ) -> Optional[dict]:
        """
        Fetch single order detail from Kaspi REST API.

        Args:
            api_token: X-Auth-Token
            order_id: Kaspi order ID (from JSON:API 'id' field)

        Returns:
            Order data dict or None

        Raises:
            KaspiTokenInvalidError: If token is invalid/expired
        """
        headers = self._get_headers(api_token)
        url = f"{self.BASE_URL}/{order_id}"

        try:
            async with httpx.AsyncClient(**self._get_client_kwargs()) as client:
                response = await client.get(url, headers=headers)

                if response.status_code in (401, 403):
                    raise KaspiTokenInvalidError(
                        f"API token invalid or expired (HTTP {response.status_code})"
                    )

                if response.status_code == 404:
                    return None

                if response.status_code != 200:
                    logger.warning(f"Kaspi REST API error for order {order_id}: {response.status_code}")
                    return None

                data = response.json()
                return data.get("data")

        except httpx.TimeoutException:
            logger.warning(f"Timeout fetching order {order_id} (geo-restricted?)")
            return None
        except httpx.RequestError as e:
            logger.error(f"Network error fetching order {order_id}: {e}")
            return None

    async def get_customer_phone(
        self,
        api_token: str,
        order_code: str,
        pool: asyncpg.Pool,
    ) -> Optional[Dict[str, Any]]:
        """
        Get real customer phone number for an order via REST API.

        Returns same format as kaspi_mc_service.get_order_customer_phone()
        for drop-in replacement.

        Args:
            api_token: X-Auth-Token
            order_code: Kaspi order code (e.g. "790686780")
            pool: DB connection pool

        Returns:
            Dict with phone, first_name, last_name, full_name, etc. or None

        Raises:
            KaspiTokenInvalidError: If token is invalid/expired
        """
        # Find kaspi_order_id by order_code in DB
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT kaspi_order_id
                FROM orders
                WHERE kaspi_order_code = $1
                LIMIT 1
            """, order_code)

        order_id = row['kaspi_order_id'] if row else None

        if not order_id:
            # Try fetching orders list to find the order
            logger.debug(f"Order {order_code} not in DB, searching via API")
            now = datetime.utcnow()
            orders = await self.fetch_orders(
                api_token=api_token,
                date_from=now - timedelta(days=14),
                date_to=now,
                states=["APPROVED", "ACCEPTED_BY_MERCHANT", "DELIVERY", "DELIVERED", "COMPLETED"],
                size=100,
            )
            for order in orders:
                if order.get("attributes", {}).get("code") == order_code:
                    order_id = order.get("id")
                    break

        if not order_id:
            logger.warning(f"Could not find order_id for code {order_code}")
            return None

        # Fetch order detail with real phone
        order_data = await self.fetch_order_detail(api_token, order_id)
        if not order_data:
            return None

        # Parse using the same structure as api_parser.parse_order_details
        attributes = order_data.get("attributes", {})
        customer = attributes.get("customer", {})
        delivery = attributes.get("deliveryAddress", {}) or {}

        raw_phone = customer.get("cellPhone", "")
        if not raw_phone:
            return None

        # Format phone: cellPhone comes as "77XXXXXXXXX" (11 digits)
        digits = "".join(filter(str.isdigit, str(raw_phone)))
        if len(digits) == 11 and digits.startswith('7'):
            formatted_phone = f"+{digits}"
        elif len(digits) == 10:
            formatted_phone = f"+7{digits}"
        else:
            formatted_phone = f"+7{digits}" if digits else None

        if not formatted_phone:
            return None

        # Build address
        address_parts = []
        formatted_address = delivery.get("formattedAddress", "")
        if formatted_address:
            address_parts.append(formatted_address)

        # Parse entries
        entries = []
        for entry in attributes.get("entries", []):
            entries.append({
                "productName": entry.get("product", {}).get("name", ""),
                "quantity": entry.get("quantity", 1),
                "basePrice": entry.get("basePrice", 0),
            })

        return {
            "phone": formatted_phone,
            "phone_raw": digits,
            "first_name": customer.get("firstName"),
            "last_name": customer.get("lastName"),
            "full_name": f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip() or None,
            "order_code": attributes.get("code", order_code),
            "order_state": attributes.get("state"),
            "order_total": attributes.get("totalPrice"),
            "order_date": attributes.get("creationDate"),
            "items": entries,
            "delivery_address": ", ".join(address_parts) if address_parts else None,
            "delivery_city": delivery.get("city") if isinstance(delivery, dict) else None,
        }


# Singleton
_kaspi_orders_api: Optional[KaspiOrdersAPI] = None


def get_kaspi_orders_api() -> KaspiOrdersAPI:
    global _kaspi_orders_api
    if _kaspi_orders_api is None:
        _kaspi_orders_api = KaspiOrdersAPI()
    return _kaspi_orders_api
