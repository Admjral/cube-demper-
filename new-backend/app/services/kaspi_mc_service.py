"""
Kaspi MC (Merchant Cabinet) Service - парсинг данных через GraphQL API

Работает с mc.shop.kaspi.kz для получения:
- Номера телефона покупателя по заказу
- Детали заказа
- Статусы заказов

Требует авторизованную сессию из kaspi_auth_service.
"""
import logging
import asyncio
import uuid as uuid_module
from typing import Optional, Dict, Any, List
from datetime import datetime

import httpx
import asyncpg

from ..config import settings
from ..core.database import get_db_pool
from .kaspi_auth_service import get_active_session, get_active_session_with_refresh, KaspiAuthError

logger = logging.getLogger(__name__)


class KaspiMCError(Exception):
    """Base exception for Kaspi MC errors"""
    pass


class KaspiMCService:
    """
    Сервис для работы с Kaspi Merchant Cabinet через GraphQL API.

    Позволяет получать детали заказов, включая номера телефонов покупателей,
    которые недоступны через обычное API.
    """

    MC_GRAPHQL_URL = "https://mc.shop.kaspi.kz/mc/facade/graphql"

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def _get_session_for_store(self, user_id: str, store_id: str, pool: asyncpg.Pool) -> dict:
        """Get active session by looking up merchant_id from store_id"""
        async with pool.acquire() as conn:
            store = await conn.fetchrow(
                "SELECT merchant_id FROM kaspi_stores WHERE id = $1 AND user_id = $2",
                uuid_module.UUID(store_id), uuid_module.UUID(user_id)
            )
            if not store:
                raise KaspiMCError("Store not found")
        session = await get_active_session(store['merchant_id'])
        if not session:
            raise KaspiMCError("No active Kaspi session. Please login first.")
        return session

    def _format_cookies_header(self, cookies: list) -> str:
        """Format cookies list to Cookie header string"""
        if isinstance(cookies, list):
            return "; ".join([
                f"{c.get('name', '')}={c.get('value', '')}"
                for c in cookies if isinstance(c, dict)
            ])
        elif isinstance(cookies, dict):
            return "; ".join([f"{k}={v}" for k, v in cookies.items()])
        return ""

    def _cookies_to_dict(self, cookies) -> dict:
        """Convert cookies list to dict for httpx cookies parameter"""
        if isinstance(cookies, list):
            return {
                c['name']: c['value']
                for c in cookies
                if isinstance(c, dict) and c.get('name') and c.get('value')
            }
        elif isinstance(cookies, dict):
            return cookies
        return {}

    def _mc_headers(self) -> dict:
        """Standard headers for MC GraphQL requests"""
        return {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "x-auth-version": "3",
            "Origin": "https://kaspi.kz",
            "Referer": "https://kaspi.kz/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }

    async def get_order_customer_phone(
        self,
        user_id: str,
        store_id: str,
        order_code: str,
        pool: asyncpg.Pool,
    ) -> Optional[Dict[str, Any]]:
        """
        Получить номер телефона покупателя по коду заказа.

        Args:
            user_id: ID пользователя
            store_id: ID магазина
            order_code: Код заказа (например, "790686780")
            pool: Пул соединений к БД

        Returns:
            Dict с данными покупателя: {phone, first_name, last_name}
        """
        # Получаем активную сессию для магазина
        session = await self._get_session_for_store(user_id, store_id, pool)

        # Получаем merchant_uid из сессии
        merchant_uid = session.get('merchant_uid')
        if not merchant_uid:
            raise KaspiMCError("Merchant UID not found in session")

        cookies = session.get('cookies', [])

        # GraphQL запрос для получения деталей заказа
        query = """
        query getOrderDetails($orderCode: String!) {
            merchant(id: "%s") {
                orderDetail(code: $orderCode) {
                    code
                    state
                    createdAt
                    customer {
                        phoneNumber
                        firstName
                        lastName
                    }
                    entries {
                        productName
                        quantity
                        basePrice
                    }
                    totalPrice
                    deliveryAddress {
                        city
                        street
                        building
                        apartment
                    }
                }
            }
        }
        """ % merchant_uid

        payload = {
            "query": query,
            "variables": {"orderCode": order_code},
            "operationName": "getOrderDetails"
        }

        cookies_dict = self._cookies_to_dict(cookies)
        headers = self._mc_headers()

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.MC_GRAPHQL_URL}?opName=getOrderDetails",
                    json=payload,
                    headers=headers,
                    cookies=cookies_dict,
                )

                if response.status_code != 200:
                    logger.error(f"MC GraphQL error: {response.status_code} - {response.text}")
                    raise KaspiMCError(f"MC API error: {response.status_code}")

                data = response.json()

                if "errors" in data:
                    error_msg = data["errors"][0].get("message", "Unknown error")
                    logger.error(f"GraphQL error: {error_msg}")
                    raise KaspiMCError(f"GraphQL error: {error_msg}")

                order_detail = data.get("data", {}).get("merchant", {}).get("orderDetail")

                if not order_detail:
                    logger.warning(f"Order {order_code} not found")
                    return None

                customer = order_detail.get("customer", {})
                delivery = order_detail.get("deliveryAddress", {}) or {}

                # Форматируем адрес доставки
                address_parts = []
                if delivery.get('city'):
                    address_parts.append(delivery['city'])
                if delivery.get('street'):
                    address_parts.append(delivery['street'])
                if delivery.get('building'):
                    address_parts.append(f"д. {delivery['building']}")
                if delivery.get('apartment'):
                    address_parts.append(f"кв. {delivery['apartment']}")

                # Форматируем телефон: Kaspi MC возвращает 10 цифр (без +7)
                # но на всякий случай защищаемся от 11 цифр с ведущей 7
                raw_phone = customer.get('phoneNumber', '')
                if raw_phone:
                    digits = "".join(filter(str.isdigit, raw_phone))
                    if len(digits) == 11 and digits.startswith('7'):
                        formatted_phone = f"+{digits}"
                    elif len(digits) == 10:
                        formatted_phone = f"+7{digits}"
                    else:
                        formatted_phone = f"+7{digits}"
                else:
                    formatted_phone = None

                return {
                    "phone": formatted_phone,
                    "phone_raw": raw_phone,
                    "first_name": customer.get('firstName'),
                    "last_name": customer.get('lastName'),
                    "full_name": f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip() or None,
                    "order_code": order_detail.get('code'),
                    "order_state": order_detail.get('state'),
                    "order_total": order_detail.get('totalPrice'),
                    "order_date": order_detail.get('createdAt'),
                    "items": order_detail.get('entries', []),
                    "delivery_address": ", ".join(address_parts) if address_parts else None,
                    "delivery_city": delivery.get('city'),
                }

        except httpx.RequestError as e:
            logger.error(f"HTTP error fetching order details: {e}")
            raise KaspiMCError(f"Network error: {str(e)}")

    async def get_recent_orders(
        self,
        user_id: str,
        store_id: str,
        pool: asyncpg.Pool,
        limit: int = 50,
        tab_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Получить список активных заказов магазина.

        MC GraphQL only shows active orders via presetFilter tabs:
        NEW, DELIVERY, PICKUP, SIGN_REQUIRED

        Args:
            user_id: ID пользователя
            store_id: ID магазина
            pool: Пул соединений к БД
            limit: Максимальное количество заказов
            tab_filter: Фильтр по табу (NEW, DELIVERY, PICKUP, SIGN_REQUIRED)

        Returns:
            Список заказов с базовой информацией
        """
        session = await self._get_session_for_store(user_id, store_id, pool)

        merchant_uid = session.get('merchant_uid')
        if not merchant_uid:
            raise KaspiMCError("Merchant UID not found")

        cookies = session.get('cookies', [])
        cookies_dict = self._cookies_to_dict(cookies)
        headers = self._mc_headers()

        # Query specified tab or all active tabs
        tabs = [tab_filter] if tab_filter else ["NEW", "DELIVERY", "PICKUP", "SIGN_REQUIRED"]

        all_orders = []
        seen_codes = set()

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                for tab in tabs:
                    query = """
                    {
                        merchant(id: "%s") {
                            orders {
                                orders(input: { presetFilter: %s }) {
                                    total
                                    orders {
                                        code
                                        totalPrice
                                        status
                                    }
                                }
                            }
                        }
                    }
                    """ % (merchant_uid, tab)

                    response = await client.post(
                        f"{self.MC_GRAPHQL_URL}?opName=getOrders",
                        json={"query": query},
                        headers=headers,
                        cookies=cookies_dict,
                    )

                    if response.status_code != 200:
                        logger.warning(f"MC API error for tab {tab}: {response.status_code}")
                        continue

                    data = response.json()
                    if "errors" in data:
                        logger.warning(f"GraphQL error for tab {tab}: {data['errors'][0].get('message', '')}")
                        continue

                    orders_page = (data.get("data", {}).get("merchant", {})
                                   .get("orders", {}).get("orders", {}))
                    orders_list = orders_page.get("orders", [])

                    for order in orders_list:
                        code = order.get("code")
                        if code and code not in seen_codes:
                            seen_codes.add(code)
                            all_orders.append({
                                "code": code,
                                "status": order.get("status"),
                                "total_price": order.get("totalPrice"),
                                "tab": tab,
                            })

                    if len(all_orders) >= limit:
                        break

            return all_orders[:limit]

        except httpx.RequestError as e:
            logger.error(f"HTTP error fetching orders: {e}")
            raise KaspiMCError(f"Network error: {str(e)}")

    async def fetch_orders_for_sync(
        self,
        merchant_id: str,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Fetch active orders via MC GraphQL for sync_orders_to_db.

        Queries all active order tabs (NEW, DELIVERY, PICKUP, SIGN_REQUIRED),
        then enriches each order via orderDetail to get customer info & entries.

        Returns data in Kaspi Open API format so sync_orders_to_db() works unchanged.

        Note: MC GraphQL only shows active orders. Completed/archived orders
        are not available through this API.
        """
        session = await get_active_session_with_refresh(merchant_id)
        if not session:
            raise KaspiMCError(f"No active session for merchant {merchant_id}")

        merchant_uid = session.get('merchant_uid')
        if not merchant_uid:
            raise KaspiMCError("Merchant UID not found in session")

        cookies = session.get('cookies', [])
        cookies_dict = self._cookies_to_dict(cookies)
        headers = self._mc_headers()

        # Step 1: Collect order codes from all active tabs
        all_order_codes = []
        seen_codes = set()

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                for tab in ["NEW", "DELIVERY", "PICKUP", "SIGN_REQUIRED"]:
                    query = """
                    {
                        merchant(id: "%s") {
                            orders {
                                orders(input: { presetFilter: %s }) {
                                    total
                                    orders {
                                        code
                                        totalPrice
                                        status
                                    }
                                }
                            }
                        }
                    }
                    """ % (merchant_uid, tab)

                    response = await client.post(
                        f"{self.MC_GRAPHQL_URL}?opName=getOrdersForSync",
                        json={"query": query},
                        headers=headers,
                        cookies=cookies_dict,
                    )

                    if response.status_code != 200:
                        logger.warning(f"MC orders tab {tab}: HTTP {response.status_code}")
                        continue

                    data = response.json()
                    if "errors" in data:
                        logger.warning(f"MC orders tab {tab}: {data['errors'][0].get('message', '')}")
                        continue

                    orders_page = (data.get("data", {}).get("merchant", {})
                                   .get("orders", {}).get("orders", {}))
                    orders_list = orders_page.get("orders", [])
                    total = orders_page.get("total", 0)

                    logger.info(f"MC orders tab {tab}: {total} total, {len(orders_list)} returned")

                    for order in orders_list:
                        code = order.get("code")
                        if code and code not in seen_codes:
                            seen_codes.add(code)
                            all_order_codes.append(code)

                    if len(all_order_codes) >= limit:
                        break

                if not all_order_codes:
                    logger.info(f"No active orders found for merchant {merchant_id}")
                    return []

                # Step 2: Get details for each order via orderDetail
                logger.info(f"Fetching details for {len(all_order_codes[:limit])} orders")
                result = []

                for code in all_order_codes[:limit]:
                    detail_query = """
                    {
                        merchant(id: "%s") {
                            orderDetail(code: "%s") {
                                code
                                status
                                totalPrice
                                customer {
                                    phoneNumber
                                    firstName
                                    lastName
                                }
                                entries {
                                    productName
                                    quantity
                                    basePrice
                                }
                                deliveryAddress {
                                    city
                                    street
                                    building
                                    apartment
                                }
                            }
                        }
                    }
                    """ % (merchant_uid, code)

                    resp = await client.post(
                        f"{self.MC_GRAPHQL_URL}?opName=getOrderDetail",
                        json={"query": detail_query},
                        headers=headers,
                        cookies=cookies_dict,
                    )

                    if resp.status_code != 200:
                        logger.warning(f"orderDetail {code}: HTTP {resp.status_code}")
                        continue

                    detail_data = resp.json()
                    if "errors" in detail_data:
                        logger.warning(f"orderDetail {code}: {detail_data['errors'][0].get('message', '')}")
                        continue

                    detail = (detail_data.get("data", {}).get("merchant", {})
                              .get("orderDetail"))
                    if not detail:
                        continue

                    # Convert to Open API compatible format
                    customer = detail.get("customer", {}) or {}
                    entries = detail.get("entries", []) or []
                    delivery = detail.get("deliveryAddress", {}) or {}

                    # Format phone
                    raw_phone = customer.get("phoneNumber", "")
                    phone = ""
                    if raw_phone:
                        digits = "".join(filter(str.isdigit, raw_phone))
                        if len(digits) == 11 and digits.startswith('7'):
                            phone = digits
                        elif len(digits) == 10:
                            phone = f"7{digits}"
                        else:
                            phone = digits

                    # Format address
                    addr_parts = []
                    for key in ['city', 'street', 'building']:
                        if delivery.get(key):
                            addr_parts.append(delivery[key])
                    formatted_addr = ", ".join(addr_parts) if addr_parts else ""

                    order_code = detail.get("code", code)
                    creation_ts = int(datetime.utcnow().timestamp() * 1000)

                    order = {
                        "id": order_code,
                        "attributes": {
                            "code": order_code,
                            "state": detail.get("status", ""),
                            "totalPrice": detail.get("totalPrice", 0),
                            "deliveryCost": 0,
                            "deliveryMode": "",
                            "paymentMode": "",
                            "creationDate": creation_ts,
                            "customer": {
                                "firstName": customer.get("firstName", ""),
                                "lastName": customer.get("lastName", ""),
                                "cellPhone": phone,
                            },
                            "deliveryAddress": {
                                "formattedAddress": formatted_addr,
                            },
                            "entries": [
                                {
                                    "product": {
                                        "code": "",
                                        "sku": "",
                                        "name": e.get("productName", ""),
                                    },
                                    "quantity": e.get("quantity", 1),
                                    "basePrice": e.get("basePrice", 0),
                                }
                                for e in entries
                            ],
                        },
                    }
                    result.append(order)

                    # Small delay between requests to avoid rate limiting
                    await asyncio.sleep(0.1)

                logger.info(f"Fetched {len(result)} order details for merchant {merchant_id}")
                return result

        except httpx.RequestError as e:
            logger.error(f"HTTP error fetching orders for sync: {e}")
            raise KaspiMCError(f"Network error: {str(e)}")


# Singleton instance
_kaspi_mc_service: Optional[KaspiMCService] = None


def get_kaspi_mc_service() -> KaspiMCService:
    """Get singleton instance of KaspiMCService"""
    global _kaspi_mc_service

    if _kaspi_mc_service is None:
        _kaspi_mc_service = KaspiMCService()

    return _kaspi_mc_service
