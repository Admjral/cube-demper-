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
import json

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

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Cookie": self._format_cookies_header(cookies),
            "Origin": "https://mc.shop.kaspi.kz",
            "Referer": f"https://mc.shop.kaspi.kz/orders/{order_code}",
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.MC_GRAPHQL_URL}?opName=getOrderDetails",
                    json=payload,
                    headers=headers,
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
        state_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Получить список последних заказов магазина.

        Args:
            user_id: ID пользователя
            store_id: ID магазина
            pool: Пул соединений к БД
            limit: Максимальное количество заказов
            state_filter: Фильтр по статусу (APPROVED, DELIVERY, COMPLETED и т.д.)

        Returns:
            Список заказов с базовой информацией
        """
        session = await self._get_session_for_store(user_id, store_id, pool)

        merchant_uid = session.get('merchant_uid')
        if not merchant_uid:
            raise KaspiMCError("Merchant UID not found")

        cookies = session.get('cookies', [])

        # Формируем фильтр статуса
        state_clause = f', states: [{state_filter}]' if state_filter else ''

        query = """
        query getOrders($first: Int!) {
            merchant(id: "%s") {
                orders(first: $first%s) {
                    edges {
                        node {
                            code
                            state
                            createdAt
                            totalPrice
                            customer {
                                firstName
                                lastName
                            }
                            entries {
                                productName
                            }
                        }
                    }
                    totalCount
                }
            }
        }
        """ % (merchant_uid, state_clause)

        payload = {
            "query": query,
            "variables": {"first": limit},
            "operationName": "getOrders"
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Cookie": self._format_cookies_header(cookies),
            "Origin": "https://mc.shop.kaspi.kz",
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.MC_GRAPHQL_URL}?opName=getOrders",
                    json=payload,
                    headers=headers,
                )

                if response.status_code != 200:
                    raise KaspiMCError(f"MC API error: {response.status_code}")

                data = response.json()

                if "errors" in data:
                    error_msg = data["errors"][0].get("message", "Unknown error")
                    raise KaspiMCError(f"GraphQL error: {error_msg}")

                orders_data = data.get("data", {}).get("merchant", {}).get("orders", {})
                edges = orders_data.get("edges", [])

                orders = []
                for edge in edges:
                    node = edge.get("node", {})
                    customer = node.get("customer", {})
                    entries = node.get("entries", [])

                    orders.append({
                        "code": node.get("code"),
                        "state": node.get("state"),
                        "created_at": node.get("createdAt"),
                        "total_price": node.get("totalPrice"),
                        "customer_name": f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip(),
                        "items_count": len(entries),
                        "first_item": entries[0].get("productName") if entries else None,
                    })

                return orders

        except httpx.RequestError as e:
            logger.error(f"HTTP error fetching orders: {e}")
            raise KaspiMCError(f"Network error: {str(e)}")

    async def fetch_orders_for_sync(
        self,
        merchant_id: str,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Fetch orders via MC GraphQL for sync_orders_to_db.

        Returns data in the same format as Kaspi Open API so that
        parse_order_details() and sync_orders_to_db() work without changes.
        """
        session = await get_active_session_with_refresh(merchant_id)
        if not session:
            raise KaspiMCError(f"No active session for merchant {merchant_id}")

        merchant_uid = session.get('merchant_uid')
        if not merchant_uid:
            raise KaspiMCError("Merchant UID not found in session")

        cookies = session.get('cookies', [])

        # Use introspection-safe query: first try to discover the orders schema
        # MC GraphQL orders type is not Relay-style (no edges/node)
        query = """
        query getOrdersForSync {
            merchant(id: "%s") {
                orders {
                    __typename
                }
            }
        }
        """ % merchant_uid

        payload = {
            "query": query,
            "variables": {},
            "operationName": "getOrdersForSync"
        }

        # Format cookies as dict for httpx (same as _get_merchant_info in auth service)
        cookies_dict = {}
        if isinstance(cookies, list):
            for c in cookies:
                if isinstance(c, dict) and c.get('name') and c.get('value'):
                    cookies_dict[c['name']] = c['value']
        elif isinstance(cookies, dict):
            cookies_dict = cookies

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "x-auth-version": "3",
            "Origin": "https://kaspi.kz",
            "Referer": "https://kaspi.kz/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.MC_GRAPHQL_URL}?opName=getOrdersForSync",
                    json=payload,
                    headers=headers,
                    cookies=cookies_dict,
                )

                if response.status_code != 200:
                    logger.error(f"MC GraphQL error: {response.status_code} - {response.text}")
                    raise KaspiMCError(f"MC API error: {response.status_code}")

                data = response.json()

                # Log raw response for debugging schema
                logger.info(f"MC GraphQL raw response: {json.dumps(data, ensure_ascii=False, default=str)[:2000]}")

                if "errors" in data:
                    error_msg = data["errors"][0].get("message", "Unknown error")
                    logger.error(f"GraphQL error fetching orders for sync: {error_msg}")
                    raise KaspiMCError(f"GraphQL error: {error_msg}")

                orders_data = data.get("data", {}).get("merchant", {}).get("orders", {})
                logger.info(f"Orders data keys: {list(orders_data.keys()) if isinstance(orders_data, dict) else type(orders_data)}")

                # Schema discovery mode - return empty, check logs for structure
                return []

                # Convert to Open API compatible format for parse_order_details()
                result = []
                for edge in edges:
                    node = edge.get("node", {})
                    customer = node.get("customer", {}) or {}
                    entries = node.get("entries", [])
                    delivery = node.get("deliveryAddress", {}) or {}
                    code = node.get("code", "")

                    # Format phone
                    raw_phone = customer.get("phoneNumber", "")
                    if raw_phone:
                        digits = "".join(filter(str.isdigit, raw_phone))
                        if len(digits) == 11 and digits.startswith('7'):
                            phone = digits
                        elif len(digits) == 10:
                            phone = f"7{digits}"
                        else:
                            phone = digits
                    else:
                        phone = ""

                    # Format address
                    addr_parts = []
                    if delivery.get('city'):
                        addr_parts.append(delivery['city'])
                    if delivery.get('street'):
                        addr_parts.append(delivery['street'])
                    if delivery.get('building'):
                        addr_parts.append(delivery['building'])
                    formatted_addr = ", ".join(addr_parts) if addr_parts else ""

                    # Format createdAt to timestamp ms
                    created_at = node.get("createdAt")
                    if isinstance(created_at, (int, float)):
                        creation_ts = int(created_at)
                    elif isinstance(created_at, str):
                        try:
                            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                            creation_ts = int(dt.timestamp() * 1000)
                        except (ValueError, TypeError):
                            creation_ts = int(datetime.utcnow().timestamp() * 1000)
                    else:
                        creation_ts = int(datetime.utcnow().timestamp() * 1000)

                    # Build Open API compatible structure
                    order = {
                        "id": code,  # Use code as ID
                        "attributes": {
                            "code": code,
                            "state": node.get("state", ""),
                            "totalPrice": node.get("totalPrice", 0),
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
                                        "code": "",  # MC GraphQL doesn't provide product codes
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
