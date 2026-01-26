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
from typing import Optional, Dict, Any, List
from datetime import datetime
import json

import httpx
import asyncpg

from ..config import settings
from ..core.database import get_db_pool
from .kaspi_auth_service import get_active_session, KaspiAuthError

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
        session = await get_active_session(user_id, store_id, pool)

        if not session:
            logger.warning(f"No active Kaspi session for store {store_id}")
            raise KaspiMCError("No active Kaspi session. Please login first.")

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

                return {
                    "phone": f"+7{customer.get('phoneNumber', '')}" if customer.get('phoneNumber') else None,
                    "phone_raw": customer.get('phoneNumber'),
                    "first_name": customer.get('firstName'),
                    "last_name": customer.get('lastName'),
                    "full_name": f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip() or None,
                    "order_state": order_detail.get('state'),
                    "order_total": order_detail.get('totalPrice'),
                    "items": order_detail.get('entries', []),
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
        session = await get_active_session(user_id, store_id, pool)

        if not session:
            raise KaspiMCError("No active Kaspi session")

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


# Singleton instance
_kaspi_mc_service: Optional[KaspiMCService] = None


def get_kaspi_mc_service() -> KaspiMCService:
    """Get singleton instance of KaspiMCService"""
    global _kaspi_mc_service

    if _kaspi_mc_service is None:
        _kaspi_mc_service = KaspiMCService()

    return _kaspi_mc_service
