"""
Orders Worker - автоматический мониторинг заказов Kaspi и отправка WhatsApp

Функции:
- Опрос Kaspi API для получения новых/обновлённых заказов
- Отслеживание изменений статусов
- Автоматическая отправка WhatsApp сообщений по событиям
- Парсинг номеров телефонов из Kaspi MC

Запуск:
    $ python -m app.workers.orders_worker

Или с переменными окружения:
    $ ORDERS_POLLING_INTERVAL=60 python -m app.workers.orders_worker
"""

import asyncio
import logging
import signal
import sys
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from uuid import UUID

from ..config import settings
from ..core.database import get_db_pool, close_pool
from ..core.proxy_rotator import get_user_proxy_rotator, NoProxiesAllocatedError, NoProxiesAvailableError
from ..services.kaspi_auth_service import get_active_session_with_refresh, KaspiAuthError
from ..services.kaspi_mc_service import get_kaspi_mc_service, KaspiMCError
from ..services.order_event_processor import (
    get_order_event_processor,
    OrderEvent,
    KASPI_STATE_TO_EVENT,
)

logger = logging.getLogger(__name__)


# Kaspi API order states
KASPI_ORDER_STATES = [
    "APPROVED",  # Оплачен
    "ACCEPTED_BY_MERCHANT",  # Принят продавцом
    "DELIVERY",  # В доставке
    "DELIVERED",  # Доставлен
    "COMPLETED",  # Завершён
    "CANCELLED",  # Отменён
    "CANCELLING",  # Отменяется
    "RETURNED",  # Возврат
]


class OrdersWorker:
    """
    Воркер для мониторинга заказов и отправки WhatsApp уведомлений.

    Опрашивает Kaspi API каждые N секунд, отслеживает изменения статусов
    и триггерит отправку WhatsApp сообщений через order_event_processor.
    """

    def __init__(
        self,
        polling_interval: int = 600,  # ✅ Changed from 60s to 600s (10 minutes) for proxy pool
        batch_size: int = 50,
    ):
        """
        Args:
            polling_interval: Интервал опроса в секундах (по умолчанию 600 = 10 минут)
            batch_size: Количество заказов за один запрос (по умолчанию 50)
        """
        self.polling_interval = int(os.getenv("ORDERS_POLLING_INTERVAL", polling_interval))
        self.batch_size = batch_size
        self._running = False
        self._pool = None
        self._event_processor = None
        self._mc_service = None

    async def start(self):
        """Запустить воркер"""
        logger.info(f"Starting Orders Worker (polling interval: {self.polling_interval}s)")

        self._running = True
        self._pool = await get_db_pool()
        self._event_processor = get_order_event_processor()
        self._mc_service = get_kaspi_mc_service()

        # Setup signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_event_loop().add_signal_handler(
                sig, lambda: asyncio.create_task(self.stop())
            )

        try:
            while self._running:
                try:
                    await self._poll_all_stores()
                except Exception as e:
                    logger.error(f"Error in polling cycle: {e}", exc_info=True)

                # Wait for next cycle
                await asyncio.sleep(self.polling_interval)

        except asyncio.CancelledError:
            logger.info("Worker cancelled")
        finally:
            await self.stop()

    async def stop(self):
        """Остановить воркер"""
        logger.info("Stopping Orders Worker...")
        self._running = False
        await close_pool()

    async def _poll_all_stores(self):
        """Опросить все активные магазины"""
        async with self._pool.acquire() as conn:
            # Получаем магазины с включенным опросом заказов
            stores = await conn.fetch("""
                SELECT
                    s.id, s.user_id, s.merchant_id, s.name,
                    s.orders_polling_interval_seconds,
                    s.last_orders_sync
                FROM kaspi_stores s
                WHERE s.is_active = TRUE
                AND s.orders_polling_enabled = TRUE
            """)

            if not stores:
                logger.debug("No stores with orders polling enabled")
                return

            logger.info(f"Polling {len(stores)} stores for orders")

            for store in stores:
                try:
                    await self._poll_store_orders(store, conn)
                except Exception as e:
                    logger.error(f"Error polling store {store['name']}: {e}")

    async def _poll_store_orders(self, store: dict, conn):
        """Опросить заказы одного магазина"""
        store_id = store['id']
        user_id = store['user_id']
        merchant_id = store['merchant_id']

        logger.debug(f"Polling orders for store: {store['name']}")

        # Получаем сессию Kaspi
        try:
            session = await get_active_session_with_refresh(
                str(user_id), str(store_id), self._pool
            )
        except KaspiAuthError as e:
            logger.warning(f"No active session for store {store['name']}: {e}")
            return

        if not session:
            logger.warning(f"No session for store {store['name']}")
            return

        # Запрашиваем заказы через Kaspi API (с proxy rotation для module='orders')
        try:
            orders = await self._fetch_kaspi_orders(
                session,
                merchant_id,
                user_id=user_id  # ✅ Use proxy pool for orders module (25 proxies)
            )
        except Exception as e:
            logger.error(f"Error fetching orders from Kaspi: {e}")
            return

        if not orders:
            logger.debug(f"No orders for store {store['name']}")
            return

        logger.info(f"Processing {len(orders)} orders for {store['name']}")

        # Обрабатываем каждый заказ
        for order_data in orders:
            try:
                await self._process_order(store, order_data, conn)
            except Exception as e:
                logger.error(f"Error processing order {order_data.get('code')}: {e}")

        # Обновляем время последней синхронизации
        await conn.execute(
            "UPDATE kaspi_stores SET last_orders_sync = NOW() WHERE id = $1",
            store_id
        )

    async def _fetch_kaspi_orders(
        self,
        session: dict,
        merchant_id: str,
        user_id: Optional[UUID] = None,
        days_back: int = 7,
    ) -> List[Dict[str, Any]]:
        """
        Получить заказы из Kaspi API.

        Использует /v2/orders endpoint с фильтром по дате.
        С поддержкой proxy rotation для module='orders'.
        """
        import httpx

        # Format cookies for request
        cookies = session.get('cookies', [])
        if isinstance(cookies, list):
            cookies_dict = {c.get('name', ''): c.get('value', '') for c in cookies if isinstance(c, dict)}
        else:
            cookies_dict = cookies or {}

        # Calculate date range
        date_to = datetime.utcnow()
        date_from = date_to - timedelta(days=days_back)

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        # Kaspi API endpoint
        url = f"https://kaspi.kz/shop/api/v2/orders"
        params = {
            "filter[orders][state]": ",".join(KASPI_ORDER_STATES[:6]),  # Active states
            "filter[orders][creationDate][$gte]": date_from.strftime("%Y-%m-%d"),
            "filter[orders][creationDate][$lte]": date_to.strftime("%Y-%m-%d"),
            "page[number]": 0,
            "page[size]": self.batch_size,
        }

        # Get proxy rotator for orders module
        rotator = None
        proxy_url = None
        use_proxy = user_id is not None

        if use_proxy:
            try:
                rotator = await get_user_proxy_rotator(user_id, module='orders')
                proxy = await rotator.get_current_proxy()
                proxy_url = f"{proxy.protocol}://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
                logger.debug(f"Using proxy {proxy.id} for orders (merchant {merchant_id})")
            except (NoProxiesAllocatedError, NoProxiesAvailableError) as e:
                logger.warning(f"No proxies available for user {user_id}, module 'orders': {e}")
                use_proxy = False

        try:
            # Create HTTP client with or without proxy
            if use_proxy and proxy_url:
                client = httpx.AsyncClient(
                    timeout=30.0,
                    cookies=cookies_dict,
                    proxies={"http://": proxy_url, "https://": proxy_url}
                )
            else:
                client = httpx.AsyncClient(timeout=30.0, cookies=cookies_dict)

            async with client:
                response = await client.get(url, headers=headers, params=params)

                if response.status_code == 401:
                    if rotator:
                        await rotator.record_request(success=False)
                    raise KaspiAuthError("Session expired")

                if response.status_code != 200:
                    logger.warning(f"Kaspi API error: {response.status_code}")
                    if rotator:
                        await rotator.record_request(success=False)
                    return []

                data = response.json()

                # ✅ Record successful request with proxy
                if rotator:
                    await rotator.record_request(success=True)

                return data.get("data", [])

        except httpx.RequestError as e:
            logger.error(f"Network error fetching orders: {e}")
            if rotator:
                await rotator.record_request(success=False)
            return []

    async def _process_order(self, store: dict, order_data: dict, conn):
        """Обработать один заказ - проверить изменение статуса и отправить WA"""
        kaspi_order_id = order_data.get("id")
        order_code = order_data.get("attributes", {}).get("code") or order_data.get("code")
        new_status = order_data.get("attributes", {}).get("state") or order_data.get("state")

        if not order_code or not new_status:
            return

        store_id = store['id']
        user_id = store['user_id']

        # Проверяем существующий заказ в БД
        existing = await conn.fetchrow("""
            SELECT id, status, customer_phone,
                   wa_approved_sent_at, wa_shipped_sent_at,
                   wa_delivered_sent_at, wa_completed_sent_at
            FROM orders
            WHERE store_id = $1 AND (kaspi_order_id = $2 OR kaspi_order_code = $3)
        """, store_id, kaspi_order_id, order_code)

        if existing:
            old_status = existing['status']
            order_id = existing['id']

            # Если статус изменился
            if old_status != new_status:
                logger.info(f"Order {order_code}: {old_status} -> {new_status}")

                # Обновляем статус в БД
                await conn.execute("""
                    UPDATE orders
                    SET status = $1, previous_status = $2, status_changed_at = NOW(), updated_at = NOW()
                    WHERE id = $3
                """, new_status, old_status, order_id)

                # Записываем в историю
                await conn.execute("""
                    INSERT INTO order_status_history (order_id, old_status, new_status)
                    VALUES ($1, $2, $3)
                """, order_id, old_status, new_status)

                # Триггерим WhatsApp если ещё не отправляли
                await self._trigger_whatsapp_if_needed(
                    store, order_code, new_status, existing, conn
                )
        else:
            # Новый заказ - создаём запись
            await self._create_order_record(store, order_data, conn)

            # Триггерим WhatsApp для нового заказа
            if new_status == "APPROVED":
                await self._trigger_whatsapp_for_event(
                    store, order_code, OrderEvent.ORDER_APPROVED, conn
                )

    async def _trigger_whatsapp_if_needed(
        self,
        store: dict,
        order_code: str,
        new_status: str,
        existing: dict,
        conn,
    ):
        """Отправить WhatsApp если ещё не отправляли для этого статуса"""
        # Маппинг статуса на поле wa_*_sent_at
        status_to_field = {
            "APPROVED": "wa_approved_sent_at",
            "DELIVERY": "wa_shipped_sent_at",
            "DELIVERED": "wa_delivered_sent_at",
            "COMPLETED": "wa_completed_sent_at",
        }

        field = status_to_field.get(new_status)
        if not field:
            return

        # Проверяем не отправляли ли уже
        if existing.get(field):
            logger.debug(f"WA already sent for {order_code} status {new_status}")
            return

        # Получаем событие
        event = KASPI_STATE_TO_EVENT.get(new_status)
        if not event:
            return

        # Отправляем
        result = await self._trigger_whatsapp_for_event(store, order_code, event, conn)

        # Обновляем флаг отправки
        if result and result.get("status") == "sent":
            await conn.execute(f"""
                UPDATE orders SET {field} = NOW() WHERE kaspi_order_code = $1 AND store_id = $2
            """, order_code, store['id'])

    async def _trigger_whatsapp_for_event(
        self,
        store: dict,
        order_code: str,
        event: OrderEvent,
        conn,
    ) -> Optional[Dict[str, Any]]:
        """Вызвать order_event_processor для отправки WhatsApp"""
        try:
            result = await self._event_processor.process_order_event(
                user_id=str(store['user_id']),
                store_id=str(store['id']),
                order_code=order_code,
                event=event,
                pool=self._pool,
            )

            if result:
                logger.info(f"WA trigger result for {order_code}/{event.value}: {result.get('status')}")
            return result

        except Exception as e:
            logger.error(f"Error triggering WA for {order_code}: {e}")
            return None

    async def _create_order_record(self, store: dict, order_data: dict, conn):
        """Создать запись о новом заказе"""
        attrs = order_data.get("attributes", {})

        kaspi_order_id = order_data.get("id")
        order_code = attrs.get("code") or order_data.get("code")
        status = attrs.get("state") or order_data.get("state")
        total_price = int(attrs.get("totalPrice", 0) * 100)  # В тиынах
        delivery_cost = int(attrs.get("deliveryCost", 0) * 100)
        customer_name = attrs.get("customer", {}).get("name")
        delivery_mode = attrs.get("deliveryMode")
        payment_mode = attrs.get("paymentMode")

        # Дата заказа
        created_at_str = attrs.get("creationDate")
        order_date = datetime.utcnow()
        if created_at_str:
            try:
                order_date = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
            except Exception:
                pass

        try:
            await conn.execute("""
                INSERT INTO orders (
                    store_id, kaspi_order_id, kaspi_order_code, status,
                    total_price, delivery_cost, customer_name,
                    delivery_mode, payment_mode, order_date
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (store_id, kaspi_order_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """,
                store['id'], kaspi_order_id, order_code, status,
                total_price, delivery_cost, customer_name,
                delivery_mode, payment_mode, order_date
            )
        except Exception as e:
            logger.error(f"Error creating order record: {e}")


async def main():
    """Entry point для запуска воркера"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Suppress noisy loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    logger.info("=" * 60)
    logger.info("ORDERS WORKER - Kaspi Order Monitoring & WhatsApp Automation")
    logger.info("=" * 60)

    worker = OrdersWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
