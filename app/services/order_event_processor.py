"""
Order Event Processor - автоматическая отправка WhatsApp по событиям заказа

Обрабатывает события:
- order_approved - Заказ оплачен
- order_shipped - Заказ отправлен
- order_delivered - Заказ доставлен
- order_completed - Заказ завершён
- review_request - Запрос отзыва (отложенный)

Для каждого события ищет подходящий шаблон WhatsApp и отправляет сообщение.
"""
import logging
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from enum import Enum
from uuid import UUID

import asyncpg

from ..config import settings
from ..core.database import get_db_pool
from .kaspi_mc_service import get_kaspi_mc_service, KaspiMCError
from .waha_service import get_waha_service, WahaError

logger = logging.getLogger(__name__)


class OrderEvent(str, Enum):
    """События заказа для WhatsApp триггеров"""
    ORDER_APPROVED = "order_approved"  # Оплачен (Kaspi: APPROVED)
    ORDER_ACCEPTED_BY_MERCHANT = "order_accepted_by_merchant"  # Принят продавцом
    ORDER_SHIPPED = "order_shipped"  # Отправлен (Kaspi: DELIVERY)
    ORDER_DELIVERED = "order_delivered"  # Доставлен
    ORDER_COMPLETED = "order_completed"  # Завершён (Kaspi: COMPLETED)
    ORDER_CANCELLED = "order_cancelled"  # Отменён (Kaspi: CANCELLED)
    REVIEW_REQUEST = "review_request"  # Запрос отзыва (отложенный)


# Маппинг статусов Kaspi на события
KASPI_STATE_TO_EVENT = {
    "APPROVED": OrderEvent.ORDER_APPROVED,
    "ACCEPTED_BY_MERCHANT": OrderEvent.ORDER_ACCEPTED_BY_MERCHANT,
    "DELIVERY": OrderEvent.ORDER_SHIPPED,
    "DELIVERED": OrderEvent.ORDER_DELIVERED,
    "COMPLETED": OrderEvent.ORDER_COMPLETED,
    "CANCELLED": OrderEvent.ORDER_CANCELLED,
}


class OrderEventProcessor:
    """
    Обработчик событий заказов.

    Отслеживает изменения статусов заказов и отправляет WhatsApp сообщения
    по соответствующим шаблонам.
    """

    # Шаблонные переменные для замены
    TEMPLATE_VARIABLES = {
        "{customer_name}": "customer_name",
        "{customer_first_name}": "customer_first_name",
        "{order_code}": "order_code",
        "{order_total}": "order_total",
        "{store_name}": "store_name",
        "{items_list}": "items_list",
        "{delivery_address}": "delivery_address",
        "{promo_code}": "promo_code",
    }

    def __init__(self):
        self._kaspi_mc = get_kaspi_mc_service()

    async def process_order_event(
        self,
        user_id: str,
        store_id: str,
        order_code: str,
        event: OrderEvent,
        pool: asyncpg.Pool,
        extra_data: Dict[str, Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Обработать событие заказа.

        Args:
            user_id: ID пользователя
            store_id: ID магазина
            order_code: Код заказа Kaspi
            event: Тип события
            pool: Пул соединений к БД
            extra_data: Дополнительные данные

        Returns:
            Результат отправки или None если сообщение не отправлено
        """
        logger.info(f"Processing order event: {event.value} for order {order_code}")

        async with pool.acquire() as conn:
            # 1. Проверяем, не отправляли ли мы уже сообщение по этому событию
            existing = await conn.fetchrow("""
                SELECT id FROM whatsapp_messages
                WHERE order_code = $1 AND trigger_event = $2 AND user_id = $3
            """, order_code, event.value, UUID(user_id))

            if existing:
                logger.info(f"Message already sent for event {event.value} order {order_code}")
                return {"status": "already_sent", "message_id": str(existing['id'])}

            # 2. Получаем активный шаблон для этого события
            template = await conn.fetchrow("""
                SELECT id, name, message, variables
                FROM whatsapp_templates
                WHERE user_id = $1 AND trigger_event = $2 AND is_active = TRUE
                LIMIT 1
            """, UUID(user_id), event.value)

            if not template:
                logger.info(f"No active template for event {event.value}")
                return {"status": "no_template"}

            # 3. Получаем данные заказа и телефон покупателя
            try:
                order_data = await self._kaspi_mc.get_order_customer_phone(
                    user_id=user_id,
                    store_id=store_id,
                    order_code=order_code,
                    pool=pool,
                )
            except KaspiMCError as e:
                logger.error(f"Failed to get order data: {e}")
                return {"status": "error", "error": str(e)}

            if not order_data or not order_data.get('phone'):
                logger.warning(f"No phone number for order {order_code}")
                return {"status": "no_phone"}

            # 4. Получаем данные магазина
            store = await conn.fetchrow("""
                SELECT name, ai_promo_code
                FROM kaspi_stores
                WHERE id = $1 AND user_id = $2
            """, UUID(store_id), UUID(user_id))

            # 5. Формируем данные для замены переменных
            variables_data = {
                "customer_name": order_data.get('full_name') or order_data.get('first_name') or 'Покупатель',
                "customer_first_name": order_data.get('first_name') or 'Покупатель',
                "order_code": order_code,
                "order_total": f"{order_data.get('order_total', 0):,.0f}".replace(",", " ") + " тг",
                "store_name": store['name'] if store else "Наш магазин",
                "items_list": self._format_items_list(order_data.get('items', [])),
                "delivery_address": "",  # TODO: добавить адрес если есть
                "promo_code": store.get('ai_promo_code', '') if store else '',
            }

            # Добавляем extra_data если есть
            if extra_data:
                variables_data.update(extra_data)

            # 6. Заменяем переменные в шаблоне
            message_text = self._replace_variables(template['message'], variables_data)

            # 7. Проверяем активную WhatsApp сессию
            wa_session = await conn.fetchrow("""
                SELECT session_name, status
                FROM whatsapp_sessions
                WHERE user_id = $1 AND status IN ('connected', 'WORKING')
                LIMIT 1
            """, UUID(user_id))

            if not wa_session:
                logger.warning(f"No active WhatsApp session for user {user_id}")
                return {"status": "no_whatsapp_session"}

            # 8. Отправляем сообщение
            try:
                waha = get_waha_service()
                result = await waha.send_text(
                    phone=order_data['phone_raw'] or order_data['phone'].replace('+7', '7'),
                    text=message_text,
                    session=wa_session['session_name'],
                )

                logger.info(f"Message sent to {order_data['phone']} for order {order_code}")

                # 9. Сохраняем в историю
                message_record = await conn.fetchrow("""
                    INSERT INTO whatsapp_messages (
                        user_id, session_id, template_id, recipient_phone, recipient_name,
                        message_content, message_type, status, trigger_event, order_code,
                        waha_message_id, sent_at
                    )
                    SELECT
                        $1, ws.id, $2, $3, $4, $5, 'template', 'sent', $6, $7, $8, NOW()
                    FROM whatsapp_sessions ws
                    WHERE ws.session_name = $9 AND ws.user_id = $1
                    RETURNING id
                """,
                    UUID(user_id), template['id'], order_data['phone'], order_data.get('full_name'),
                    message_text, event.value, order_code, result.get('id'),
                    wa_session['session_name']
                )

                return {
                    "status": "sent",
                    "message_id": str(message_record['id']) if message_record else None,
                    "recipient": order_data['phone'],
                    "text": message_text,
                }

            except WahaError as e:
                logger.error(f"Failed to send WhatsApp message: {e}")

                # Сохраняем failed сообщение
                await conn.execute("""
                    INSERT INTO whatsapp_messages (
                        user_id, template_id, recipient_phone, recipient_name,
                        message_content, message_type, status, trigger_event, order_code,
                        error_message
                    )
                    VALUES ($1, $2, $3, $4, $5, 'template', 'failed', $6, $7, $8)
                """,
                    UUID(user_id), template['id'], order_data['phone'], order_data.get('full_name'),
                    message_text, event.value, order_code, str(e)
                )

                return {"status": "error", "error": str(e)}

    def _replace_variables(self, template: str, data: Dict[str, Any]) -> str:
        """Заменить переменные в шаблоне на значения"""
        result = template

        for var_placeholder, var_key in self.TEMPLATE_VARIABLES.items():
            value = data.get(var_key, '')
            result = result.replace(var_placeholder, str(value))

        return result

    def _format_items_list(self, items: List[Dict[str, Any]]) -> str:
        """Форматировать список товаров для сообщения"""
        if not items:
            return ""

        lines = []
        for item in items[:5]:  # Максимум 5 товаров
            name = item.get('productName', 'Товар')
            qty = item.get('quantity', 1)
            price = item.get('basePrice', 0)

            if qty > 1:
                lines.append(f"• {name} x{qty}")
            else:
                lines.append(f"• {name}")

        if len(items) > 5:
            lines.append(f"...и ещё {len(items) - 5} товар(ов)")

        return "\n".join(lines)

    async def process_kaspi_state_change(
        self,
        user_id: str,
        store_id: str,
        order_code: str,
        old_state: Optional[str],
        new_state: str,
        pool: asyncpg.Pool,
    ) -> Optional[Dict[str, Any]]:
        """
        Обработать изменение статуса Kaspi заказа.

        Используется при опросе API или получении webhook.
        """
        event = KASPI_STATE_TO_EVENT.get(new_state)

        if not event:
            logger.debug(f"No event mapping for Kaspi state: {new_state}")
            return None

        return await self.process_order_event(
            user_id=user_id,
            store_id=store_id,
            order_code=order_code,
            event=event,
            pool=pool,
        )

    async def schedule_review_request(
        self,
        user_id: str,
        store_id: str,
        order_code: str,
        pool: asyncpg.Pool,
        delay_days: int = 3,
    ) -> Optional[str]:
        """
        Запланировать отправку запроса отзыва.

        Args:
            delay_days: Через сколько дней после доставки отправить

        Returns:
            ID задания или None
        """
        async with pool.acquire() as conn:
            # Проверяем есть ли шаблон для отзывов
            template = await conn.fetchrow("""
                SELECT id FROM whatsapp_templates
                WHERE user_id = $1 AND trigger_event = $2 AND is_active = TRUE
            """, UUID(user_id), OrderEvent.REVIEW_REQUEST.value)

            if not template:
                return None

            # Создаём отложенное задание
            scheduled_at = datetime.utcnow() + timedelta(days=delay_days)

            job = await conn.fetchrow("""
                INSERT INTO scheduled_messages (
                    user_id, store_id, order_code, event_type, scheduled_at, status
                )
                VALUES ($1, $2, $3, $4, $5, 'pending')
                RETURNING id
            """, UUID(user_id), UUID(store_id), order_code, OrderEvent.REVIEW_REQUEST.value, scheduled_at)

            logger.info(f"Scheduled review request for order {order_code} at {scheduled_at}")

            return str(job['id']) if job else None


# Singleton
_event_processor: Optional[OrderEventProcessor] = None


def get_order_event_processor() -> OrderEventProcessor:
    """Get singleton instance of OrderEventProcessor"""
    global _event_processor

    if _event_processor is None:
        _event_processor = OrderEventProcessor()

    return _event_processor


async def process_new_kaspi_order(
    user_id: str,
    store_id: str,
    order_code: str,
    kaspi_state: str,
    pool: asyncpg.Pool,
) -> Dict[str, Any]:
    """
    Обработать новый/обновлённый заказ из Kaspi.

    Вызывается из воркера опроса заказов или webhook.
    """
    processor = get_order_event_processor()

    result = await processor.process_kaspi_state_change(
        user_id=user_id,
        store_id=store_id,
        order_code=order_code,
        old_state=None,
        new_state=kaspi_state,
        pool=pool,
    )

    # Если заказ завершён - планируем запрос отзыва
    if kaspi_state in ('COMPLETED', 'DELIVERED'):
        await processor.schedule_review_request(
            user_id=user_id,
            store_id=store_id,
            order_code=order_code,
            pool=pool,
        )

    return result or {"status": "no_action"}
