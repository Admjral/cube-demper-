"""Notification service for creating and managing user notifications."""

import uuid
from typing import Optional, Dict, Any
import asyncpg

from ..core.database import get_db_pool


# Notification types
class NotificationType:
    # Demping
    DEMPING_PRICE_CHANGED = "demping_price_changed"
    DEMPING_COMPETITOR_FOUND = "demping_competitor_found"
    DEMPING_MIN_REACHED = "demping_min_reached"
    DEMPING_SESSION_EXPIRED = "demping_session_expired"

    # Orders
    ORDER_NEW = "order_new"
    ORDER_STATUS_CHANGED = "order_status_changed"

    # Referral
    REFERRAL_SIGNUP = "referral_signup"
    REFERRAL_PAID = "referral_paid"
    REFERRAL_PAYOUT_COMPLETED = "referral_payout_completed"

    # Support
    SUPPORT_MESSAGE = "support_message"

    # System
    SYSTEM_SUBSCRIPTION_EXPIRING = "system_subscription_expiring"
    SYSTEM_SUBSCRIPTION_EXPIRED = "system_subscription_expired"
    SYSTEM_STORE_SYNC_COMPLETED = "system_store_sync_completed"
    SYSTEM_STORE_SYNC_FAILED = "system_store_sync_failed"


async def create_notification(
    pool: asyncpg.Pool,
    user_id: uuid.UUID,
    notification_type: str,
    title: str,
    message: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
) -> uuid.UUID:
    """
    Create a new notification for a user.

    Args:
        pool: Database connection pool
        user_id: User UUID to send notification to
        notification_type: Type of notification (from NotificationType)
        title: Notification title
        message: Optional detailed message
        data: Optional JSON data with additional info

    Returns:
        UUID of created notification
    """
    import json

    async with pool.acquire() as conn:
        notification_id = await conn.fetchval(
            """
            INSERT INTO notifications (user_id, type, title, message, data)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            """,
            user_id,
            notification_type,
            title,
            message,
            json.dumps(data or {}),
        )

    return notification_id


async def create_notification_for_store_owner(
    pool: asyncpg.Pool,
    store_id: uuid.UUID,
    notification_type: str,
    title: str,
    message: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
) -> Optional[uuid.UUID]:
    """
    Create a notification for the owner of a store.

    Args:
        pool: Database connection pool
        store_id: Store UUID to find owner
        notification_type: Type of notification
        title: Notification title
        message: Optional detailed message
        data: Optional JSON data

    Returns:
        UUID of created notification, or None if store not found
    """
    async with pool.acquire() as conn:
        user_id = await conn.fetchval(
            "SELECT user_id FROM kaspi_stores WHERE id = $1",
            store_id
        )

    if not user_id:
        return None

    return await create_notification(
        pool=pool,
        user_id=user_id,
        notification_type=notification_type,
        title=title,
        message=message,
        data=data,
    )


# Helper functions for common notifications

async def notify_price_changed(
    pool: asyncpg.Pool,
    user_id: uuid.UUID,
    product_name: str,
    old_price: int,
    new_price: int,
    product_id: Optional[uuid.UUID] = None,
) -> uuid.UUID:
    """Notify user that bot changed a product price."""
    return await create_notification(
        pool=pool,
        user_id=user_id,
        notification_type=NotificationType.DEMPING_PRICE_CHANGED,
        title="Цена изменена",
        message=f"{product_name}: {old_price:,} ₸ → {new_price:,} ₸".replace(",", " "),
        data={
            "product_id": str(product_id) if product_id else None,
            "product_name": product_name,
            "old_price": old_price,
            "new_price": new_price,
        },
    )


async def notify_min_price_reached(
    pool: asyncpg.Pool,
    user_id: uuid.UUID,
    product_name: str,
    min_price: int,
    product_id: Optional[uuid.UUID] = None,
) -> uuid.UUID:
    """Notify user that minimum price was reached for a product."""
    return await create_notification(
        pool=pool,
        user_id=user_id,
        notification_type=NotificationType.DEMPING_MIN_REACHED,
        title="Достигнута мин. цена",
        message=f"{product_name}: демпинг остановлен на {min_price:,} ₸".replace(",", " "),
        data={
            "product_id": str(product_id) if product_id else None,
            "product_name": product_name,
            "min_price": min_price,
        },
    )


async def notify_session_expired(
    pool: asyncpg.Pool,
    user_id: uuid.UUID,
    store_name: str,
    store_id: Optional[uuid.UUID] = None,
) -> uuid.UUID:
    """Notify user that Kaspi session needs reauthorization."""
    return await create_notification(
        pool=pool,
        user_id=user_id,
        notification_type=NotificationType.DEMPING_SESSION_EXPIRED,
        title="Требуется авторизация",
        message=f"Сессия магазина «{store_name}» истекла. Пройдите авторизацию заново.",
        data={
            "store_id": str(store_id) if store_id else None,
            "store_name": store_name,
        },
    )


async def notify_referral_signup(
    pool: asyncpg.Pool,
    user_id: uuid.UUID,
    referral_email: str,
) -> uuid.UUID:
    """Notify user that someone signed up with their referral link."""
    return await create_notification(
        pool=pool,
        user_id=user_id,
        notification_type=NotificationType.REFERRAL_SIGNUP,
        title="Новый реферал",
        message=f"Пользователь {referral_email} зарегистрировался по вашей ссылке",
        data={"referral_email": referral_email},
    )


async def notify_referral_paid(
    pool: asyncpg.Pool,
    user_id: uuid.UUID,
    referral_email: str,
    commission: int,
) -> uuid.UUID:
    """Notify user that their referral paid for subscription."""
    return await create_notification(
        pool=pool,
        user_id=user_id,
        notification_type=NotificationType.REFERRAL_PAID,
        title="Реферал оплатил подписку",
        message=f"{referral_email} оплатил подписку. Вы заработали {commission // 100:,} ₸".replace(",", " "),
        data={
            "referral_email": referral_email,
            "commission": commission,
        },
    )


async def notify_subscription_expiring(
    pool: asyncpg.Pool,
    user_id: uuid.UUID,
    days_left: int,
) -> uuid.UUID:
    """Notify user that subscription is expiring soon."""
    return await create_notification(
        pool=pool,
        user_id=user_id,
        notification_type=NotificationType.SYSTEM_SUBSCRIPTION_EXPIRING,
        title="Подписка истекает",
        message=f"Ваша подписка истекает через {days_left} дней. Продлите подписку, чтобы не потерять доступ.",
        data={"days_left": days_left},
    )


async def get_user_notification_settings(
    pool: asyncpg.Pool,
    user_id: uuid.UUID,
) -> Dict[str, bool]:
    """Get user notification preferences. Returns defaults if not set."""
    import json

    defaults = {"orders": True, "price_changes": True, "support": True}
    async with pool.acquire() as conn:
        raw = await conn.fetchval(
            "SELECT notification_settings FROM users WHERE id = $1",
            user_id,
        )
    if raw:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(parsed, dict):
            defaults.update(parsed)
    return defaults


async def notify_support_message(
    pool: asyncpg.Pool,
    user_id: uuid.UUID,
    message_preview: str,
    chat_id: Optional[uuid.UUID] = None,
) -> Optional[uuid.UUID]:
    """Notify user about new support message (if enabled)."""
    prefs = await get_user_notification_settings(pool, user_id)
    if not prefs.get("support", True):
        return None

    return await create_notification(
        pool=pool,
        user_id=user_id,
        notification_type=NotificationType.SUPPORT_MESSAGE,
        title="Сообщение от поддержки",
        message=message_preview[:200],
        data={"chat_id": str(chat_id) if chat_id else None},
    )
