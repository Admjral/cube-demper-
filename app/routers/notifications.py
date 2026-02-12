"""Notifications router for user notification management."""

import json
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from typing import Annotated, Optional
import asyncpg
import uuid

from ..core.database import get_db_pool
from ..dependencies import get_current_user

router = APIRouter()


class NotificationSettings(BaseModel):
    orders: bool = True
    price_changes: bool = True
    support: bool = True


@router.get("/settings")
async def get_notification_settings(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """Get user's notification preferences."""
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        raw = await conn.fetchval(
            "SELECT notification_settings FROM users WHERE id = $1",
            user_id,
        )

    settings = {"orders": True, "price_changes": True, "support": True}
    if raw:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(parsed, dict):
            settings.update(parsed)

    return settings


@router.put("/settings")
async def update_notification_settings(
    body: NotificationSettings,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """Update user's notification preferences."""
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET notification_settings = $1 WHERE id = $2",
            json.dumps(body.model_dump(), ensure_ascii=False),
            user_id,
        )

    return {"success": True, **body.model_dump()}


@router.get("")
async def get_notifications(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    unread_only: bool = Query(False),
):
    """
    Get user's notifications with pagination.
    """
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        # Build query based on filters
        if unread_only:
            notifications = await conn.fetch(
                """
                SELECT id, type, title, message, data, is_read, created_at
                FROM notifications
                WHERE user_id = $1 AND is_read = FALSE
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
                """,
                user_id, limit, offset
            )
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM notifications WHERE user_id = $1 AND is_read = FALSE",
                user_id
            )
        else:
            notifications = await conn.fetch(
                """
                SELECT id, type, title, message, data, is_read, created_at
                FROM notifications
                WHERE user_id = $1
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
                """,
                user_id, limit, offset
            )
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM notifications WHERE user_id = $1",
                user_id
            )

        # Get unread count
        unread_count = await conn.fetchval(
            "SELECT COUNT(*) FROM notifications WHERE user_id = $1 AND is_read = FALSE",
            user_id
        )

    return {
        "notifications": [
            {
                "id": str(n["id"]),
                "type": n["type"],
                "title": n["title"],
                "message": n["message"],
                "data": n["data"] if n["data"] else {},
                "is_read": n["is_read"],
                "created_at": n["created_at"].isoformat() if n["created_at"] else None,
            }
            for n in notifications
        ],
        "total": total or 0,
        "unread_count": unread_count or 0,
    }


@router.get("/unread-count")
async def get_unread_count(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Get count of unread notifications.
    """
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM notifications WHERE user_id = $1 AND is_read = FALSE",
            user_id
        )

    return {"unread_count": count or 0}


@router.post("/{notification_id}/read")
async def mark_as_read(
    notification_id: uuid.UUID,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Mark a notification as read.
    """
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE notifications
            SET is_read = TRUE
            WHERE id = $1 AND user_id = $2
            """,
            notification_id, user_id
        )

        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Notification not found")

    return {"success": True}


@router.post("/read-all")
async def mark_all_as_read(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Mark all user's notifications as read.
    """
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE notifications
            SET is_read = TRUE
            WHERE user_id = $1 AND is_read = FALSE
            """,
            user_id
        )

    return {"success": True}


@router.delete("/{notification_id}")
async def delete_notification(
    notification_id: uuid.UUID,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Delete a notification.
    """
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM notifications WHERE id = $1 AND user_id = $2",
            notification_id, user_id
        )

        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Notification not found")

    return {"success": True}
