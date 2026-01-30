"""Support chat router - чат техподдержки."""

from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect
from typing import Annotated, Optional
import asyncpg
from datetime import datetime
import uuid
import json
import logging

from ..core.database import get_db_pool
from ..core.security import decode_access_token, verify_password, create_access_token
from ..core.exceptions import AuthenticationError, AuthorizationError, NotFoundError
from ..dependencies import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter()

# WebSocket connections store: {chat_id: {user_id: websocket}}
active_connections: dict[str, dict[str, WebSocket]] = {}


async def get_current_support_user(
    authorization: Optional[str] = None,
    pool: asyncpg.Pool = Depends(get_db_pool)
) -> dict:
    """Dependency to get current support staff or admin user"""
    if not authorization or not authorization.startswith("Bearer "):
        raise AuthenticationError("Missing or invalid authorization header")

    token = authorization.replace("Bearer ", "")
    payload = decode_access_token(token)

    if not payload:
        raise AuthenticationError("Invalid or expired token")

    user_id = payload.get("sub")
    role = payload.get("role")

    if not user_id:
        raise AuthenticationError("Invalid token payload")

    # Only admin and support roles allowed
    if role not in ("admin", "support"):
        raise AuthorizationError("Support or admin access required")

    try:
        user_uuid = uuid.UUID(user_id)
    except (ValueError, TypeError):
        raise AuthenticationError("Invalid user ID format in token")

    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id, email, full_name, role, created_at FROM users WHERE id = $1",
            user_uuid
        )

    if not user:
        raise AuthenticationError("User not found")

    if user["role"] not in ("admin", "support"):
        raise AuthorizationError("Support or admin access required")

    return dict(user)


# === Auth endpoints for support staff ===

@router.post("/login")
async def support_login(
    credentials: dict,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Логин сотрудника поддержки (admin или support роль).
    """
    email = credentials.get("email")
    password = credentials.get("password")

    if not email or not password:
        raise AuthenticationError("Email и пароль обязательны")

    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id, email, password_hash, full_name, role FROM users WHERE email = $1",
            email,
        )

    if not user:
        raise AuthenticationError("Неверный логин или пароль")

    if user["role"] not in ("admin", "support"):
        raise AuthorizationError("Доступ запрещён. Требуется роль admin или support")

    if not verify_password(password, user["password_hash"]):
        raise AuthenticationError("Неверный логин или пароль")

    from datetime import timedelta
    access_token = create_access_token(
        user_id=user["id"],
        role=user["role"],
        expires_delta=timedelta(hours=24),
    )

    return {
        "access_token": access_token,
        "token_type": "bearer",
    }


@router.get("/me")
async def support_me(
    authorization: Annotated[Optional[str], Depends(lambda: None)] = None,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)] = None,
):
    """
    Текущий пользователь по JWT-токену.
    """
    from fastapi import Request
    # Manual header extraction since we need to pass to dependency
    from starlette.requests import Request as StarletteRequest
    # This will be handled by the actual request
    pass


@router.get("/me", include_in_schema=False)
async def support_me_real(
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    authorization: Optional[str] = None,
):
    """Fallback - actual implementation is below"""
    pass


# Override with proper implementation
from fastapi import Header

@router.get("/me")
async def get_support_profile(
    authorization: Optional[str] = Header(None),
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """
    Текущий сотрудник поддержки по JWT-токену.
    """
    current_user = await get_current_support_user(authorization, pool)
    return {
        "id": str(current_user["id"]),
        "email": current_user["email"],
        "full_name": current_user.get("full_name"),
        "role": current_user["role"],
        "created_at": current_user["created_at"].isoformat() if current_user["created_at"] else None,
    }


# === Chat list endpoints ===

@router.get("/chats")
async def get_chats(
    authorization: Optional[str] = Header(None),
    pool: asyncpg.Pool = Depends(get_db_pool),
    status: Optional[str] = Query(None, description="Filter by status: open, closed, pending"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """
    Список чатов поддержки для CRM панели.
    """
    current_user = await get_current_support_user(authorization, pool)

    async with pool.acquire() as conn:
        # Build query with optional status filter
        query = """
            SELECT
                sc.id,
                sc.user_id,
                u.email as user_email,
                u.full_name as user_name,
                sc.status,
                sc.assigned_to,
                assigned_user.full_name as assigned_name,
                sc.created_at,
                sc.updated_at,
                (
                    SELECT content FROM support_messages
                    WHERE chat_id = sc.id
                    ORDER BY created_at DESC LIMIT 1
                ) as last_message,
                (
                    SELECT created_at FROM support_messages
                    WHERE chat_id = sc.id
                    ORDER BY created_at DESC LIMIT 1
                ) as last_message_at,
                (
                    SELECT COUNT(*) FROM support_messages
                    WHERE chat_id = sc.id AND sender_type = 'user' AND is_read = FALSE
                ) as unread_count
            FROM support_chats sc
            JOIN users u ON u.id = sc.user_id
            LEFT JOIN users assigned_user ON assigned_user.id = sc.assigned_to
            WHERE 1=1
        """
        params = []

        if status:
            params.append(status)
            query += f" AND sc.status = ${len(params)}"

        query += " ORDER BY last_message_at DESC NULLS LAST, sc.created_at DESC"
        params.extend([limit, offset])
        query += f" LIMIT ${len(params)-1} OFFSET ${len(params)}"

        chats = await conn.fetch(query, *params)

        # Count total
        count_query = "SELECT COUNT(*) FROM support_chats WHERE 1=1"
        count_params = []
        if status:
            count_params.append(status)
            count_query += f" AND status = ${len(count_params)}"

        total = await conn.fetchval(count_query, *count_params)

    return {
        "chats": [
            {
                "id": str(chat["id"]),
                "user_id": str(chat["user_id"]),
                "user_email": chat["user_email"],
                "user_name": chat["user_name"],
                "status": chat["status"],
                "assigned_to": str(chat["assigned_to"]) if chat["assigned_to"] else None,
                "assigned_name": chat["assigned_name"],
                "last_message": chat["last_message"],
                "last_message_at": chat["last_message_at"].isoformat() if chat["last_message_at"] else None,
                "unread_count": chat["unread_count"] or 0,
                "created_at": chat["created_at"].isoformat() if chat["created_at"] else None,
                "updated_at": chat["updated_at"].isoformat() if chat["updated_at"] else None,
            }
            for chat in chats
        ],
        "total": total or 0,
    }


@router.get("/chats/{chat_id}")
async def get_chat(
    chat_id: str,
    authorization: Optional[str] = Header(None),
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """
    Детали конкретного чата.
    """
    current_user = await get_current_support_user(authorization, pool)

    try:
        chat_uuid = uuid.UUID(chat_id)
    except ValueError:
        raise NotFoundError("Чат не найден")

    async with pool.acquire() as conn:
        chat = await conn.fetchrow(
            """
            SELECT
                sc.id,
                sc.user_id,
                u.email as user_email,
                u.full_name as user_name,
                sc.status,
                sc.assigned_to,
                assigned_user.full_name as assigned_name,
                sc.created_at,
                sc.updated_at
            FROM support_chats sc
            JOIN users u ON u.id = sc.user_id
            LEFT JOIN users assigned_user ON assigned_user.id = sc.assigned_to
            WHERE sc.id = $1
            """,
            chat_uuid
        )

    if not chat:
        raise NotFoundError("Чат не найден")

    return {
        "id": str(chat["id"]),
        "user_id": str(chat["user_id"]),
        "user_email": chat["user_email"],
        "user_name": chat["user_name"],
        "status": chat["status"],
        "assigned_to": str(chat["assigned_to"]) if chat["assigned_to"] else None,
        "assigned_name": chat["assigned_name"],
        "created_at": chat["created_at"].isoformat() if chat["created_at"] else None,
        "updated_at": chat["updated_at"].isoformat() if chat["updated_at"] else None,
    }


# === Messages endpoints ===

@router.get("/chats/{chat_id}/messages")
async def get_messages(
    chat_id: str,
    authorization: Optional[str] = Header(None),
    pool: asyncpg.Pool = Depends(get_db_pool),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Сообщения в чате.
    """
    current_user = await get_current_support_user(authorization, pool)

    try:
        chat_uuid = uuid.UUID(chat_id)
    except ValueError:
        raise NotFoundError("Чат не найден")

    async with pool.acquire() as conn:
        # Verify chat exists
        chat = await conn.fetchval("SELECT id FROM support_chats WHERE id = $1", chat_uuid)
        if not chat:
            raise NotFoundError("Чат не найден")

        messages = await conn.fetch(
            """
            SELECT
                sm.id,
                sm.chat_id,
                sm.sender_id,
                sm.sender_type,
                CASE
                    WHEN sm.sender_type = 'user' THEN u.full_name
                    ELSE support_user.full_name
                END as sender_name,
                sm.content,
                sm.is_read,
                sm.created_at
            FROM support_messages sm
            LEFT JOIN users u ON u.id = sm.sender_id AND sm.sender_type = 'user'
            LEFT JOIN users support_user ON support_user.id = sm.sender_id AND sm.sender_type = 'support'
            WHERE sm.chat_id = $1
            ORDER BY sm.created_at ASC
            LIMIT $2 OFFSET $3
            """,
            chat_uuid, limit, offset
        )

        total = await conn.fetchval(
            "SELECT COUNT(*) FROM support_messages WHERE chat_id = $1",
            chat_uuid
        )

    return {
        "messages": [
            {
                "id": str(msg["id"]),
                "chat_id": str(msg["chat_id"]),
                "sender_id": str(msg["sender_id"]),
                "sender_type": msg["sender_type"],
                "sender_name": msg["sender_name"],
                "content": msg["content"],
                "is_read": msg["is_read"],
                "created_at": msg["created_at"].isoformat() if msg["created_at"] else None,
            }
            for msg in messages
        ],
        "total": total or 0,
    }


@router.post("/chats/{chat_id}/messages")
async def send_message(
    chat_id: str,
    request: dict,
    authorization: Optional[str] = Header(None),
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """
    Отправить сообщение в чат (от сотрудника поддержки).
    """
    current_user = await get_current_support_user(authorization, pool)
    content = request.get("content", "").strip()

    if not content:
        raise AuthenticationError("Сообщение не может быть пустым")

    try:
        chat_uuid = uuid.UUID(chat_id)
    except ValueError:
        raise NotFoundError("Чат не найден")

    async with pool.acquire() as conn:
        # Verify chat exists and is not closed
        chat = await conn.fetchrow(
            "SELECT id, status FROM support_chats WHERE id = $1",
            chat_uuid
        )
        if not chat:
            raise NotFoundError("Чат не найден")

        if chat["status"] == "closed":
            raise AuthorizationError("Нельзя отправлять сообщения в закрытый чат")

        # Create message
        message_id = uuid.uuid4()
        await conn.execute(
            """
            INSERT INTO support_messages (id, chat_id, sender_id, sender_type, content, is_read, created_at)
            VALUES ($1, $2, $3, 'support', $4, FALSE, NOW())
            """,
            message_id, chat_uuid, current_user["id"], content
        )

        # Update chat status to open if it was pending
        if chat["status"] == "pending":
            await conn.execute(
                "UPDATE support_chats SET status = 'open', updated_at = NOW() WHERE id = $1",
                chat_uuid
            )

        # Fetch the created message
        message = await conn.fetchrow(
            """
            SELECT
                sm.id,
                sm.chat_id,
                sm.sender_id,
                sm.sender_type,
                u.full_name as sender_name,
                sm.content,
                sm.is_read,
                sm.created_at
            FROM support_messages sm
            LEFT JOIN users u ON u.id = sm.sender_id
            WHERE sm.id = $1
            """,
            message_id
        )

    # Broadcast to WebSocket connections
    await broadcast_message(chat_id, {
        "id": str(message["id"]),
        "chat_id": str(message["chat_id"]),
        "sender_id": str(message["sender_id"]),
        "sender_type": message["sender_type"],
        "sender_name": message["sender_name"],
        "content": message["content"],
        "is_read": message["is_read"],
        "created_at": message["created_at"].isoformat() if message["created_at"] else None,
    })

    return {
        "id": str(message["id"]),
        "chat_id": str(message["chat_id"]),
        "sender_id": str(message["sender_id"]),
        "sender_type": message["sender_type"],
        "sender_name": message["sender_name"],
        "content": message["content"],
        "is_read": message["is_read"],
        "created_at": message["created_at"].isoformat() if message["created_at"] else None,
    }


# === Chat actions ===

@router.post("/chats/{chat_id}/assign")
async def assign_chat(
    chat_id: str,
    authorization: Optional[str] = Header(None),
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """
    Взять чат в работу (назначить себя ответственным).
    """
    current_user = await get_current_support_user(authorization, pool)

    try:
        chat_uuid = uuid.UUID(chat_id)
    except ValueError:
        raise NotFoundError("Чат не найден")

    async with pool.acquire() as conn:
        chat = await conn.fetchrow(
            "SELECT id, status FROM support_chats WHERE id = $1",
            chat_uuid
        )
        if not chat:
            raise NotFoundError("Чат не найден")

        await conn.execute(
            """
            UPDATE support_chats
            SET assigned_to = $1, status = 'open', updated_at = NOW()
            WHERE id = $2
            """,
            current_user["id"], chat_uuid
        )

        # Return updated chat
        updated = await conn.fetchrow(
            """
            SELECT
                sc.id,
                sc.user_id,
                u.email as user_email,
                u.full_name as user_name,
                sc.status,
                sc.assigned_to,
                assigned_user.full_name as assigned_name,
                sc.created_at,
                sc.updated_at
            FROM support_chats sc
            JOIN users u ON u.id = sc.user_id
            LEFT JOIN users assigned_user ON assigned_user.id = sc.assigned_to
            WHERE sc.id = $1
            """,
            chat_uuid
        )

    return {
        "id": str(updated["id"]),
        "user_id": str(updated["user_id"]),
        "user_email": updated["user_email"],
        "user_name": updated["user_name"],
        "status": updated["status"],
        "assigned_to": str(updated["assigned_to"]) if updated["assigned_to"] else None,
        "assigned_name": updated["assigned_name"],
        "created_at": updated["created_at"].isoformat() if updated["created_at"] else None,
        "updated_at": updated["updated_at"].isoformat() if updated["updated_at"] else None,
    }


@router.post("/chats/{chat_id}/close")
async def close_chat(
    chat_id: str,
    authorization: Optional[str] = Header(None),
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """
    Закрыть чат.
    """
    current_user = await get_current_support_user(authorization, pool)

    try:
        chat_uuid = uuid.UUID(chat_id)
    except ValueError:
        raise NotFoundError("Чат не найден")

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE support_chats SET status = 'closed', updated_at = NOW() WHERE id = $1",
            chat_uuid
        )

        updated = await conn.fetchrow(
            """
            SELECT
                sc.id,
                sc.user_id,
                u.email as user_email,
                u.full_name as user_name,
                sc.status,
                sc.assigned_to,
                assigned_user.full_name as assigned_name,
                sc.created_at,
                sc.updated_at
            FROM support_chats sc
            JOIN users u ON u.id = sc.user_id
            LEFT JOIN users assigned_user ON assigned_user.id = sc.assigned_to
            WHERE sc.id = $1
            """,
            chat_uuid
        )

    if not updated:
        raise NotFoundError("Чат не найден")

    return {
        "id": str(updated["id"]),
        "user_id": str(updated["user_id"]),
        "user_email": updated["user_email"],
        "user_name": updated["user_name"],
        "status": updated["status"],
        "assigned_to": str(updated["assigned_to"]) if updated["assigned_to"] else None,
        "assigned_name": updated["assigned_name"],
        "created_at": updated["created_at"].isoformat() if updated["created_at"] else None,
        "updated_at": updated["updated_at"].isoformat() if updated["updated_at"] else None,
    }


@router.post("/chats/{chat_id}/reopen")
async def reopen_chat(
    chat_id: str,
    authorization: Optional[str] = Header(None),
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """
    Открыть закрытый чат снова.
    """
    current_user = await get_current_support_user(authorization, pool)

    try:
        chat_uuid = uuid.UUID(chat_id)
    except ValueError:
        raise NotFoundError("Чат не найден")

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE support_chats SET status = 'open', updated_at = NOW() WHERE id = $1",
            chat_uuid
        )

        updated = await conn.fetchrow(
            """
            SELECT
                sc.id,
                sc.user_id,
                u.email as user_email,
                u.full_name as user_name,
                sc.status,
                sc.assigned_to,
                assigned_user.full_name as assigned_name,
                sc.created_at,
                sc.updated_at
            FROM support_chats sc
            JOIN users u ON u.id = sc.user_id
            LEFT JOIN users assigned_user ON assigned_user.id = sc.assigned_to
            WHERE sc.id = $1
            """,
            chat_uuid
        )

    if not updated:
        raise NotFoundError("Чат не найден")

    return {
        "id": str(updated["id"]),
        "user_id": str(updated["user_id"]),
        "user_email": updated["user_email"],
        "user_name": updated["user_name"],
        "status": updated["status"],
        "assigned_to": str(updated["assigned_to"]) if updated["assigned_to"] else None,
        "assigned_name": updated["assigned_name"],
        "created_at": updated["created_at"].isoformat() if updated["created_at"] else None,
        "updated_at": updated["updated_at"].isoformat() if updated["updated_at"] else None,
    }


@router.post("/chats/{chat_id}/read")
async def mark_messages_read(
    chat_id: str,
    authorization: Optional[str] = Header(None),
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """
    Отметить все сообщения от пользователя как прочитанные.
    """
    current_user = await get_current_support_user(authorization, pool)

    try:
        chat_uuid = uuid.UUID(chat_id)
    except ValueError:
        raise NotFoundError("Чат не найден")

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE support_messages
            SET is_read = TRUE
            WHERE chat_id = $1 AND sender_type = 'user' AND is_read = FALSE
            """,
            chat_uuid
        )

    return {"success": True}


# === User-side endpoints (for the chat widget) ===

@router.get("/user/chat")
async def get_user_chat(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Получить или создать чат текущего пользователя.
    """
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        # Try to find existing open chat
        chat = await conn.fetchrow(
            """
            SELECT id, status, created_at
            FROM support_chats
            WHERE user_id = $1 AND status != 'closed'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            user_id
        )

        if not chat:
            # Create new chat
            chat_id = uuid.uuid4()
            await conn.execute(
                """
                INSERT INTO support_chats (id, user_id, status, created_at, updated_at)
                VALUES ($1, $2, 'pending', NOW(), NOW())
                """,
                chat_id, user_id
            )
            chat = await conn.fetchrow(
                "SELECT id, status, created_at FROM support_chats WHERE id = $1",
                chat_id
            )

    return {
        "id": str(chat["id"]),
        "status": chat["status"],
        "created_at": chat["created_at"].isoformat() if chat["created_at"] else None,
    }


@router.get("/user/chat/messages")
async def get_user_messages(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Получить сообщения в чате пользователя.
    """
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        # Find user's chat
        chat = await conn.fetchrow(
            """
            SELECT id FROM support_chats
            WHERE user_id = $1 AND status != 'closed'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            user_id
        )

        if not chat:
            return {"messages": [], "total": 0}

        messages = await conn.fetch(
            """
            SELECT
                sm.id,
                sm.chat_id,
                sm.sender_id,
                sm.sender_type,
                CASE
                    WHEN sm.sender_type = 'support' THEN 'Поддержка'
                    ELSE u.full_name
                END as sender_name,
                sm.content,
                sm.is_read,
                sm.created_at
            FROM support_messages sm
            LEFT JOIN users u ON u.id = sm.sender_id
            WHERE sm.chat_id = $1
            ORDER BY sm.created_at ASC
            LIMIT $2 OFFSET $3
            """,
            chat["id"], limit, offset
        )

        total = await conn.fetchval(
            "SELECT COUNT(*) FROM support_messages WHERE chat_id = $1",
            chat["id"]
        )

    return {
        "messages": [
            {
                "id": str(msg["id"]),
                "chat_id": str(msg["chat_id"]),
                "sender_id": str(msg["sender_id"]),
                "sender_type": msg["sender_type"],
                "sender_name": msg["sender_name"],
                "content": msg["content"],
                "is_read": msg["is_read"],
                "created_at": msg["created_at"].isoformat() if msg["created_at"] else None,
            }
            for msg in messages
        ],
        "total": total or 0,
    }


@router.post("/user/chat/messages")
async def send_user_message(
    request: dict,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Отправить сообщение от пользователя в чат поддержки.
    """
    content = request.get("content", "").strip()

    if not content:
        raise AuthenticationError("Сообщение не может быть пустым")

    user_id = current_user["id"]

    async with pool.acquire() as conn:
        # Get or create chat
        chat = await conn.fetchrow(
            """
            SELECT id, status FROM support_chats
            WHERE user_id = $1 AND status != 'closed'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            user_id
        )

        if not chat:
            # Create new chat
            chat_id = uuid.uuid4()
            await conn.execute(
                """
                INSERT INTO support_chats (id, user_id, status, created_at, updated_at)
                VALUES ($1, $2, 'pending', NOW(), NOW())
                """,
                chat_id, user_id
            )
        else:
            chat_id = chat["id"]

        # Create message
        message_id = uuid.uuid4()
        await conn.execute(
            """
            INSERT INTO support_messages (id, chat_id, sender_id, sender_type, content, is_read, created_at)
            VALUES ($1, $2, $3, 'user', $4, FALSE, NOW())
            """,
            message_id, chat_id, user_id, content
        )

        # Update chat timestamp
        await conn.execute(
            "UPDATE support_chats SET updated_at = NOW() WHERE id = $1",
            chat_id
        )

        # Fetch the created message
        message = await conn.fetchrow(
            """
            SELECT
                sm.id,
                sm.chat_id,
                sm.sender_id,
                sm.sender_type,
                u.full_name as sender_name,
                sm.content,
                sm.is_read,
                sm.created_at
            FROM support_messages sm
            LEFT JOIN users u ON u.id = sm.sender_id
            WHERE sm.id = $1
            """,
            message_id
        )

    # Broadcast to WebSocket connections
    await broadcast_message(str(chat_id), {
        "id": str(message["id"]),
        "chat_id": str(message["chat_id"]),
        "sender_id": str(message["sender_id"]),
        "sender_type": message["sender_type"],
        "sender_name": message["sender_name"],
        "content": message["content"],
        "is_read": message["is_read"],
        "created_at": message["created_at"].isoformat() if message["created_at"] else None,
    })

    return {
        "id": str(message["id"]),
        "chat_id": str(message["chat_id"]),
        "sender_id": str(message["sender_id"]),
        "sender_type": message["sender_type"],
        "sender_name": message["sender_name"],
        "content": message["content"],
        "is_read": message["is_read"],
        "created_at": message["created_at"].isoformat() if message["created_at"] else None,
    }


# === WebSocket for real-time messaging ===

async def broadcast_message(chat_id: str, message: dict):
    """Broadcast message to all connected WebSocket clients for a chat."""
    if chat_id in active_connections:
        disconnected = []
        for user_id, ws in active_connections[chat_id].items():
            try:
                await ws.send_json(message)
            except Exception as e:
                logger.error(f"WebSocket send error: {e}")
                disconnected.append(user_id)

        # Clean up disconnected clients
        for user_id in disconnected:
            del active_connections[chat_id][user_id]


@router.websocket("/ws/{chat_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    chat_id: str,
    token: str = Query(...),
):
    """
    WebSocket endpoint for real-time chat messages.
    Connect with: ws://host/support/ws/{chat_id}?token=JWT_TOKEN
    """
    # Verify token
    payload = decode_access_token(token)
    if not payload:
        await websocket.close(code=4001)
        return

    user_id = payload.get("sub")
    if not user_id:
        await websocket.close(code=4001)
        return

    await websocket.accept()

    # Register connection
    if chat_id not in active_connections:
        active_connections[chat_id] = {}
    active_connections[chat_id][user_id] = websocket

    try:
        while True:
            # Keep connection alive, messages are sent via broadcast
            data = await websocket.receive_text()
            # Could handle incoming messages here if needed

    except WebSocketDisconnect:
        # Clean up on disconnect
        if chat_id in active_connections and user_id in active_connections[chat_id]:
            del active_connections[chat_id][user_id]
            if not active_connections[chat_id]:
                del active_connections[chat_id]
