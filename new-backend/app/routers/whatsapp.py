"""
WhatsApp router - WAHA API integration for messaging

Использует общий WAHA контейнер из docker-compose.
Поддерживает:
- Управление сессиями (создание, статус, QR код)
- Отправку сообщений (текст, изображения, файлы, опросы, локации)
- Webhook для получения входящих сообщений
- Шаблоны сообщений
"""

from fastapi import APIRouter, Depends, HTTPException, status, Response, BackgroundTasks
from typing import Annotated, List, Optional
from pydantic import BaseModel
import asyncpg
import logging
import base64
from uuid import UUID
from datetime import datetime
import asyncio

import json as json_module

def _parse_jsonb(val):
    """Parse asyncpg JSONB value (returned as raw string)"""
    if val is None:
        return None
    if isinstance(val, str):
        try:
            return json_module.loads(val)
        except (json_module.JSONDecodeError, ValueError):
            return val
    return val

from ..schemas.whatsapp import (
    WhatsAppSessionResponse,
    SendMessageRequest,
    SendPollRequest,
    SendLocationRequest,
    SendContactRequest,
    WhatsAppTemplateCreate,
    WhatsAppTemplateUpdate,
    WhatsAppTemplateResponse,
    WhatsAppWebhook,
)
from ..core.database import get_db_pool
from ..dependencies import get_current_user
from ..services.waha_service import (
    get_waha_service,
    WahaService,
    WahaError,
    WahaConnectionError,
    WahaSessionError,
    WahaMessageError,
    WahaSessionStatus,
)
from ..config import settings

router = APIRouter()
logger = logging.getLogger(__name__)


def get_waha() -> WahaService:
    """Dependency для получения WAHA сервиса"""
    return get_waha_service()


def normalize_waha_status(waha_status: str) -> str:
    """Map WAHA-native status values to app-level status values."""
    mapping = {
        'WORKING': 'connected',
        'SCAN_QR_CODE': 'qr_pending',
        'FAILED': 'failed',
        'STOPPED': 'disconnected',
        'STARTING': 'connecting',
    }
    return mapping.get(waha_status, waha_status)


# ==================== SESSION MANAGEMENT ====================

@router.post("/session/create", response_model=WhatsAppSessionResponse, status_code=status.HTTP_201_CREATED)
async def create_whatsapp_session(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """
    Создать WhatsApp сессию для пользователя.
    
    Использует общий WAHA контейнер. Сессия именуется по user_id.
    После создания нужно отсканировать QR код через /session/qr
    """
    # Проверяем, включен ли WAHA
    if not settings.waha_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="WAHA service is currently disabled. Please enable it in configuration."
        )

    user_id = current_user['id']
    session_name = f"user-{user_id}"

    async with pool.acquire() as conn:
        # Проверяем существующую сессию
        existing = await conn.fetchrow(
            "SELECT id, status FROM whatsapp_sessions WHERE user_id = $1",
            user_id
        )

        if existing:
            # Если сессия уже есть, возвращаем её
            logger.info(f"Session already exists for user {user_id}")
            session = await conn.fetchrow(
                """
                SELECT id, user_id, waha_container_name, waha_port,
                       session_name, phone_number, status, created_at, updated_at
                FROM whatsapp_sessions
                WHERE user_id = $1
                """,
                user_id
            )
            return _session_to_response(session)

    # Создаём сессию в WAHA
    try:
        # Проверяем доступность WAHA
        if not await waha.health_check():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="WAHA service is not available. Please ensure WAHA container is running."
            )

        webhook_url = f"{settings.backend_url}/whatsapp/webhook"
        
        await waha.create_session(
            name=session_name,
            webhook_url=webhook_url,
            webhook_events=["message", "session.status", "message.ack"],
        )
        
        logger.info(f"Created WAHA session for user {user_id}: {session_name}")

        # Сохраняем в БД
        async with pool.acquire() as conn:
            session = await conn.fetchrow(
                """
                INSERT INTO whatsapp_sessions 
                (user_id, waha_container_name, session_name, status)
                VALUES ($1, $2, $3, $4)
                RETURNING id, user_id, waha_container_name, waha_port,
                          session_name, phone_number, status, created_at, updated_at
                """,
                user_id,
                "demper_waha",  # Имя контейнера из docker-compose
                session_name,
                WahaSessionStatus.SCAN_QR_CODE.value,
            )

        return _session_to_response(session)

    except WahaConnectionError as e:
        logger.error(f"WAHA connection error: {e.message}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Cannot connect to WAHA service: {e.message}"
        )
    except WahaError as e:
        logger.error(f"WAHA error creating session: {e.message}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create WhatsApp session: {e.message}"
        )
    except Exception as e:
        logger.error(f"Unexpected error creating session: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create WhatsApp session: {str(e)}"
        )


@router.get("/session", response_model=WhatsAppSessionResponse)
async def get_whatsapp_session(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """
    Получить статус WhatsApp сессии текущего пользователя.
    
    Также синхронизирует статус с WAHA API.
    """
    user_id = current_user['id']

    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            """
            SELECT id, user_id, waha_container_name, waha_port,
                   session_name, phone_number, status, created_at, updated_at
            FROM whatsapp_sessions
            WHERE user_id = $1
            """,
            user_id
        )

        if not session:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No WhatsApp session found. Create one first."
            )

        # Синхронизируем статус с WAHA
        try:
            waha_session = await waha.get_session(session['session_name'])
            waha_status = waha_session.get("status", session['status'])
            
            # Обновляем статус в БД если изменился
            if waha_status != session['status']:
                await conn.execute(
                    "UPDATE whatsapp_sessions SET status = $1, updated_at = NOW() WHERE id = $2",
                    waha_status,
                    session['id']
                )
                # Обновляем локальные данные
                session = await conn.fetchrow(
                    """
                    SELECT id, user_id, waha_container_name, waha_port,
                           session_name, phone_number, status, created_at, updated_at
                    FROM whatsapp_sessions
                    WHERE id = $1
                    """,
                    session['id']
                )
        except WahaError as e:
            logger.warning(f"Could not sync WAHA status: {e.message}")

        return _session_to_response(session)


@router.get("/session/qr")
async def get_qr_code(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
    qr_format: str = "image",
):
    """
    Получить QR код для авторизации WhatsApp.
    
    Args:
        qr_format: "image" для PNG изображения, "raw" для base64 строки
        
    Returns:
        PNG изображение или JSON с base64
    """
    user_id = current_user['id']

    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT session_name, status FROM whatsapp_sessions WHERE user_id = $1",
            user_id
        )

        if not session:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No WhatsApp session found. Create one first."
            )

        # Если сессия уже авторизована, QR не нужен
        if normalize_waha_status(session['status']) == 'connected':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Session already authenticated. QR code not needed."
            )

    try:
        if qr_format == "image":
            qr_bytes = await waha.get_qr_code(session['session_name'], format="image")
            return Response(
                content=qr_bytes,
                media_type="image/png",
                headers={"Cache-Control": "no-cache, no-store, must-revalidate"}
            )
        else:
            qr_data = await waha.get_qr_code(session['session_name'], format="raw")
            return qr_data

    except WahaSessionError as e:
        if e.status_code == 404:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Session not ready for QR code. Try again in a moment."
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get QR code: {e.message}"
        )
    except WahaError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get QR code: {e.message}"
        )


@router.delete("/session")
async def delete_whatsapp_session(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """
    Удалить WhatsApp сессию (logout).
    
    Удаляет сессию из WAHA и из базы данных.
    """
    user_id = current_user['id']

    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT id, session_name FROM whatsapp_sessions WHERE user_id = $1",
            user_id
        )

        if not session:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No WhatsApp session found"
            )

        # Удаляем из WAHA
        try:
            await waha.delete_session(session['session_name'])
            logger.info(f"Deleted WAHA session: {session['session_name']}")
        except WahaError as e:
            logger.warning(f"Could not delete WAHA session: {e.message}")
            # Продолжаем удаление из БД даже если WAHA не доступен

        # Удаляем из БД
        await conn.execute(
            "DELETE FROM whatsapp_sessions WHERE id = $1",
            session['id']
        )

    return {"message": "WhatsApp session deleted successfully"}


# ==================== MESSAGING ====================

@router.post("/send", status_code=status.HTTP_200_OK)
async def send_message(
    message_data: SendMessageRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """
    Отправить текстовое сообщение WhatsApp.
    
    Args:
        message_data: Номер телефона и текст сообщения
        
    Returns:
        Информация об отправленном сообщении
    """
    user_id = current_user['id']

    # Получаем сессию пользователя
    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT session_name, status FROM whatsapp_sessions WHERE user_id = $1",
            user_id
        )

        if not session:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No WhatsApp session found. Create one first."
            )

        if normalize_waha_status(session['status']) != 'connected':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"WhatsApp session not ready. Current status: {session['status']}. Please scan QR code first."
            )

    try:
        result = await waha.send_text(
            phone=message_data.phone,
            text=message_data.message,
            session=session['session_name'],
        )
        
        logger.info(f"Message sent to {message_data.phone} by user {user_id}")
        
        return {
            "success": True,
            "message_id": result.get("id"),
            "chat_id": result.get("chatId"),
            "timestamp": result.get("timestamp"),
        }

    except WahaMessageError as e:
        logger.error(f"Failed to send message: {e.message}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to send message: {e.message}"
        )
    except WahaError as e:
        logger.error(f"WAHA error: {e.message}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"WhatsApp error: {e.message}"
        )


@router.post("/send/poll", status_code=status.HTTP_200_OK)
async def send_poll(
    poll_data: SendPollRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """Отправить опрос WhatsApp"""
    session_name = await _get_active_session(current_user['id'], pool)

    try:
        result = await waha.send_poll(
            phone=poll_data.phone,
            question=poll_data.question,
            options=poll_data.options,
            session=session_name,
        )
        
        return {
            "success": True,
            "message_id": result.get("id"),
        }

    except WahaError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to send poll: {e.message}"
        )


@router.post("/send/location", status_code=status.HTTP_200_OK)
async def send_location(
    location_data: SendLocationRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """Отправить локацию WhatsApp"""
    session_name = await _get_active_session(current_user['id'], pool)

    try:
        result = await waha.send_location(
            phone=location_data.phone,
            latitude=location_data.latitude,
            longitude=location_data.longitude,
            name=location_data.name,
            address=location_data.address,
            session=session_name,
        )
        
        return {
            "success": True,
            "message_id": result.get("id"),
        }

    except WahaError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to send location: {e.message}"
        )


@router.post("/send/contact", status_code=status.HTTP_200_OK)
async def send_contact(
    contact_data: SendContactRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """Отправить контакт WhatsApp"""
    session_name = await _get_active_session(current_user['id'], pool)

    try:
        result = await waha.send_contact(
            phone=contact_data.phone,
            contact_name=contact_data.contact_name,
            contact_phone=contact_data.contact_phone,
            session=session_name,
        )
        
        return {
            "success": True,
            "message_id": result.get("id"),
        }

    except WahaError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to send contact: {e.message}"
        )


# ==================== BULK MESSAGING ====================

@router.post("/send/bulk", status_code=status.HTTP_202_ACCEPTED)
async def send_bulk_messages(
    phones: List[str],
    message: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """
    Отправить сообщение нескольким получателям (рассылка).
    
    Args:
        phones: Список номеров телефонов
        message: Текст сообщения
        
    Returns:
        Результаты отправки для каждого номера
    """
    session_name = await _get_active_session(current_user['id'], pool)

    results = []
    for phone in phones:
        try:
            result = await waha.send_text(
                phone=phone,
                text=message,
                session=session_name,
            )
            results.append({
                "phone": phone,
                "success": True,
                "message_id": result.get("id"),
            })
        except WahaError as e:
            results.append({
                "phone": phone,
                "success": False,
                "error": e.message,
            })

    success_count = sum(1 for r in results if r["success"])
    logger.info(f"Bulk send: {success_count}/{len(phones)} messages sent by user {current_user['id']}")

    return {
        "total": len(phones),
        "success": success_count,
        "failed": len(phones) - success_count,
        "results": results,
    }


# ==================== WEBHOOK ====================

@router.post("/webhook")
async def waha_webhook(
    payload: WhatsAppWebhook,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Webhook endpoint для получения событий от WAHA.
    
    Обрабатывает:
    - message: входящие сообщения
    - session.status: изменения статуса сессии
    - message.ack: подтверждения доставки
    """
    event = payload.event
    session_name = payload.session
    data = payload.payload

    logger.info(f"WAHA webhook received: {event} for session {session_name}")

    async with pool.acquire() as conn:
        if event == "session.status":
            # Обновляем статус сессии
            new_status = data.get("status", "UNKNOWN")
            
            await conn.execute(
                """
                UPDATE whatsapp_sessions 
                SET status = $1, updated_at = NOW()
                WHERE session_name = $2
                """,
                new_status,
                session_name,
            )
            
            logger.info(f"Session {session_name} status updated to {new_status}")

        elif event == "message":
            # Входящее сообщение
            message_data = data.get("message", {})
            from_number = data.get("from", "").replace("@c.us", "")
            body = message_data.get("body", "")
            
            logger.info(f"Incoming message from {from_number}: {body[:50]}...")
            
            # Можно сохранить в БД для истории
            # Можно передать в AI для автоответа
            # Пока просто логируем
            
        elif event == "message.ack":
            # Подтверждение доставки/прочтения
            ack_status = data.get("ack")
            # 1 = sent, 2 = delivered, 3 = read
            logger.debug(f"Message ACK: {ack_status}")

    return {"status": "ok"}


# ==================== TEMPLATES ====================

@router.get("/templates", response_model=List[WhatsAppTemplateResponse])
async def list_templates(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Получить список шаблонов сообщений"""
    async with pool.acquire() as conn:
        templates = await conn.fetch(
            """
            SELECT id, user_id, name, message, variables, trigger_event,
                   is_active, created_at, updated_at
            FROM whatsapp_templates
            WHERE user_id = $1
            ORDER BY created_at DESC
            """,
            current_user['id']
        )

        return [
            WhatsAppTemplateResponse(
                id=str(t['id']),
                user_id=str(t['user_id']),
                name=t['name'],
                name_en=t.get('name_en'),
                message=t['message'],
                variables=_parse_jsonb(t['variables']),
                trigger_event=t['trigger_event'],
                is_active=t['is_active'],
                created_at=t['created_at'],
                updated_at=t['updated_at']
            )
            for t in templates
        ]


@router.post("/templates", response_model=WhatsAppTemplateResponse, status_code=status.HTTP_201_CREATED)
async def create_template(
    template_data: WhatsAppTemplateCreate,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Создать шаблон сообщения"""
    # asyncpg requires JSON string for JSONB columns
    variables_json = None
    if template_data.variables is not None:
        variables_json = json_module.dumps(template_data.variables, ensure_ascii=False, default=str)

    async with pool.acquire() as conn:
        template = await conn.fetchrow(
            """
            INSERT INTO whatsapp_templates (
                user_id, name, name_en, message, variables, trigger_event, is_active
            )
            VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
            RETURNING *
            """,
            current_user['id'],
            template_data.name,
            template_data.name_en,
            template_data.message,
            variables_json,
            template_data.trigger_event,
            template_data.is_active
        )

        return WhatsAppTemplateResponse(
            id=str(template['id']),
            user_id=str(template['user_id']),
            name=template['name'],
            name_en=template.get('name_en'),
            message=template['message'],
            variables=_parse_jsonb(template['variables']),
            trigger_event=template['trigger_event'],
            is_active=template['is_active'],
            created_at=template['created_at'],
            updated_at=template['updated_at']
        )


@router.patch("/templates/{template_id}", response_model=WhatsAppTemplateResponse)
async def update_template(
    template_id: str,
    template_data: WhatsAppTemplateUpdate,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Обновить шаблон сообщения"""
    async with pool.acquire() as conn:
        # Проверяем принадлежность
        existing = await conn.fetchrow(
            "SELECT id FROM whatsapp_templates WHERE id = $1 AND user_id = $2",
            UUID(template_id),
            current_user['id']
        )
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Template not found"
            )

        # Собираем поля для обновления
        updates = []
        values = []
        idx = 1

        if template_data.name is not None:
            updates.append(f"name = ${idx}")
            values.append(template_data.name)
            idx += 1
        if template_data.name_en is not None:
            updates.append(f"name_en = ${idx}")
            values.append(template_data.name_en)
            idx += 1
        if template_data.message is not None:
            updates.append(f"message = ${idx}")
            values.append(template_data.message)
            idx += 1
        if template_data.variables is not None:
            updates.append(f"variables = ${idx}::jsonb")
            values.append(json_module.dumps(template_data.variables, ensure_ascii=False, default=str))
            idx += 1
        if template_data.trigger_event is not None:
            updates.append(f"trigger_event = ${idx}")
            values.append(template_data.trigger_event)
            idx += 1
        if template_data.is_active is not None:
            updates.append(f"is_active = ${idx}")
            values.append(template_data.is_active)
            idx += 1

        if not updates:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields to update"
            )

        values.append(UUID(template_id))
        
        template = await conn.fetchrow(
            f"""
            UPDATE whatsapp_templates
            SET {", ".join(updates)}, updated_at = NOW()
            WHERE id = ${idx}
            RETURNING *
            """,
            *values
        )

        return WhatsAppTemplateResponse(
            id=str(template['id']),
            user_id=str(template['user_id']),
            name=template['name'],
            name_en=template.get('name_en'),
            message=template['message'],
            variables=_parse_jsonb(template['variables']),
            trigger_event=template['trigger_event'],
            is_active=template['is_active'],
            created_at=template['created_at'],
            updated_at=template['updated_at']
        )


@router.delete("/templates/{template_id}")
async def delete_template(
    template_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Удалить шаблон сообщения"""
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM whatsapp_templates WHERE id = $1 AND user_id = $2",
            UUID(template_id),
            current_user['id']
        )
        
        if result == "DELETE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Template not found"
            )

    return {"message": "Template deleted successfully"}


# ==================== MESSAGE HISTORY ====================

class WhatsAppMessageResponse(BaseModel):
    """Ответ с информацией о сообщении"""
    id: str
    recipient_phone: str
    recipient_name: Optional[str]
    message_content: str
    message_type: str
    status: str
    template_name: Optional[str] = None
    error_message: Optional[str] = None
    sent_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    read_at: Optional[datetime] = None
    created_at: datetime


class MessageHistoryResponse(BaseModel):
    """Ответ со списком сообщений"""
    messages: List[WhatsAppMessageResponse]
    total: int
    page: int
    per_page: int


@router.get("/messages", response_model=MessageHistoryResponse)
async def get_message_history(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    status_filter: Optional[str] = None,
    phone: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    per_page: int = 50,
):
    """
    Получить историю отправленных сообщений.

    Фильтры:
    - status_filter: pending, sent, delivered, read, failed
    - phone: фильтр по номеру телефона
    - date_from, date_to: фильтр по дате (YYYY-MM-DD)
    """
    async with pool.acquire() as conn:
        # Build query with filters
        conditions = ["m.user_id = $1"]
        params = [current_user['id']]
        param_count = 1

        if status_filter:
            param_count += 1
            conditions.append(f"m.status = ${param_count}")
            params.append(status_filter)

        if phone:
            param_count += 1
            conditions.append(f"m.recipient_phone LIKE ${param_count}")
            params.append(f"%{phone}%")

        if date_from:
            param_count += 1
            conditions.append(f"m.created_at >= ${param_count}::date")
            params.append(date_from)

        if date_to:
            param_count += 1
            conditions.append(f"m.created_at < ${param_count}::date + interval '1 day'")
            params.append(date_to)

        where_clause = " AND ".join(conditions)

        # Get total count
        count_query = f"SELECT COUNT(*) FROM whatsapp_messages m WHERE {where_clause}"
        total = await conn.fetchval(count_query, *params)

        # Get messages with pagination
        offset = (page - 1) * per_page
        param_count += 1
        limit_param = param_count
        param_count += 1
        offset_param = param_count

        query = f"""
            SELECT
                m.id, m.recipient_phone, m.recipient_name, m.message_content,
                m.message_type, m.status, m.error_message,
                m.sent_at, m.delivered_at, m.read_at, m.created_at,
                t.name as template_name
            FROM whatsapp_messages m
            LEFT JOIN whatsapp_templates t ON m.template_id = t.id
            WHERE {where_clause}
            ORDER BY m.created_at DESC
            LIMIT ${limit_param} OFFSET ${offset_param}
        """

        messages = await conn.fetch(query, *params, per_page, offset)

        return MessageHistoryResponse(
            messages=[
                WhatsAppMessageResponse(
                    id=str(m['id']),
                    recipient_phone=m['recipient_phone'],
                    recipient_name=m['recipient_name'],
                    message_content=m['message_content'],
                    message_type=m['message_type'],
                    status=m['status'],
                    template_name=m['template_name'],
                    error_message=m['error_message'],
                    sent_at=m['sent_at'],
                    delivered_at=m['delivered_at'],
                    read_at=m['read_at'],
                    created_at=m['created_at'],
                )
                for m in messages
            ],
            total=total or 0,
            page=page,
            per_page=per_page,
        )


# ==================== STATISTICS ====================

class WhatsAppStatsResponse(BaseModel):
    """Статистика WhatsApp сообщений"""
    total_sent: int
    total_delivered: int
    total_read: int
    total_failed: int
    total_pending: int
    delivery_rate: float
    read_rate: float
    today_sent: int
    today_limit: int
    messages_by_day: List[dict]


@router.get("/stats", response_model=WhatsAppStatsResponse)
async def get_whatsapp_stats(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    days: int = 7,
):
    """
    Получить статистику WhatsApp сообщений.

    Args:
        days: количество дней для графика (по умолчанию 7)
    """
    async with pool.acquire() as conn:
        # Overall stats
        stats = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE status IN ('sent', 'delivered', 'read')) as total_sent,
                COUNT(*) FILTER (WHERE status IN ('delivered', 'read')) as total_delivered,
                COUNT(*) FILTER (WHERE status = 'read') as total_read,
                COUNT(*) FILTER (WHERE status = 'failed') as total_failed,
                COUNT(*) FILTER (WHERE status = 'pending') as total_pending
            FROM whatsapp_messages
            WHERE user_id = $1
        """, current_user['id'])

        total_sent = stats['total_sent'] or 0
        total_delivered = stats['total_delivered'] or 0
        total_read = stats['total_read'] or 0

        delivery_rate = (total_delivered / total_sent * 100) if total_sent > 0 else 0
        read_rate = (total_read / total_delivered * 100) if total_delivered > 0 else 0

        # Today's stats
        today_sent = await conn.fetchval("""
            SELECT COUNT(*)
            FROM whatsapp_messages
            WHERE user_id = $1
            AND created_at >= CURRENT_DATE
            AND status != 'failed'
        """, current_user['id']) or 0

        # Get daily limit from settings or default
        settings = await conn.fetchrow("""
            SELECT daily_limit
            FROM whatsapp_settings
            WHERE user_id = $1
        """, current_user['id'])
        today_limit = settings['daily_limit'] if settings else 100

        # Messages by day for chart
        messages_by_day = await conn.fetch("""
            SELECT
                DATE(created_at) as date,
                COUNT(*) FILTER (WHERE status IN ('sent', 'delivered', 'read')) as sent,
                COUNT(*) FILTER (WHERE status IN ('delivered', 'read')) as delivered,
                COUNT(*) FILTER (WHERE status = 'read') as read,
                COUNT(*) FILTER (WHERE status = 'failed') as failed
            FROM whatsapp_messages
            WHERE user_id = $1
            AND created_at >= CURRENT_DATE - $2::integer
            GROUP BY DATE(created_at)
            ORDER BY date
        """, current_user['id'], days)

        return WhatsAppStatsResponse(
            total_sent=total_sent,
            total_delivered=total_delivered,
            total_read=total_read,
            total_failed=stats['total_failed'] or 0,
            total_pending=stats['total_pending'] or 0,
            delivery_rate=round(delivery_rate, 1),
            read_rate=round(read_rate, 1),
            today_sent=today_sent,
            today_limit=today_limit,
            messages_by_day=[
                {
                    "date": str(row['date']),
                    "sent": row['sent'],
                    "delivered": row['delivered'],
                    "read": row['read'],
                    "failed": row['failed'],
                }
                for row in messages_by_day
            ],
        )


# ==================== HELPER FUNCTIONS ====================

def _session_to_response(session) -> WhatsAppSessionResponse:
    """Преобразовать запись БД в response schema"""
    return WhatsAppSessionResponse(
        id=str(session['id']),
        user_id=str(session['user_id']),
        waha_container_name=session['waha_container_name'] or "demper_waha",
        waha_port=session['waha_port'],
        session_name=session['session_name'] or "default",
        phone_number=session['phone_number'],
        status=session['status'],
        created_at=session['created_at'],
        updated_at=session['updated_at']
    )


async def _get_active_session(user_id: UUID, pool: asyncpg.Pool) -> str:
    """
    Получить имя активной сессии пользователя.
    
    Raises:
        HTTPException: если сессия не найдена или не активна
    """
    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT session_name, status FROM whatsapp_sessions WHERE user_id = $1",
            user_id
        )

        if not session:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No WhatsApp session found. Create one first."
            )

        if normalize_waha_status(session['status']) != 'connected':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"WhatsApp session not ready. Current status: {session['status']}. Please scan QR code first."
            )

        return session['session_name']


# ==================== SESSIONS API (matching frontend) ====================

class SessionListResponse(BaseModel):
    """Список сессий пользователя"""
    id: str
    user_id: str
    session_name: str
    phone_number: Optional[str] = None
    status: str
    last_seen: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class CreateSessionRequest(BaseModel):
    """Запрос на создание сессии"""
    name: str


class SettingsResponse(BaseModel):
    """Настройки WhatsApp пользователя"""
    id: str
    user_id: str
    daily_limit: int
    interval_seconds: int
    work_hours_start: str
    work_hours_end: str
    work_days: List[int]
    auto_reply_enabled: bool
    created_at: datetime
    updated_at: datetime


class UpdateSettingsRequest(BaseModel):
    """Запрос на обновление настроек"""
    daily_limit: Optional[int] = None
    interval_seconds: Optional[int] = None
    work_hours_start: Optional[str] = None
    work_hours_end: Optional[str] = None
    work_days: Optional[List[int]] = None
    auto_reply_enabled: Optional[bool] = None


@router.get("/sessions", response_model=List[SessionListResponse])
async def list_sessions(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Получить все сессии пользователя.
    """
    async with pool.acquire() as conn:
        sessions = await conn.fetch("""
            SELECT id, user_id, session_name, phone_number, status,
                   created_at, updated_at, created_at as last_seen
            FROM whatsapp_sessions
            WHERE user_id = $1
            ORDER BY created_at DESC
        """, current_user['id'])

        return [
            SessionListResponse(
                id=str(s['id']),
                user_id=str(s['user_id']),
                session_name=s['session_name'] or 'default',
                phone_number=s['phone_number'],
                status=normalize_waha_status(s['status'] or 'disconnected'),
                last_seen=s['last_seen'],
                created_at=s['created_at'],
                updated_at=s['updated_at'],
            )
            for s in sessions
        ]


@router.post("/sessions", response_model=SessionListResponse, status_code=status.HTTP_201_CREATED)
async def create_session_new(
    request: CreateSessionRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """
    Создать новую сессию WhatsApp.
    WAHA Core: только одна сессия 'default' на всех пользователей.
    WAHA Plus: уникальные сессии для каждого пользователя.
    """
    from ..config import settings
    waha_plus = getattr(settings, 'waha_plus', False)
    waha_api_key = getattr(settings, 'waha_api_key', None) or ''

    if waha_plus:
        # WAHA Plus: unique session per user
        # Sanitize name: WAHA only accepts a-z, A-Z, 0-9, -, _
        import re
        raw_name = request.name or f"user_{str(current_user['id'])[:8]}"
        session_name = re.sub(r'[^a-zA-Z0-9_-]', '', raw_name)
        if not session_name:
            session_name = f"user_{str(current_user['id'])[:8]}"
    else:
        # WAHA Core: only 'default' session supported
        session_name = "default"

    async with pool.acquire() as conn:
        if waha_plus:
            # WAHA Plus: check if this specific session exists for user
            existing = await conn.fetchrow(
                "SELECT id FROM whatsapp_sessions WHERE user_id = $1 AND session_name = $2",
                current_user['id'], session_name
            )
            if existing:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Session '{session_name}' already exists"
                )
        else:
            # WAHA Core: check if ANY session exists (shared 'default')
            existing = await conn.fetchrow(
                "SELECT id FROM whatsapp_sessions WHERE user_id = $1",
                current_user['id']
            )
            if existing:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="You already have a WhatsApp session. Upgrade to WAHA Plus for multiple sessions."
                )

        # Create session in WAHA
        try:
            result = await waha.create_session(session_name)
            logger.info(f"WAHA session created: {result}")
        except WahaConnectionError as e:
            logger.error(f"WAHA connection error: {e}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"WAHA service unavailable: {e.message}"
            )
        except WahaError as e:
            # If session already exists in WAHA, that's OK - continue
            if "already exists" not in str(e).lower():
                logger.error(f"WAHA session creation failed: {e}")
                raise HTTPException(
                    status_code=e.status_code or status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to create WAHA session: {e.message}"
                )

        # Insert into database
        session = await conn.fetchrow("""
            INSERT INTO whatsapp_sessions (user_id, session_name, status, waha_container_name, waha_port, waha_api_key)
            VALUES ($1, $2, 'connecting', 'demper_waha', 3000, $3)
            RETURNING id, user_id, session_name, phone_number, status, created_at, updated_at
        """, current_user['id'], session_name, waha_api_key)

        return SessionListResponse(
            id=str(session['id']),
            user_id=str(session['user_id']),
            session_name=session['session_name'],
            phone_number=session['phone_number'],
            status=session['status'],
            last_seen=None,
            created_at=session['created_at'],
            updated_at=session['updated_at'],
        )


@router.get("/sessions/{session_id}/qr")
async def get_session_qr_by_id(
    session_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """
    Получить QR код для конкретной сессии.
    """
    try:
        session_uuid = UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid session_id format")

    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT session_name, status FROM whatsapp_sessions WHERE id = $1 AND user_id = $2",
            session_uuid, current_user['id']
        )

        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        session_name = session['session_name'] or 'default'

        try:
            # Get QR code from WAHA - returns JSON with mimetype and data fields
            qr_response = await waha.get_qr_code(session_name, format="image")

            # WAHA returns either:
            # - bytes (raw PNG)
            # - dict with {"mimetype": "image/png", "data": "base64..."}
            if isinstance(qr_response, dict):
                # Extract base64 data from JSON response
                qr_base64 = qr_response.get("data", "")
            elif isinstance(qr_response, bytes):
                # Convert raw bytes to base64
                qr_base64 = base64.b64encode(qr_response).decode('utf-8')
            else:
                # Assume it's already a string
                qr_base64 = str(qr_response)

            # Update status
            await conn.execute(
                "UPDATE whatsapp_sessions SET status = 'qr_pending', updated_at = NOW() WHERE id = $1",
                session_uuid
            )

            return {"qr_code": qr_base64, "status": "qr_pending"}
        except WahaError as e:
            # Check if session is already connected
            try:
                status_info = await waha.get_session(session_name)
                if normalize_waha_status(status_info.get('status', '')) == 'connected':
                    await conn.execute(
                        "UPDATE whatsapp_sessions SET status = 'connected', updated_at = NOW() WHERE id = $1",
                        session_uuid
                    )
                    return {"qr_code": None, "status": "connected"}
            except Exception:
                pass
            raise HTTPException(status_code=500, detail=str(e))


class PairPhoneRequest(BaseModel):
    phone_number: str


@router.post("/sessions/{session_id}/pair-phone")
async def pair_by_phone(
    session_id: str,
    request: PairPhoneRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """
    Получить pairing code для подключения WhatsApp по номеру телефона.
    """
    try:
        session_uuid = UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid session_id format")

    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT session_name, status FROM whatsapp_sessions WHERE id = $1 AND user_id = $2",
            session_uuid, current_user['id']
        )

        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        session_name = session['session_name'] or 'default'

        try:
            code = await waha.request_pairing_code(session_name, request.phone_number)

            await conn.execute(
                "UPDATE whatsapp_sessions SET status = 'qr_pending', updated_at = NOW() WHERE id = $1",
                session_uuid
            )

            return {"code": code, "status": "pairing_pending"}
        except WahaError as e:
            logger.error(f"Pairing code error: {e}")
            raise HTTPException(status_code=500, detail=str(e.message))


@router.delete("/sessions/{session_id}")
async def delete_session_by_id(
    session_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """
    Удалить сессию WhatsApp.
    """
    try:
        session_uuid = UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid session_id format")

    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT session_name FROM whatsapp_sessions WHERE id = $1 AND user_id = $2",
            session_uuid, current_user['id']
        )

        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        session_name = session['session_name'] or 'default'

        # Delete from WAHA
        try:
            await waha.delete_session(session_name)
        except WahaError as e:
            logger.warning(f"WAHA session deletion warning: {e}")

        # Delete from database
        await conn.execute(
            "DELETE FROM whatsapp_sessions WHERE id = $1",
            session_uuid
        )

        return {"success": True}


@router.get("/settings", response_model=SettingsResponse)
async def get_whatsapp_settings(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Получить настройки WhatsApp пользователя.
    """
    async with pool.acquire() as conn:
        settings = await conn.fetchrow("""
            SELECT * FROM whatsapp_settings WHERE user_id = $1
        """, current_user['id'])

        if not settings:
            # Create default settings
            settings = await conn.fetchrow("""
                INSERT INTO whatsapp_settings (user_id)
                VALUES ($1)
                RETURNING *
            """, current_user['id'])

        return SettingsResponse(
            id=str(settings['id']),
            user_id=str(settings['user_id']),
            daily_limit=settings['daily_limit'] or 100,
            interval_seconds=settings['interval_seconds'] or 30,
            work_hours_start=settings['work_hours_start'] or '09:00',
            work_hours_end=settings['work_hours_end'] or '21:00',
            work_days=settings['work_days'] or [1, 2, 3, 4, 5],
            auto_reply_enabled=settings['auto_reply_enabled'] or False,
            created_at=settings['created_at'],
            updated_at=settings['updated_at'],
        )


@router.patch("/settings", response_model=SettingsResponse)
async def update_whatsapp_settings(
    request: UpdateSettingsRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Обновить настройки WhatsApp пользователя.
    """
    async with pool.acquire() as conn:
        # Ensure settings exist
        existing = await conn.fetchrow(
            "SELECT id FROM whatsapp_settings WHERE user_id = $1",
            current_user['id']
        )

        if not existing:
            await conn.execute(
                "INSERT INTO whatsapp_settings (user_id) VALUES ($1)",
                current_user['id']
            )

        # Build update query
        updates = []
        params = []
        param_count = 0

        if request.daily_limit is not None:
            param_count += 1
            updates.append(f"daily_limit = ${param_count}")
            params.append(request.daily_limit)

        if request.interval_seconds is not None:
            param_count += 1
            updates.append(f"interval_seconds = ${param_count}")
            params.append(request.interval_seconds)

        if request.work_hours_start is not None:
            param_count += 1
            updates.append(f"work_hours_start = ${param_count}")
            params.append(request.work_hours_start)

        if request.work_hours_end is not None:
            param_count += 1
            updates.append(f"work_hours_end = ${param_count}")
            params.append(request.work_hours_end)

        if request.work_days is not None:
            param_count += 1
            updates.append(f"work_days = ${param_count}")
            params.append(request.work_days)

        if request.auto_reply_enabled is not None:
            param_count += 1
            updates.append(f"auto_reply_enabled = ${param_count}")
            params.append(request.auto_reply_enabled)

        if updates:
            param_count += 1
            params.append(current_user['id'])

            query = f"""
                UPDATE whatsapp_settings
                SET {', '.join(updates)}, updated_at = NOW()
                WHERE user_id = ${param_count}
                RETURNING *
            """
            settings = await conn.fetchrow(query, *params)
        else:
            settings = await conn.fetchrow(
                "SELECT * FROM whatsapp_settings WHERE user_id = $1",
                current_user['id']
            )

        return SettingsResponse(
            id=str(settings['id']),
            user_id=str(settings['user_id']),
            daily_limit=settings['daily_limit'] or 100,
            interval_seconds=settings['interval_seconds'] or 30,
            work_hours_start=settings['work_hours_start'] or '09:00',
            work_hours_end=settings['work_hours_end'] or '21:00',
            work_days=settings['work_days'] or [1, 2, 3, 4, 5],
            auto_reply_enabled=settings['auto_reply_enabled'] or False,
            created_at=settings['created_at'],
            updated_at=settings['updated_at'],
        )


# ==================== CUSTOMER CONTACTS ====================

class CustomerContactResponse(BaseModel):
    id: str
    phone: str
    name: Optional[str] = None
    store_name: Optional[str] = None
    orders_count: int = 1
    first_order_code: Optional[str] = None
    last_order_code: Optional[str] = None
    is_blocked: bool = False
    created_at: datetime
    updated_at: datetime


class ContactsListResponse(BaseModel):
    contacts: List[CustomerContactResponse]
    total: int
    page: int
    per_page: int


@router.get("/contacts", response_model=ContactsListResponse)
async def get_customer_contacts(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    store_id: Optional[str] = None,
    search: Optional[str] = None,
    page: int = 1,
    per_page: int = 50,
):
    """Get customer contacts accumulated from orders."""
    async with pool.acquire() as conn:
        conditions = ["cc.user_id = $1", "cc.is_blocked = FALSE"]
        params: list = [current_user['id']]
        idx = 1

        if store_id:
            idx += 1
            conditions.append(f"cc.store_id = ${idx}")
            params.append(UUID(store_id))

        if search:
            idx += 1
            conditions.append(f"(cc.phone LIKE ${idx} OR cc.name ILIKE ${idx})")
            params.append(f"%{search}%")

        where = " AND ".join(conditions)
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM customer_contacts cc WHERE {where}", *params
        )

        offset = (page - 1) * per_page
        idx += 1
        limit_idx = idx
        idx += 1
        offset_idx = idx

        rows = await conn.fetch(f"""
            SELECT cc.*, ks.name as store_name
            FROM customer_contacts cc
            LEFT JOIN kaspi_stores ks ON ks.id = cc.store_id
            WHERE {where}
            ORDER BY cc.orders_count DESC, cc.updated_at DESC
            LIMIT ${limit_idx} OFFSET ${offset_idx}
        """, *params, per_page, offset)

        return ContactsListResponse(
            contacts=[
                CustomerContactResponse(
                    id=str(r['id']),
                    phone=r['phone'],
                    name=r['name'],
                    store_name=r['store_name'],
                    orders_count=r['orders_count'] or 1,
                    first_order_code=r['first_order_code'],
                    last_order_code=r['last_order_code'],
                    is_blocked=r['is_blocked'] or False,
                    created_at=r['created_at'],
                    updated_at=r['updated_at'],
                )
                for r in rows
            ],
            total=total or 0,
            page=page,
            per_page=per_page,
        )


@router.patch("/contacts/{contact_id}/block")
async def block_contact(
    contact_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """Block a contact (exclude from broadcasts)."""
    async with pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE customer_contacts SET is_blocked = TRUE, updated_at = NOW()
            WHERE id = $1 AND user_id = $2
        """, UUID(contact_id), current_user['id'])
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Contact not found")
    return {"success": True}


@router.patch("/contacts/{contact_id}/unblock")
async def unblock_contact(
    contact_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """Unblock a contact."""
    async with pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE customer_contacts SET is_blocked = FALSE, updated_at = NOW()
            WHERE id = $1 AND user_id = $2
        """, UUID(contact_id), current_user['id'])
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Contact not found")
    return {"success": True}


# ==================== BROADCAST CAMPAIGNS ====================

class BroadcastCampaignCreate(BaseModel):
    name: str
    message_text: str
    store_id: Optional[str] = None
    template_id: Optional[str] = None
    filter_min_orders: int = 0
    filter_store_id: Optional[str] = None
    scheduled_at: Optional[datetime] = None


class BroadcastCampaignResponse(BaseModel):
    id: str
    name: str
    message_text: str
    status: str
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_recipients: int = 0
    sent_count: int = 0
    failed_count: int = 0
    filter_min_orders: int = 0
    created_at: datetime


@router.get("/broadcasts", response_model=List[BroadcastCampaignResponse])
async def list_broadcasts(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """List broadcast campaigns."""
    async with pool.acquire() as conn:
        campaigns = await conn.fetch("""
            SELECT * FROM broadcast_campaigns
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT 50
        """, current_user['id'])

        return [
            BroadcastCampaignResponse(
                id=str(c['id']),
                name=c['name'],
                message_text=c['message_text'],
                status=c['status'],
                scheduled_at=c['scheduled_at'],
                started_at=c['started_at'],
                completed_at=c['completed_at'],
                total_recipients=c['total_recipients'] or 0,
                sent_count=c['sent_count'] or 0,
                failed_count=c['failed_count'] or 0,
                filter_min_orders=c['filter_min_orders'] or 0,
                created_at=c['created_at'],
            )
            for c in campaigns
        ]


@router.post("/broadcasts", response_model=BroadcastCampaignResponse, status_code=201)
async def create_broadcast(
    data: BroadcastCampaignCreate,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """Create a broadcast campaign (status=draft, not yet started)."""
    async with pool.acquire() as conn:
        conditions = ["cc.user_id = $1", "cc.is_blocked = FALSE"]
        params: list = [current_user['id']]
        idx = 1

        if data.filter_min_orders > 0:
            idx += 1
            conditions.append(f"cc.orders_count >= ${idx}")
            params.append(data.filter_min_orders)

        if data.filter_store_id:
            idx += 1
            conditions.append(f"cc.store_id = ${idx}")
            params.append(UUID(data.filter_store_id))

        where = " AND ".join(conditions)
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM customer_contacts cc WHERE {where}", *params
        )

        campaign = await conn.fetchrow("""
            INSERT INTO broadcast_campaigns (
                user_id, store_id, template_id, name, message_text,
                status, scheduled_at, total_recipients, filter_min_orders, filter_store_id
            )
            VALUES ($1, $2, $3, $4, $5, 'draft', $6, $7, $8, $9)
            RETURNING *
        """,
            current_user['id'],
            UUID(data.store_id) if data.store_id else None,
            UUID(data.template_id) if data.template_id else None,
            data.name,
            data.message_text,
            data.scheduled_at,
            total or 0,
            data.filter_min_orders,
            UUID(data.filter_store_id) if data.filter_store_id else None,
        )

        return BroadcastCampaignResponse(
            id=str(campaign['id']),
            name=campaign['name'],
            message_text=campaign['message_text'],
            status=campaign['status'],
            scheduled_at=campaign['scheduled_at'],
            started_at=campaign['started_at'],
            completed_at=campaign['completed_at'],
            total_recipients=campaign['total_recipients'] or 0,
            sent_count=campaign['sent_count'] or 0,
            failed_count=campaign['failed_count'] or 0,
            filter_min_orders=campaign['filter_min_orders'] or 0,
            created_at=campaign['created_at'],
        )


@router.post("/broadcasts/{campaign_id}/start")
async def start_broadcast(
    campaign_id: str,
    background_tasks: BackgroundTasks,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    waha: Annotated[WahaService, Depends(get_waha)],
):
    """Start a broadcast: select recipients, send in background."""
    async with pool.acquire() as conn:
        campaign = await conn.fetchrow("""
            SELECT * FROM broadcast_campaigns
            WHERE id = $1 AND user_id = $2
        """, UUID(campaign_id), current_user['id'])

        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")

        if campaign['status'] not in ('draft', 'failed'):
            raise HTTPException(status_code=400, detail=f"Cannot start campaign in '{campaign['status']}' status")

        session = await conn.fetchrow("""
            SELECT session_name, status FROM whatsapp_sessions
            WHERE user_id = $1 AND status IN ('connected', 'WORKING')
            LIMIT 1
        """, current_user['id'])

        if not session:
            raise HTTPException(status_code=400, detail="No active WhatsApp session")

        conditions = ["cc.user_id = $1", "cc.is_blocked = FALSE"]
        params: list = [current_user['id']]
        idx = 1

        if campaign['filter_min_orders'] and campaign['filter_min_orders'] > 0:
            idx += 1
            conditions.append(f"cc.orders_count >= ${idx}")
            params.append(campaign['filter_min_orders'])

        if campaign['filter_store_id']:
            idx += 1
            conditions.append(f"cc.store_id = ${idx}")
            params.append(campaign['filter_store_id'])

        where = " AND ".join(conditions)
        contacts = await conn.fetch(
            f"SELECT id, phone FROM customer_contacts cc WHERE {where}", *params
        )

        if not contacts:
            raise HTTPException(status_code=400, detail="No eligible recipients found")

        for contact in contacts:
            await conn.execute("""
                INSERT INTO broadcast_recipients (campaign_id, contact_id, phone, status)
                VALUES ($1, $2, $3, 'pending')
                ON CONFLICT DO NOTHING
            """, UUID(campaign_id), contact['id'], contact['phone'])

        await conn.execute("""
            UPDATE broadcast_campaigns
            SET status = 'sending', started_at = NOW(), total_recipients = $2, updated_at = NOW()
            WHERE id = $1
        """, UUID(campaign_id), len(contacts))

    background_tasks.add_task(
        _send_broadcast_background,
        campaign_id=campaign_id,
        session_name=session['session_name'],
        message_text=campaign['message_text'],
        pool=pool,
        waha=waha,
    )

    return {
        "success": True,
        "message": f"Broadcast started with {len(contacts)} recipients",
        "total_recipients": len(contacts),
    }


@router.post("/broadcasts/{campaign_id}/cancel")
async def cancel_broadcast(
    campaign_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """Cancel a broadcast (pending messages will be skipped)."""
    async with pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE broadcast_campaigns
            SET status = 'cancelled', completed_at = NOW(), updated_at = NOW()
            WHERE id = $1 AND user_id = $2 AND status IN ('draft', 'sending')
        """, UUID(campaign_id), current_user['id'])
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Campaign not found or cannot cancel")
    return {"success": True}


async def _send_broadcast_background(
    campaign_id: str,
    session_name: str,
    message_text: str,
    pool: asyncpg.Pool,
    waha: WahaService,
):
    """Background task: send messages to all broadcast recipients (15-30s anti-spam delay)."""
    import random

    sent = 0
    failed = 0

    try:
        async with pool.acquire() as conn:
            recipients = await conn.fetch("""
                SELECT id, phone FROM broadcast_recipients
                WHERE campaign_id = $1 AND status = 'pending'
                ORDER BY id
            """, UUID(campaign_id))

            for recipient in recipients:
                campaign_status = await conn.fetchval(
                    "SELECT status FROM broadcast_campaigns WHERE id = $1",
                    UUID(campaign_id)
                )
                if campaign_status == 'cancelled':
                    logger.info(f"Broadcast {campaign_id} cancelled, stopping")
                    break

                try:
                    await waha.send_text(
                        phone=recipient['phone'],
                        text=message_text,
                        session=session_name,
                    )
                    await conn.execute("""
                        UPDATE broadcast_recipients
                        SET status = 'sent', sent_at = NOW()
                        WHERE id = $1
                    """, recipient['id'])
                    sent += 1
                except Exception as e:
                    await conn.execute("""
                        UPDATE broadcast_recipients
                        SET status = 'failed', error_message = $2
                        WHERE id = $1
                    """, recipient['id'], str(e)[:500])
                    failed += 1

                await conn.execute("""
                    UPDATE broadcast_campaigns
                    SET sent_count = $2, failed_count = $3, updated_at = NOW()
                    WHERE id = $1
                """, UUID(campaign_id), sent, failed)

                await asyncio.sleep(random.uniform(15, 30))

            await conn.execute("""
                UPDATE broadcast_campaigns
                SET status = 'completed', completed_at = NOW(),
                    sent_count = $2, failed_count = $3, updated_at = NOW()
                WHERE id = $1 AND status = 'sending'
            """, UUID(campaign_id), sent, failed)

    except Exception as e:
        logger.error(f"Broadcast {campaign_id} error: {e}")
        try:
            async with pool.acquire() as conn:
                await conn.execute("""
                    UPDATE broadcast_campaigns
                    SET status = 'failed', sent_count = $2, failed_count = $3, updated_at = NOW()
                    WHERE id = $1
                """, UUID(campaign_id), sent, failed)
        except Exception:
            pass

    logger.info(f"Broadcast {campaign_id} finished: {sent} sent, {failed} failed")
