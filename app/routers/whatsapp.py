"""
WhatsApp router - WAHA API integration for messaging

Использует общий WAHA контейнер из docker-compose.
Поддерживает:
- Управление сессиями (создание, статус, QR код)
- Отправку сообщений (текст, изображения, файлы, опросы, локации)
- Webhook для получения входящих сообщений
- Шаблоны сообщений
"""

from fastapi import APIRouter, Depends, HTTPException, status, Response
from typing import Annotated, List
import asyncpg
import logging
from uuid import UUID

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
        if session['status'] == WahaSessionStatus.WORKING.value:
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

        if session['status'] != WahaSessionStatus.WORKING.value:
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
                message=t['message'],
                variables=t['variables'],
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
    async with pool.acquire() as conn:
        template = await conn.fetchrow(
            """
            INSERT INTO whatsapp_templates (
                user_id, name, message, variables, trigger_event, is_active
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
            """,
            current_user['id'],
            template_data.name,
            template_data.message,
            template_data.variables,
            template_data.trigger_event,
            template_data.is_active
        )

        return WhatsAppTemplateResponse(
            id=str(template['id']),
            user_id=str(template['user_id']),
            name=template['name'],
            message=template['message'],
            variables=template['variables'],
            trigger_event=template['trigger_event'],
            is_active=template['is_active'],
            created_at=template['created_at'],
            updated_at=template['updated_at']
        )


@router.put("/templates/{template_id}", response_model=WhatsAppTemplateResponse)
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
        if template_data.message is not None:
            updates.append(f"message = ${idx}")
            values.append(template_data.message)
            idx += 1
        if template_data.variables is not None:
            updates.append(f"variables = ${idx}")
            values.append(template_data.variables)
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
            message=template['message'],
            variables=template['variables'],
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

        if session['status'] != WahaSessionStatus.WORKING.value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"WhatsApp session not ready. Current status: {session['status']}. Please scan QR code first."
            )

        return session['session_name']
