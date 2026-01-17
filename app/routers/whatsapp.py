"""WhatsApp router - WAHA Core integration for messaging"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import Annotated, List
import asyncpg
import logging

from ..schemas.whatsapp import (
    WhatsAppSessionResponse,
    WhatsAppSessionCreate,
    SendMessageRequest,
    WhatsAppTemplateCreate,
    WhatsAppTemplateUpdate,
    WhatsAppTemplateResponse,
)
from ..core.database import get_db_pool
from ..dependencies import get_current_user
from ..services.railway_waha_service import RailwayWahaService
from ..config import settings

router = APIRouter()
logger = logging.getLogger(__name__)
waha_service = RailwayWahaService()


@router.post("/session/create", response_model=WhatsAppSessionResponse, status_code=status.HTTP_201_CREATED)
async def create_whatsapp_session(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """
    Create WhatsApp session (WAHA Core container via Railway API).
    Note: WAHA Core supports only one session per container.
    """
    # Check if WAHA is enabled (disabled on Railway by default)
    if not settings.waha_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="WAHA service is currently disabled. Please enable it in configuration."
        )

    async with pool.acquire() as conn:
        # Check if session already exists
        existing = await conn.fetchrow(
            "SELECT id FROM whatsapp_sessions WHERE user_id = $1",
            current_user['id']
        )

        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="WhatsApp session already exists for this user"
            )

    # Create WAHA service via Railway API
    try:
        result = await waha_service.create_user_session(current_user['id'], pool)
        logger.info(f"Created WAHA Railway service for user {current_user['id']}: {result['service_id']}")

        # Fetch the created session
        async with pool.acquire() as conn:
            session = await conn.fetchrow(
                """
                SELECT id, user_id, waha_container_name, waha_port,
                       session_name, phone_number, status, created_at, updated_at
                FROM whatsapp_sessions
                WHERE user_id = $1
                """,
                current_user['id']
            )

        return WhatsAppSessionResponse(
            id=str(session['id']),
            user_id=str(session['user_id']),
            waha_container_name=session['waha_container_name'],
            waha_port=session['waha_port'],
            session_name=session['session_name'],
            phone_number=session['phone_number'],
            status=session['status'],
            created_at=session['created_at'],
            updated_at=session['updated_at']
        )
    except Exception as e:
        logger.error(f"Failed to create WAHA session: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create WhatsApp session: {str(e)}"
        )


@router.get("/session", response_model=WhatsAppSessionResponse)
async def get_whatsapp_session(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Get current user's WhatsApp session status"""
    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            """
            SELECT id, user_id, waha_container_name, waha_port,
                   session_name, phone_number, status, created_at, updated_at
            FROM whatsapp_sessions
            WHERE user_id = $1
            """,
            current_user['id']
        )

        if not session:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No WhatsApp session found"
            )

        return WhatsAppSessionResponse(
            id=str(session['id']),
            user_id=str(session['user_id']),
            waha_container_name=session['waha_container_name'],
            waha_port=session['waha_port'],
            session_name=session['session_name'],
            phone_number=session['phone_number'],
            status=session['status'],
            created_at=session['created_at'],
            updated_at=session['updated_at']
        )


@router.post("/send", status_code=status.HTTP_200_OK)
async def send_message(
    message_data: SendMessageRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Send WhatsApp message"""
    # TODO: Implement WAHA message sending
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="WAHA integration pending implementation"
    )


@router.get("/templates", response_model=List[WhatsAppTemplateResponse])
async def list_templates(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """List WhatsApp message templates"""
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
    """Create WhatsApp message template"""
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
