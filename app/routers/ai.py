"""AI router - Gemini assistants (Lawyer, Salesman)"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from typing import Annotated, List, Optional
from pydantic import BaseModel, Field
import asyncpg
import logging
from datetime import datetime
from uuid import UUID

import google.generativeai as genai

from ..schemas.ai import (
    AIChatRequest,
    AIChatResponse,
    AIChatConversation,
    ClearHistoryRequest,
)
from ..core.database import get_db_pool
from ..dependencies import get_current_user
from ..config import settings
from ..services.ai_salesman_service import (
    process_order_for_upsell,
    get_ai_salesman,
    SalesmanTrigger,
)

router = APIRouter()
logger = logging.getLogger(__name__)


# ==================== SYSTEM PROMPTS ====================

LAWYER_SYSTEM_PROMPT = """Ты - профессиональный юрист-консультант, специализирующийся на законодательстве Республики Казахстан.

Твоя задача:
- Консультировать по вопросам гражданского, трудового, налогового и предпринимательского права РК
- Помогать с составлением и анализом договоров
- Разъяснять права и обязанности сторон
- Давать рекомендации по разрешению юридических споров
- Объяснять процедуры обращения в государственные органы

Важные правила:
1. Всегда ссылайся на конкретные статьи законов РК, если это применимо
2. Предупреждай, когда вопрос требует очной консультации с адвокатом
3. Не давай советов по уголовным делам - рекомендуй обратиться к адвокату
4. Отвечай на русском языке, понятным языком
5. Если вопрос не связан с юриспруденцией, вежливо объясни что специализируешься только на правовых вопросах

Актуальные законы РК которые ты знаешь:
- Гражданский кодекс РК
- Трудовой кодекс РК
- Налоговый кодекс РК
- Предпринимательский кодекс РК
- Закон о защите прав потребителей
- Закон о государственных закупках"""


# ==================== SCHEMAS ====================

class ProcessOrderRequest(BaseModel):
    """Запрос на обработку заказа ИИ продажником"""
    order_id: str = Field(..., description="UUID заказа")
    send_message: bool = Field(default=True, description="Отправить сообщение в WhatsApp")


class SalesmanMessageResponse(BaseModel):
    """Ответ с сгенерированным сообщением"""
    text: str
    trigger: str
    products_suggested: List[str]
    generated_at: datetime


class BulkProcessRequest(BaseModel):
    """Запрос на массовую обработку заказов"""
    order_ids: List[str] = Field(..., min_length=1, max_length=100)
    send_messages: bool = Field(default=True)


@router.post("/chat", response_model=AIChatResponse)
async def chat_with_assistant(
    chat_request: AIChatRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """
    Chat with AI assistant (lawyer or salesman).

    Uses Google Gemini for generating responses.
    """
    # Check if Gemini is configured
    if not settings.gemini_api_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="AI service not configured. Please contact support."
        )

    # Get system prompt based on assistant type
    if chat_request.assistant_type == "lawyer":
        system_prompt = LAWYER_SYSTEM_PROMPT
    else:
        # Salesman uses different service
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Use /ai/salesman/process-order for salesman functionality"
        )

    async with pool.acquire() as conn:
        # Build messages array
        messages = [{"role": "system", "content": system_prompt}]

        # Fetch conversation history if requested
        if chat_request.include_history:
            history = await conn.fetch(
                """
                SELECT role, content FROM ai_chat_history
                WHERE user_id = $1 AND assistant_type = $2
                ORDER BY created_at ASC
                LIMIT 20
                """,
                current_user['id'],
                chat_request.assistant_type
            )
            for msg in history:
                messages.append({"role": msg['role'], "content": msg['content']})

        # Add current user message
        messages.append({"role": "user", "content": chat_request.message})

        # Save user message to history
        await conn.execute(
            """
            INSERT INTO ai_chat_history (user_id, assistant_type, role, content)
            VALUES ($1, $2, 'user', $3)
            """,
            current_user['id'],
            chat_request.assistant_type,
            chat_request.message
        )

    # Call Gemini API
    try:
        genai.configure(api_key=settings.gemini_api_key)
        model = genai.GenerativeModel(
            model_name=settings.gemini_model,
            system_instruction=system_prompt
        )

        # Build chat history for Gemini format
        chat_history = []
        for msg in messages[1:]:  # Skip system message (already in system_instruction)
            role = "user" if msg["role"] == "user" else "model"
            chat_history.append({"role": role, "parts": [msg["content"]]})

        # Start chat and get response
        chat = model.start_chat(history=chat_history[:-1] if len(chat_history) > 1 else [])
        response = await chat.send_message_async(
            chat_request.message,
            generation_config=genai.GenerationConfig(
                max_output_tokens=settings.gemini_max_tokens,
                temperature=0.7,
            )
        )
        assistant_message = response.text
    except Exception as e:
        logger.error(f"Gemini API error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="AI service temporarily unavailable. Please try again later."
        )

    # Save assistant response to history
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ai_chat_history (user_id, assistant_type, role, content)
            VALUES ($1, $2, 'assistant', $3)
            """,
            current_user['id'],
            chat_request.assistant_type,
            assistant_message
        )

    return AIChatResponse(
        assistant_type=chat_request.assistant_type,
        message=assistant_message,
        created_at=datetime.now()
    )


@router.get("/history/{assistant_type}", response_model=AIChatConversation)
async def get_conversation_history(
    assistant_type: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    limit: int = 50
):
    """Get conversation history with specific assistant"""
    if assistant_type not in ['lawyer', 'salesman']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid assistant type. Must be: lawyer or salesman"
        )

    async with pool.acquire() as conn:
        messages = await conn.fetch(
            """
            SELECT id, user_id, assistant_type, role, content, created_at
            FROM ai_chat_history
            WHERE user_id = $1 AND assistant_type = $2
            ORDER BY created_at DESC
            LIMIT $3
            """,
            current_user['id'],
            assistant_type,
            limit
        )

        # Reverse to show oldest first
        messages = list(reversed(messages))

        from ..schemas.ai import AIChatHistoryResponse

        return AIChatConversation(
            assistant_type=assistant_type,
            messages=[
                AIChatHistoryResponse(
                    id=str(m['id']),
                    user_id=str(m['user_id']),
                    assistant_type=m['assistant_type'],
                    role=m['role'],
                    content=m['content'],
                    created_at=m['created_at']
                )
                for m in messages
            ],
            total_messages=len(messages)
        )


@router.post("/clear-history", status_code=status.HTTP_204_NO_CONTENT)
async def clear_chat_history(
    clear_request: ClearHistoryRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Clear conversation history with specific assistant"""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM ai_chat_history
            WHERE user_id = $1 AND assistant_type = $2
            """,
            current_user['id'],
            clear_request.assistant_type
        )

    return None


# ==================== AI SALESMAN ====================

@router.post("/salesman/process-order", response_model=SalesmanMessageResponse)
async def process_order_salesman(
    request: ProcessOrderRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Обработать заказ ИИ продажником.
    
    Генерирует персонализированное сообщение для допродажи
    и отправляет в WhatsApp клиенту (если send_message=True).
    
    Заказ должен принадлежать магазину текущего пользователя.
    """
    try:
        order_id = UUID(request.order_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid order_id format"
        )
    
    # Проверяем принадлежность заказа
    async with pool.acquire() as conn:
        order = await conn.fetchrow("""
            SELECT o.id, s.user_id
            FROM orders o
            JOIN kaspi_stores s ON o.store_id = s.id
            WHERE o.id = $1
        """, order_id)
        
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Order not found"
            )
        
        if str(order['user_id']) != str(current_user['id']):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Order does not belong to your store"
            )
    
    # Обрабатываем заказ
    result = await process_order_for_upsell(
        order_id=order_id,
        pool=pool,
        send_message=request.send_message,
    )
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Could not generate message for this order (missing customer phone or AI disabled)"
        )
    
    return SalesmanMessageResponse(
        text=result.text,
        trigger=result.trigger.value,
        products_suggested=result.products_suggested,
        generated_at=result.generated_at,
    )


@router.post("/salesman/process-bulk", status_code=status.HTTP_202_ACCEPTED)
async def process_orders_bulk(
    request: BulkProcessRequest,
    background_tasks: BackgroundTasks,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Массовая обработка заказов ИИ продажником.
    
    Обработка выполняется в фоне. Возвращает количество заказов в очереди.
    """
    # Валидируем order_ids
    valid_order_ids = []
    for oid in request.order_ids:
        try:
            valid_order_ids.append(UUID(oid))
        except ValueError:
            continue
    
    if not valid_order_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid order IDs provided"
        )
    
    # Проверяем принадлежность заказов пользователю
    async with pool.acquire() as conn:
        owned_orders = await conn.fetch("""
            SELECT o.id
            FROM orders o
            JOIN kaspi_stores s ON o.store_id = s.id
            WHERE o.id = ANY($1::uuid[]) AND s.user_id = $2
        """, valid_order_ids, current_user['id'])
        
        owned_ids = [row['id'] for row in owned_orders]
    
    if not owned_ids:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No orders belong to your stores"
        )
    
    # Запускаем обработку в фоне
    async def process_in_background():
        for order_id in owned_ids:
            try:
                await process_order_for_upsell(
                    order_id=order_id,
                    pool=pool,
                    send_message=request.send_messages,
                )
            except Exception as e:
                logger.error("Failed to process order %s: %s", order_id, str(e))
    
    background_tasks.add_task(process_in_background)
    
    return {
        "message": "Processing started",
        "orders_queued": len(owned_ids),
        "orders_skipped": len(valid_order_ids) - len(owned_ids),
    }


@router.get("/salesman/history")
async def get_salesman_history(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    limit: int = 50,
):
    """
    Получить историю сообщений ИИ продажника.
    
    Показывает последние сгенерированные и отправленные сообщения.
    """
    async with pool.acquire() as conn:
        # Получаем магазины пользователя
        stores = await conn.fetch(
            "SELECT id FROM kaspi_stores WHERE user_id = $1",
            current_user['id']
        )
        store_ids = [s['id'] for s in stores]
        
        if not store_ids:
            return {"messages": [], "total": 0}
        
        # Получаем историю сообщений
        messages = await conn.fetch("""
            SELECT 
                m.id, m.order_id, m.store_id, m.customer_phone,
                m.trigger_type, m.message_text, m.products_suggested,
                m.created_at, o.customer_name
            FROM ai_salesman_messages m
            LEFT JOIN orders o ON m.order_id = o.id
            WHERE m.store_id = ANY($1::uuid[])
            ORDER BY m.created_at DESC
            LIMIT $2
        """, store_ids, limit)
        
        return {
            "messages": [
                {
                    "id": str(m['id']),
                    "order_id": str(m['order_id']) if m['order_id'] else None,
                    "customer_phone": m['customer_phone'],
                    "customer_name": m['customer_name'],
                    "trigger": m['trigger_type'],
                    "text": m['message_text'],
                    "products_suggested": m['products_suggested'] or [],
                    "created_at": m['created_at'].isoformat(),
                }
                for m in messages
            ],
            "total": len(messages),
        }


# ==================== AI SALESMAN SETTINGS ====================

class AISalesmanSettingsResponse(BaseModel):
    """Настройки AI Salesman для магазина"""
    store_id: str
    store_name: str
    ai_enabled: bool
    ai_tone: Optional[str] = None
    ai_discount_percent: Optional[int] = None
    ai_promo_code: Optional[str] = None
    ai_review_bonus: Optional[str] = None
    ai_send_delay_minutes: int = 10
    ai_max_messages_per_day: int = 50


class UpdateAISalesmanSettingsRequest(BaseModel):
    """Запрос на обновление настроек AI Salesman"""
    ai_enabled: Optional[bool] = None
    ai_tone: Optional[str] = None
    ai_discount_percent: Optional[int] = None
    ai_promo_code: Optional[str] = None
    ai_review_bonus: Optional[str] = None
    ai_send_delay_minutes: Optional[int] = None
    ai_max_messages_per_day: Optional[int] = None


@router.get("/salesman/settings", response_model=List[AISalesmanSettingsResponse])
async def get_salesman_settings(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Получить настройки AI Salesman для всех магазинов пользователя.
    """
    async with pool.acquire() as conn:
        stores = await conn.fetch("""
            SELECT
                id, name,
                COALESCE(ai_enabled, true) as ai_enabled,
                ai_tone, ai_discount_percent, ai_promo_code, ai_review_bonus,
                COALESCE(ai_send_delay_minutes, 10) as ai_send_delay_minutes,
                COALESCE(ai_max_messages_per_day, 50) as ai_max_messages_per_day
            FROM kaspi_stores
            WHERE user_id = $1
        """, current_user['id'])

        return [
            AISalesmanSettingsResponse(
                store_id=str(s['id']),
                store_name=s['name'],
                ai_enabled=s['ai_enabled'],
                ai_tone=s['ai_tone'],
                ai_discount_percent=s['ai_discount_percent'],
                ai_promo_code=s['ai_promo_code'],
                ai_review_bonus=s['ai_review_bonus'],
                ai_send_delay_minutes=s['ai_send_delay_minutes'],
                ai_max_messages_per_day=s['ai_max_messages_per_day'],
            )
            for s in stores
        ]


@router.put("/salesman/settings/{store_id}", response_model=AISalesmanSettingsResponse)
async def update_salesman_settings(
    store_id: str,
    request: UpdateAISalesmanSettingsRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Обновить настройки AI Salesman для магазина.
    """
    try:
        store_uuid = UUID(store_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid store_id format"
        )

    async with pool.acquire() as conn:
        # Check ownership
        store = await conn.fetchrow(
            "SELECT id FROM kaspi_stores WHERE id = $1 AND user_id = $2",
            store_uuid, current_user['id']
        )

        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found or access denied"
            )

        # Build update query
        updates = []
        params = []
        param_count = 0

        if request.ai_enabled is not None:
            param_count += 1
            updates.append(f"ai_enabled = ${param_count}")
            params.append(request.ai_enabled)

        if request.ai_tone is not None:
            param_count += 1
            updates.append(f"ai_tone = ${param_count}")
            params.append(request.ai_tone)

        if request.ai_discount_percent is not None:
            param_count += 1
            updates.append(f"ai_discount_percent = ${param_count}")
            params.append(request.ai_discount_percent)

        if request.ai_promo_code is not None:
            param_count += 1
            updates.append(f"ai_promo_code = ${param_count}")
            params.append(request.ai_promo_code)

        if request.ai_review_bonus is not None:
            param_count += 1
            updates.append(f"ai_review_bonus = ${param_count}")
            params.append(request.ai_review_bonus)

        if request.ai_send_delay_minutes is not None:
            param_count += 1
            updates.append(f"ai_send_delay_minutes = ${param_count}")
            params.append(request.ai_send_delay_minutes)

        if request.ai_max_messages_per_day is not None:
            param_count += 1
            updates.append(f"ai_max_messages_per_day = ${param_count}")
            params.append(request.ai_max_messages_per_day)

        if not updates:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields to update"
            )

        param_count += 1
        params.append(store_uuid)

        query = f"""
            UPDATE kaspi_stores
            SET {', '.join(updates)}, updated_at = NOW()
            WHERE id = ${param_count}
            RETURNING
                id, name,
                COALESCE(ai_enabled, true) as ai_enabled,
                ai_tone, ai_discount_percent, ai_promo_code, ai_review_bonus,
                COALESCE(ai_send_delay_minutes, 10) as ai_send_delay_minutes,
                COALESCE(ai_max_messages_per_day, 50) as ai_max_messages_per_day
        """

        updated = await conn.fetchrow(query, *params)

        return AISalesmanSettingsResponse(
            store_id=str(updated['id']),
            store_name=updated['name'],
            ai_enabled=updated['ai_enabled'],
            ai_tone=updated['ai_tone'],
            ai_discount_percent=updated['ai_discount_percent'],
            ai_promo_code=updated['ai_promo_code'],
            ai_review_bonus=updated['ai_review_bonus'],
            ai_send_delay_minutes=updated['ai_send_delay_minutes'],
            ai_max_messages_per_day=updated['ai_max_messages_per_day'],
        )


@router.get("/salesman/stats")
async def get_salesman_stats(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    days: int = 7,
):
    """
    Получить статистику AI Salesman.
    """
    async with pool.acquire() as conn:
        # Get user's stores
        stores = await conn.fetch(
            "SELECT id FROM kaspi_stores WHERE user_id = $1",
            current_user['id']
        )
        store_ids = [s['id'] for s in stores]

        if not store_ids:
            return {
                "total_messages": 0,
                "by_trigger": {},
                "by_day": [],
                "top_products": [],
            }

        # Total messages and by trigger
        trigger_stats = await conn.fetch("""
            SELECT trigger_type, COUNT(*) as count
            FROM ai_salesman_messages
            WHERE store_id = ANY($1::uuid[])
            GROUP BY trigger_type
        """, store_ids)

        by_trigger = {row['trigger_type']: row['count'] for row in trigger_stats}
        total_messages = sum(by_trigger.values())

        # Messages by day
        by_day = await conn.fetch("""
            SELECT DATE(created_at) as date, COUNT(*) as count
            FROM ai_salesman_messages
            WHERE store_id = ANY($1::uuid[])
            AND created_at >= CURRENT_DATE - $2::integer
            GROUP BY DATE(created_at)
            ORDER BY date
        """, store_ids, days)

        # Top suggested products
        top_products = await conn.fetch("""
            SELECT unnest(products_suggested) as product, COUNT(*) as count
            FROM ai_salesman_messages
            WHERE store_id = ANY($1::uuid[])
            AND products_suggested IS NOT NULL
            GROUP BY product
            ORDER BY count DESC
            LIMIT 10
        """, store_ids)

        return {
            "total_messages": total_messages,
            "by_trigger": by_trigger,
            "by_day": [
                {"date": str(row['date']), "count": row['count']}
                for row in by_day
            ],
            "top_products": [
                {"product": row['product'], "count": row['count']}
                for row in top_products
            ],
        }
