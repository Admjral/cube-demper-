"""AI router - OpenAI assistants (Lawyer, Accountant, Salesman)"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import Annotated, List
import asyncpg
import logging
from datetime import datetime

from ..schemas.ai import (
    AIChatRequest,
    AIChatResponse,
    AIChatConversation,
    ClearHistoryRequest,
)
from ..core.database import get_db_pool
from ..dependencies import get_current_user

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/chat", response_model=AIChatResponse)
async def chat_with_assistant(
    chat_request: AIChatRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """
    Chat with AI assistant (lawyer, accountant, or salesman).

    Note: OpenAI integration pending. For now returns placeholder.
    """
    # TODO: Implement OpenAI API integration
    # 1. Fetch conversation history if include_history=True
    # 2. Build messages array with system prompt based on assistant_type
    # 3. Call OpenAI API
    # 4. Save user message and assistant response to database
    # 5. Return response

    # Placeholder response
    async with pool.acquire() as conn:
        # Save user message
        await conn.execute(
            """
            INSERT INTO ai_chat_history (user_id, assistant_type, role, content)
            VALUES ($1, $2, 'user', $3)
            """,
            current_user['id'],
            chat_request.assistant_type,
            chat_request.message
        )

        # Placeholder assistant response
        assistant_message = f"[{chat_request.assistant_type.upper()}] OpenAI integration pending. Your message: {chat_request.message}"

        # Save assistant response
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
    if assistant_type not in ['lawyer', 'accountant', 'salesman']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid assistant type. Must be: lawyer, accountant, or salesman"
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
