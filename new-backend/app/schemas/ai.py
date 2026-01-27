"""AI Assistant schemas for API requests and responses"""

from pydantic import BaseModel, Field
from typing import List, Literal
from datetime import datetime


class AIChatMessage(BaseModel):
    """Schema for AI chat message"""
    role: Literal["user", "assistant"]
    content: str


class AIChatRequest(BaseModel):
    """Schema for AI chat request"""
    assistant_type: Literal["lawyer", "accountant", "salesman"]
    message: str = Field(..., min_length=1, max_length=4096)
    include_history: bool = Field(
        default=True,
        description="Include previous conversation history"
    )


class AIChatResponse(BaseModel):
    """Schema for AI chat response"""
    assistant_type: str
    message: str
    created_at: datetime


class AIChatHistoryResponse(BaseModel):
    """Schema for AI chat history response"""
    id: str
    user_id: str
    assistant_type: str
    role: str
    content: str
    created_at: datetime

    class Config:
        from_attributes = True


class AIChatConversation(BaseModel):
    """Schema for full conversation history"""
    assistant_type: str
    messages: List[AIChatHistoryResponse]
    total_messages: int


class ClearHistoryRequest(BaseModel):
    """Schema for clearing chat history"""
    assistant_type: Literal["lawyer", "accountant", "salesman"]
