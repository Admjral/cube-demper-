"""WhatsApp schemas for API requests and responses"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class WhatsAppSessionResponse(BaseModel):
    """Schema for WhatsApp session response"""
    id: str
    user_id: str
    waha_container_name: str
    waha_port: Optional[int]
    session_name: str
    phone_number: Optional[str]
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class WhatsAppSessionCreate(BaseModel):
    """Schema for creating WhatsApp session"""
    pass  # No parameters needed, creates for current user


class SendMessageRequest(BaseModel):
    """Schema for sending WhatsApp message"""
    phone: str = Field(..., description="Phone number in format: 77001234567")
    message: str = Field(..., min_length=1, max_length=4096)


class SendPollRequest(BaseModel):
    """Schema for sending WhatsApp poll"""
    phone: str
    question: str = Field(..., min_length=1, max_length=255)
    options: List[str] = Field(..., min_items=2, max_items=12)


class SendLocationRequest(BaseModel):
    """Schema for sending WhatsApp location"""
    phone: str
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    name: Optional[str] = None
    address: Optional[str] = None


class SendContactRequest(BaseModel):
    """Schema for sending WhatsApp contact (vCard)"""
    phone: str
    contact_name: str
    contact_phone: str


class WhatsAppTemplateCreate(BaseModel):
    """Schema for creating WhatsApp template"""
    name: str = Field(..., min_length=1, max_length=255)
    message: str = Field(..., min_length=1, max_length=4096)
    variables: Optional[Dict[str, str]] = Field(
        None,
        description="Template variables like {customer_name}, {order_id}"
    )
    trigger_event: Optional[str] = Field(
        None,
        description="Auto-trigger event: new_order, payment_received, etc."
    )
    is_active: bool = True


class WhatsAppTemplateUpdate(BaseModel):
    """Schema for updating WhatsApp template"""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    message: Optional[str] = Field(None, min_length=1, max_length=4096)
    variables: Optional[Dict[str, str]] = None
    trigger_event: Optional[str] = None
    is_active: Optional[bool] = None


class WhatsAppTemplateResponse(BaseModel):
    """Schema for WhatsApp template response"""
    id: str
    user_id: str
    name: str
    message: str
    variables: Optional[Dict[str, str]]
    trigger_event: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class WhatsAppWebhook(BaseModel):
    """Schema for WAHA webhook payload"""
    event: str
    session: str
    payload: Dict[str, Any]
