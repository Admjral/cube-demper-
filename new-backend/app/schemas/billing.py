"""Billing and subscription schemas for API requests and responses"""

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class SubscriptionPlan(BaseModel):
    """Schema for subscription plan info"""
    name: str
    price_tiyns: int
    products_limit: int
    features: List[str]


class SubscriptionResponse(BaseModel):
    """Schema for subscription response"""
    id: str
    user_id: str
    plan: str
    status: str
    products_limit: int
    current_period_start: datetime
    current_period_end: datetime
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class CreateSubscriptionRequest(BaseModel):
    """Schema for creating subscription"""
    plan: str = Field(..., description="Plan: basic or pro")


class PaymentResponse(BaseModel):
    """Schema for payment response"""
    id: str
    user_id: str
    amount: int
    status: str
    plan: Optional[str]
    tiptoppay_transaction_id: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class CreatePaymentRequest(BaseModel):
    """Schema for creating payment"""
    plan: str = Field(..., description="Plan: basic or pro")
    amount: int = Field(..., description="Amount in tiyns", gt=0)


class TipTopPayWebhook(BaseModel):
    """Schema for TipTopPay webhook"""
    TransactionId: str
    Amount: float
    Currency: str
    Status: str
    Email: Optional[str] = None
    Data: Optional[dict] = None


class PaymentListResponse(BaseModel):
    """Schema for payment list"""
    payments: List[PaymentResponse]
    total: int
    page: int
    page_size: int
