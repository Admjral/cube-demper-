"""Kaspi schemas for API requests and responses"""

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class KaspiStoreResponse(BaseModel):
    """Schema for Kaspi store response"""
    id: str
    user_id: str
    merchant_id: str
    name: str
    products_count: int
    last_sync: Optional[datetime]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class KaspiAuthRequest(BaseModel):
    """Schema for Kaspi authentication"""
    email: str
    password: str
    merchant_id: Optional[str] = None


class KaspiAuthSMSRequest(BaseModel):
    """Schema for Kaspi SMS verification"""
    merchant_id: str
    sms_code: str = Field(..., min_length=4, max_length=6)


class StoreSyncRequest(BaseModel):
    """Schema for store synchronization"""
    store_id: str


class ProductUpdateRequest(BaseModel):
    """Schema for product update"""
    price: Optional[int] = Field(None, description="Price in tiyns (1 KZT = 100 tiyns)")
    min_profit: Optional[int] = Field(None, description="Minimum profit in tiyns")
    bot_active: Optional[bool] = None


class BulkPriceUpdateRequest(BaseModel):
    """Schema for bulk price update"""
    product_ids: List[str]
    price_change_percent: Optional[float] = None
    price_change_tiyns: Optional[int] = None
    bot_active: Optional[bool] = None


class DempingSettings(BaseModel):
    """Schema for demping settings"""
    min_profit: int = Field(..., description="Minimum profit in tiyns", ge=0)
    bot_active: bool = True


class StoreCreateRequest(BaseModel):
    """Schema for creating a new store"""
    merchant_id: str
    name: str
    api_key: Optional[str] = None
