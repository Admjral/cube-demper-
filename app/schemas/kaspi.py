"""Kaspi schemas for API requests and responses"""

from pydantic import BaseModel, Field
from typing import Optional, List, Literal
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


# ============================================================================
# Analytics & Stats Schemas
# ============================================================================

class StoreStats(BaseModel):
    """Store statistics"""
    store_id: str
    store_name: str
    products_count: int
    active_products_count: int
    demping_enabled_count: int
    today_orders: int = 0
    today_revenue: int = 0
    week_orders: int = 0
    week_revenue: int = 0
    month_orders: int = 0
    month_revenue: int = 0
    avg_order_value: int = 0
    last_sync: Optional[datetime] = None


class DailyStats(BaseModel):
    """Daily statistics point"""
    date: str
    orders: int = 0
    revenue: int = 0
    items: int = 0


class SalesAnalytics(BaseModel):
    """Sales analytics with time series"""
    store_id: str
    period: Literal['7d', '30d', '90d']
    total_orders: int = 0
    total_revenue: int = 0
    total_items_sold: int = 0
    avg_order_value: int = 0
    daily_stats: List[DailyStats]


class TopProduct(BaseModel):
    """Top selling product"""
    id: str
    kaspi_sku: str
    name: str
    current_price: int
    sales_count: int = 0
    revenue: int = 0
