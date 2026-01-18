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
    # Product-level demping settings
    max_price: Optional[int] = Field(None, description="Maximum price in tiyns")
    min_price: Optional[int] = Field(None, description="Minimum price in tiyns")
    price_step_override: Optional[int] = Field(None, description="Price step override in tiyns")
    demping_strategy: Optional[str] = Field(None, pattern="^(standard|always_first|stay_top_n)$")
    strategy_params: Optional[dict] = None


class BulkPriceUpdateRequest(BaseModel):
    """Schema for bulk price update"""
    product_ids: List[str]
    price_change_percent: Optional[float] = None
    price_change_tiyns: Optional[int] = None
    bot_active: Optional[bool] = None


class DempingSettings(BaseModel):
    """Schema for demping settings"""
    id: str
    store_id: str
    min_profit: int = Field(..., description="Minimum profit in tiyns", ge=0)
    bot_active: bool = True
    price_step: int = Field(100, description="Price adjustment step in tiyns", ge=0)
    min_margin_percent: int = Field(5, ge=0, le=100)
    check_interval_minutes: int = Field(15, ge=1, le=1440)
    work_hours_start: str = Field("09:00", pattern=r"^\d{2}:\d{2}$")
    work_hours_end: str = Field("21:00", pattern=r"^\d{2}:\d{2}$")
    is_enabled: bool = True
    last_check: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class DempingSettingsUpdate(BaseModel):
    """Schema for updating demping settings"""
    min_profit: Optional[int] = Field(None, ge=0)
    bot_active: Optional[bool] = None
    price_step: Optional[int] = Field(None, ge=0)
    min_margin_percent: Optional[int] = Field(None, ge=0, le=100)
    check_interval_minutes: Optional[int] = Field(None, ge=1, le=1440)
    work_hours_start: Optional[str] = Field(None, pattern=r"^\d{2}:\d{2}$")
    work_hours_end: Optional[str] = Field(None, pattern=r"^\d{2}:\d{2}$")
    is_enabled: Optional[bool] = None


class ProductDempingDetails(BaseModel):
    """Detailed product demping information"""
    product_id: str
    product_name: str
    kaspi_sku: Optional[str]
    current_price: int
    min_profit: int
    bot_active: bool

    # Product-level settings (null = use global)
    max_price: Optional[int] = None
    min_price: Optional[int] = None
    price_step_override: Optional[int] = None
    demping_strategy: str = "standard"
    strategy_params: Optional[dict] = None

    # Global store settings (for display)
    store_price_step: int
    store_min_margin_percent: int
    store_work_hours_start: str
    store_work_hours_end: str

    # Statistics
    last_check_time: Optional[datetime] = None
    price_changes_count: int = 0

    class Config:
        from_attributes = True


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
