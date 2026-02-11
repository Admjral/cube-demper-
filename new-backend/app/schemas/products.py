"""Product schemas for API requests and responses"""

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class ProductResponse(BaseModel):
    """Schema for product response"""
    id: str
    store_id: str
    kaspi_product_id: str
    kaspi_sku: Optional[str]
    external_kaspi_id: Optional[str]
    name: str
    price: int = Field(..., description="Price in tiyns (1 KZT = 100 tiyns)")
    min_profit: int = Field(..., description="Minimum profit in tiyns")
    bot_active: bool
    pre_order_days: int = 0
    last_check_time: Optional[datetime]
    availabilities: Optional[dict]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ProductListResponse(BaseModel):
    """Schema for product list with pagination"""
    products: List[ProductResponse]
    total: int
    page: int
    page_size: int
    has_more: bool


class PriceHistoryResponse(BaseModel):
    """Schema for price history response"""
    id: str
    product_id: str
    old_price: int
    new_price: int
    competitor_price: Optional[int]
    change_reason: str
    created_at: datetime

    class Config:
        from_attributes = True


class ProductFilters(BaseModel):
    """Schema for product filtering"""
    store_id: Optional[str] = None
    bot_active: Optional[bool] = None
    search: Optional[str] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=500)


class ProductAnalytics(BaseModel):
    """Schema for product analytics response"""
    total_products: int
    active_demping: int
    total_price_changes_today: int
    average_profit_margin_tiyns: int
