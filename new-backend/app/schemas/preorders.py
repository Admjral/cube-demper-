"""Preorder schemas for API requests and responses"""

from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from datetime import datetime


class PreorderResponse(BaseModel):
    """Schema for preorder response"""
    id: str
    store_id: str
    product_id: str
    article: str
    name: str
    price: int = Field(..., description="Price in tiyns")
    warehouses: Dict
    delivery_days: int
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class PreorderCreate(BaseModel):
    """Schema for creating preorder"""
    store_id: str
    product_id: str
    article: str
    name: str
    price: int = Field(..., description="Price in tiyns", gt=0)
    warehouses: Dict = Field(..., description="Warehouse data")
    delivery_days: int = Field(default=30, ge=1, le=365)


class PreorderUpdate(BaseModel):
    """Schema for updating preorder"""
    price: Optional[int] = Field(None, gt=0)
    warehouses: Optional[Dict] = None
    delivery_days: Optional[int] = Field(None, ge=1, le=365)
    status: Optional[str] = None


class PreorderListResponse(BaseModel):
    """Schema for preorder list"""
    preorders: List[PreorderResponse]
    total: int
    page: int
    page_size: int
