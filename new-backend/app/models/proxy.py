"""Proxy models for per-user proxy pool with module allocation"""

from datetime import datetime
from typing import Optional
from uuid import UUID
from pydantic import BaseModel, Field


class ProxyBase(BaseModel):
    """Base proxy model"""
    host: str
    port: int
    protocol: str = "http"
    username: Optional[str] = None
    password: Optional[str] = None
    country: str = "NL"
    provider: Optional[str] = None
    cost_usd: Optional[float] = None
    is_residential: bool = False


class ProxyCreate(ProxyBase):
    """Schema for creating a new proxy"""
    pass


class Proxy(ProxyBase):
    """Full proxy model with all fields"""
    id: UUID
    user_id: Optional[UUID] = None
    allocated_at: Optional[datetime] = None
    status: str = "available"  # available, allocated, resting, dead
    module: Optional[str] = None  # demper, orders, catalog, reserve, NULL

    requests_count: int = 0
    max_requests: int = 249
    last_used_at: Optional[datetime] = None
    available_at: Optional[datetime] = None

    success_count: int = 0
    failure_count: int = 0
    last_error: Optional[str] = None
    last_health_check: Optional[datetime] = None

    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ProxyAllocation(BaseModel):
    """Schema for proxy allocation request"""
    user_id: UUID
    count: int = 100
    distribution: dict[str, int] = Field(
        default_factory=lambda: {
            "demper": 70,
            "orders": 25,
            "catalog": 5,
            "reserve": 0
        }
    )


class ProxyUsageLog(BaseModel):
    """Schema for proxy usage logging"""
    id: UUID
    proxy_id: UUID
    user_id: UUID
    module: Optional[str] = None

    requests_made: int
    success_count: int
    failure_count: int

    started_at: datetime
    finished_at: datetime

    class Config:
        from_attributes = True


class ProxyHealthStatus(BaseModel):
    """Schema for proxy health check response"""
    proxy_id: UUID
    status: str
    success_rate: float
    last_used_at: Optional[datetime]
    available_at: Optional[datetime]
    requests_count: int


class ProxyPoolStatus(BaseModel):
    """Schema for proxy pool status overview"""
    total: int
    available: int
    allocated: int
    resting: int
    dead: int
    users_with_proxies: int
    average_proxies_per_user: float
    per_module_allocation: dict[str, int]
