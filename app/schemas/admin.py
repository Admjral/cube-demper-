"""Admin schemas for API requests and responses"""

from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class UserAdminResponse(BaseModel):
    """Schema for user admin response"""
    id: str
    email: str
    full_name: Optional[str]
    role: str
    is_blocked: bool = False
    partner_id: Optional[str] = None
    partner_name: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    subscription_plan: Optional[str]
    subscription_status: Optional[str]
    subscription_end_date: Optional[datetime] = None
    stores_count: int
    products_count: int

    class Config:
        from_attributes = True


class SystemStats(BaseModel):
    """Schema for system statistics"""
    total_users: int
    active_subscriptions: int
    blocked_users: int = 0
    online_users: int = 0
    total_products: int
    active_demping_products: int
    total_revenue_tiyns: int
    monthly_revenue: int = 0
    new_connections: int = 0
    demper_workers_status: dict


class UserListResponse(BaseModel):
    """Schema for user list"""
    users: List[UserAdminResponse]
    total: int
    page: int
    page_size: int


class UpdateUserRoleRequest(BaseModel):
    """Schema for updating user role"""
    user_id: str
    role: str  # 'user' or 'admin'


class DemperWorkerStatus(BaseModel):
    """Schema for demper worker status"""
    instance_index: int
    status: str
    last_seen: datetime
    products_processed_today: int
    current_tasks: int


class BlockUserRequest(BaseModel):
    """Schema for blocking user"""
    reason: Optional[str] = None


class ExtendSubscriptionRequest(BaseModel):
    """Schema for extending subscription"""
    days: int  # Number of days to extend


class PartnerResponse(BaseModel):
    """Schema for partner response"""
    id: str
    email: str
    full_name: Optional[str]
    created_at: datetime
    updated_at: datetime
    referred_users_count: int = 0

    class Config:
        from_attributes = True


class PartnerCreateRequest(BaseModel):
    """Schema for creating partner"""
    email: str
    password: str
    full_name: Optional[str] = None


class PartnerListResponse(BaseModel):
    """Schema for partner list"""
    partners: List[PartnerResponse]
    total: int


class PartnerStatsResponse(BaseModel):
    """Schema for partner statistics"""
    partner_id: str
    partner_email: str
    referred_users_count: int
    active_subscriptions_count: int
    total_revenue_tiyns: int


class StoreAdminResponse(BaseModel):
    """Schema for store admin response"""
    id: str
    user_id: str
    user_email: str
    user_name: Optional[str]
    merchant_id: str
    name: str
    products_count: int
    is_active: bool
    last_sync: Optional[datetime]
    created_at: datetime

    class Config:
        from_attributes = True


class StoreListResponse(BaseModel):
    """Schema for store list"""
    stores: List[StoreAdminResponse]
    total: int
    page: int
    page_size: int


class UserDetailsResponse(BaseModel):
    """Schema for detailed user information"""
    id: str
    email: str
    full_name: Optional[str]
    role: str
    is_blocked: bool = False
    partner_id: Optional[str] = None
    partner_name: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    subscription: Optional[dict] = None
    stores: List[dict] = []
    payments: List[dict] = []
