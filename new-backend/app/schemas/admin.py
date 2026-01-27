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
    created_at: datetime
    updated_at: datetime
    subscription_plan: Optional[str]
    subscription_status: Optional[str]
    stores_count: int
    products_count: int

    class Config:
        from_attributes = True


class SystemStats(BaseModel):
    """Schema for system statistics"""
    total_users: int
    active_subscriptions: int
    total_products: int
    active_demping_products: int
    total_revenue_tiyns: int
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
