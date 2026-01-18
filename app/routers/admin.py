"""Admin router - admin panel endpoints"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import Annotated
import asyncpg
import uuid

from ..schemas.admin import (
    UserListResponse,
    UserAdminResponse,
    SystemStats,
    UpdateUserRoleRequest,
)
from ..core.database import get_db_pool
from ..dependencies import get_current_admin_user

router = APIRouter()


@router.get("/users", response_model=UserListResponse)
async def list_users(
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    page: int = 1,
    page_size: int = 50
):
    """List all users (admin only)"""
    offset = (page - 1) * page_size

    async with pool.acquire() as conn:
        # Get total count
        total = await conn.fetchval("SELECT COUNT(*) FROM users")

        # Get users with subscription info, store counts, and product counts
        users = await conn.fetch(
            """
            SELECT
                u.id, u.email, u.full_name, u.role, u.created_at, u.updated_at,
                s.plan as subscription_plan,
                s.status as subscription_status,
                (
                    SELECT COUNT(DISTINCT k.id)
                    FROM kaspi_stores k
                    WHERE k.user_id = u.id
                ) as stores_count,
                (
                    SELECT COUNT(*)
                    FROM products p
                    JOIN kaspi_stores k ON k.id = p.store_id
                    WHERE k.user_id = u.id
                ) as products_count
            FROM users u
            LEFT JOIN subscriptions s ON s.user_id = u.id
            ORDER BY u.created_at DESC
            LIMIT $1 OFFSET $2
            """,
            page_size,
            offset
        )

        user_responses = [
            UserAdminResponse(
                id=str(u['id']),
                email=u['email'],
                full_name=u['full_name'],
                role=u['role'],
                created_at=u['created_at'],
                updated_at=u['updated_at'],
                subscription_plan=u['subscription_plan'],
                subscription_status=u['subscription_status'],
                stores_count=u['stores_count'],
                products_count=u['products_count']
            )
            for u in users
        ]

        return UserListResponse(
            users=user_responses,
            total=total,
            page=page,
            page_size=page_size
        )


@router.get("/stats", response_model=SystemStats)
async def get_system_stats(
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Get system statistics (admin only)"""
    async with pool.acquire() as conn:
        stats = await conn.fetchrow(
            """
            SELECT
                (SELECT COUNT(*) FROM users) as total_users,
                (SELECT COUNT(*) FROM subscriptions WHERE status = 'active') as active_subscriptions,
                (SELECT COUNT(*) FROM products) as total_products,
                (SELECT COUNT(*) FROM products WHERE bot_active = true) as active_demping_products,
                (SELECT COALESCE(SUM(amount), 0) FROM payments WHERE status = 'completed') as total_revenue_tiyns
            """
        )

        # TODO: Add demper workers status monitoring
        demper_workers_status = {
            "note": "Worker monitoring not yet implemented",
            "expected_workers": 4,
            "running_workers": 0
        }

        return SystemStats(
            total_users=stats['total_users'],
            active_subscriptions=stats['active_subscriptions'],
            total_products=stats['total_products'],
            active_demping_products=stats['active_demping_products'],
            total_revenue_tiyns=stats['total_revenue_tiyns'],
            demper_workers_status=demper_workers_status
        )


@router.patch("/users/role", status_code=status.HTTP_200_OK)
async def update_user_role(
    role_update: UpdateUserRoleRequest,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Update user role (admin only)"""
    if role_update.role not in ['user', 'admin']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role must be 'user' or 'admin'"
        )

    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE users SET role = $1 WHERE id = $2",
            role_update.role,
            uuid.UUID(role_update.user_id)
        )

        if result == "UPDATE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

    return {"status": "success", "message": f"User role updated to {role_update.role}"}


@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: str,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Delete user (admin only)"""
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM users WHERE id = $1",
            uuid.UUID(user_id)
        )

        if result == "DELETE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

    return None
