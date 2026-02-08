"""Admin router - admin panel endpoints"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import Annotated
import asyncpg
import uuid
from datetime import datetime, timedelta

from ..schemas.admin import (
    UserListResponse,
    UserAdminResponse,
    SystemStats,
    UpdateUserRoleRequest,
    BlockUserRequest,
    ExtendSubscriptionRequest,
    PartnerResponse,
    PartnerCreateRequest,
    PartnerListResponse,
    PartnerStatsResponse,
    StoreAdminResponse,
    StoreListResponse,
    UserDetailsResponse,
    PaymentAdminResponse,
    PaymentListResponse,
)
from ..core.database import get_db_pool
from ..core.redis import get_redis, get_online_users
from ..core.security import get_password_hash
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

        # Get users with subscription info, store counts, product counts, and partner info
        users = await conn.fetch(
            """
            SELECT
                u.id, u.email, u.full_name, u.role, u.is_blocked, u.partner_id,
                u.created_at, u.updated_at,
                s.plan as subscription_plan,
                s.status as subscription_status,
                s.current_period_end as subscription_end_date,
                p.full_name as partner_name,
                (
                    SELECT COUNT(DISTINCT k.id)
                    FROM kaspi_stores k
                    WHERE k.user_id = u.id
                ) as stores_count,
                (
                    SELECT COUNT(*)
                    FROM products pr
                    JOIN kaspi_stores k ON k.id = pr.store_id
                    WHERE k.user_id = u.id
                ) as products_count
            FROM users u
            LEFT JOIN subscriptions s ON s.user_id = u.id AND s.status = 'active'
            LEFT JOIN partners p ON p.id = u.partner_id
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
                is_blocked=u.get('is_blocked', False),
                partner_id=str(u['partner_id']) if u.get('partner_id') else None,
                partner_name=u.get('partner_name'),
                created_at=u['created_at'],
                updated_at=u['updated_at'],
                subscription_plan=u['subscription_plan'],
                subscription_status=u['subscription_status'],
                subscription_end_date=u.get('subscription_end_date'),
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
        # Get current month start
        now = datetime.now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        stats = await conn.fetchrow(
            """
            SELECT
                (SELECT COUNT(*) FROM users) as total_users,
                (SELECT COUNT(*) FROM users WHERE is_blocked = true) as blocked_users,
                (SELECT COUNT(*) FROM subscriptions WHERE status = 'active') as active_subscriptions,
                (SELECT COUNT(*) FROM products) as total_products,
                (SELECT COUNT(*) FROM products WHERE bot_active = true) as active_demping_products,
                (SELECT COALESCE(SUM(amount), 0) FROM payments WHERE status = 'completed') as total_revenue_tiyns,
                (SELECT COALESCE(SUM(amount), 0) FROM payments WHERE status = 'completed' AND created_at >= $1) as monthly_revenue,
                (SELECT COUNT(*) FROM kaspi_stores WHERE created_at >= $1) as new_connections
            """,
            month_start
        )

        # Get online users count
        online_user_ids = await get_online_users(threshold_minutes=5)
        online_users_count = len(online_user_ids)

        # TODO: Add demper workers status monitoring
        demper_workers_status = {
            "note": "Worker monitoring not yet implemented",
            "expected_workers": 4,
            "running_workers": 0
        }

        return SystemStats(
            total_users=stats['total_users'],
            active_subscriptions=stats['active_subscriptions'],
            blocked_users=stats['blocked_users'],
            online_users=online_users_count,
            total_products=stats['total_products'],
            active_demping_products=stats['active_demping_products'],
            total_revenue_tiyns=stats['total_revenue_tiyns'],
            monthly_revenue=stats['monthly_revenue'],
            new_connections=stats['new_connections'],
            demper_workers_status=demper_workers_status
        )


@router.patch("/users/role", status_code=status.HTTP_200_OK)
async def update_user_role(
    role_update: UpdateUserRoleRequest,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Update user role (admin only)"""
    if role_update.role not in ['user', 'admin', 'support']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role must be 'user', 'admin', or 'support'"
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


@router.post("/users/{user_id}/block", status_code=status.HTTP_200_OK)
async def block_user(
    user_id: str,
    request: BlockUserRequest,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Block user (admin only)"""
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE users SET is_blocked = true WHERE id = $1",
            uuid.UUID(user_id)
        )

        if result == "UPDATE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

    return {"status": "success", "message": "User blocked successfully"}


@router.post("/users/{user_id}/unblock", status_code=status.HTTP_200_OK)
async def unblock_user(
    user_id: str,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Unblock user (admin only)"""
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE users SET is_blocked = false WHERE id = $1",
            uuid.UUID(user_id)
        )

        if result == "UPDATE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

    return {"status": "success", "message": "User unblocked successfully"}


@router.post("/subscriptions/{subscription_id}/extend", status_code=status.HTTP_200_OK)
async def extend_subscription(
    subscription_id: str,
    request: ExtendSubscriptionRequest,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Extend subscription period (admin only)"""
    async with pool.acquire() as conn:
        # Get current subscription
        subscription = await conn.fetchrow(
            "SELECT id, current_period_end FROM subscriptions WHERE id = $1",
            uuid.UUID(subscription_id)
        )

        if not subscription:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Subscription not found"
            )

        # Calculate new end date
        current_end = subscription['current_period_end']
        if isinstance(current_end, str):
            current_end = datetime.fromisoformat(current_end.replace('Z', '+00:00'))
        new_end = current_end + timedelta(days=request.days)

        # Update subscription
        await conn.execute(
            "UPDATE subscriptions SET current_period_end = $1, updated_at = NOW() WHERE id = $2",
            new_end,
            uuid.UUID(subscription_id)
        )

    return {"status": "success", "message": f"Subscription extended by {request.days} days"}


@router.get("/users/{user_id}/details", response_model=UserDetailsResponse)
async def get_user_details(
    user_id: str,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Get detailed user information (admin only)"""
    async with pool.acquire() as conn:
        # Get user info
        user = await conn.fetchrow(
            """
            SELECT u.*, p.full_name as partner_name
            FROM users u
            LEFT JOIN partners p ON p.id = u.partner_id
            WHERE u.id = $1
            """,
            uuid.UUID(user_id)
        )

        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

        # Get subscription
        subscription = await conn.fetchrow(
            """
            SELECT id, plan, status, products_limit, current_period_start, current_period_end,
                   created_at, updated_at
            FROM subscriptions
            WHERE user_id = $1 AND status = 'active'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            uuid.UUID(user_id)
        )

        # Get stores
        stores = await conn.fetch(
            """
            SELECT id, merchant_id, name, products_count, is_active, last_sync, created_at
            FROM kaspi_stores
            WHERE user_id = $1
            ORDER BY created_at DESC
            """,
            uuid.UUID(user_id)
        )

        # Get payments
        payments = await conn.fetch(
            """
            SELECT id, amount, status, plan, created_at
            FROM payments
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT 10
            """,
            uuid.UUID(user_id)
        )

        return UserDetailsResponse(
            id=str(user['id']),
            email=user['email'],
            full_name=user['full_name'],
            role=user['role'],
            is_blocked=user.get('is_blocked', False),
            partner_id=str(user['partner_id']) if user.get('partner_id') else None,
            partner_name=user.get('partner_name'),
            created_at=user['created_at'],
            updated_at=user['updated_at'],
            subscription=dict(subscription) if subscription else None,
            stores=[dict(s) for s in stores],
            payments=[dict(p) for p in payments]
        )


@router.get("/partners", response_model=PartnerListResponse)
async def list_partners(
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """List all partners (admin only)"""
    async with pool.acquire() as conn:
        partners = await conn.fetch(
            """
            SELECT p.*,
                   (SELECT COUNT(*) FROM users WHERE partner_id = p.id) as referred_users_count
            FROM partners p
            ORDER BY p.created_at DESC
            """
        )

        partner_responses = [
            PartnerResponse(
                id=str(p['id']),
                email=p['email'],
                full_name=p['full_name'],
                created_at=p['created_at'],
                updated_at=p['updated_at'],
                referred_users_count=p['referred_users_count']
            )
            for p in partners
        ]

        return PartnerListResponse(
            partners=partner_responses,
            total=len(partner_responses)
        )


@router.post("/partners", response_model=PartnerResponse, status_code=status.HTTP_201_CREATED)
async def create_partner(
    partner_data: PartnerCreateRequest,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Create new partner (admin only)"""
    async with pool.acquire() as conn:
        # Check if partner already exists
        existing = await conn.fetchrow(
            "SELECT id FROM partners WHERE email = $1",
            partner_data.email
        )

        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Partner with this email already exists"
            )

        # Hash password
        password_hash = get_password_hash(partner_data.password)

        # Create partner
        partner = await conn.fetchrow(
            """
            INSERT INTO partners (email, password_hash, full_name)
            VALUES ($1, $2, $3)
            RETURNING id, email, full_name, created_at, updated_at
            """,
            partner_data.email,
            password_hash,
            partner_data.full_name
        )

        return PartnerResponse(
            id=str(partner['id']),
            email=partner['email'],
            full_name=partner['full_name'],
            created_at=partner['created_at'],
            updated_at=partner['updated_at'],
            referred_users_count=0
        )


@router.get("/partners/{partner_id}/stats", response_model=PartnerStatsResponse)
async def get_partner_stats(
    partner_id: str,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Get partner statistics (admin only)"""
    async with pool.acquire() as conn:
        # Get partner info
        partner = await conn.fetchrow(
            "SELECT id, email FROM partners WHERE id = $1",
            uuid.UUID(partner_id)
        )

        if not partner:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Partner not found"
            )

        # Get statistics
        stats = await conn.fetchrow(
            """
            SELECT
                (SELECT COUNT(*) FROM users WHERE partner_id = $1) as referred_users_count,
                (SELECT COUNT(*) FROM users u
                 JOIN subscriptions s ON s.user_id = u.id
                 WHERE u.partner_id = $1 AND s.status = 'active') as active_subscriptions_count,
                (SELECT COALESCE(SUM(p.amount), 0) FROM payments p
                 JOIN users u ON u.id = p.user_id
                 WHERE u.partner_id = $1 AND p.status = 'completed') as total_revenue_tiyns
            """,
            uuid.UUID(partner_id)
        )

        return PartnerStatsResponse(
            partner_id=str(partner['id']),
            partner_email=partner['email'],
            referred_users_count=stats['referred_users_count'],
            active_subscriptions_count=stats['active_subscriptions_count'],
            total_revenue_tiyns=stats['total_revenue_tiyns']
        )


@router.delete("/partners/{partner_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_partner(
    partner_id: str,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Delete partner (admin only)"""
    async with pool.acquire() as conn:
        # Remove partner_id from users first (set to NULL)
        await conn.execute(
            "UPDATE users SET partner_id = NULL WHERE partner_id = $1",
            uuid.UUID(partner_id)
        )

        # Delete partner
        result = await conn.execute(
            "DELETE FROM partners WHERE id = $1",
            uuid.UUID(partner_id)
        )

        if result == "DELETE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Partner not found"
            )

    return None


@router.get("/support-staff")
async def list_support_staff(
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """List all support staff (admin only)"""
    async with pool.acquire() as conn:
        staff = await conn.fetch(
            """
            SELECT id, email, full_name, created_at, updated_at
            FROM users
            WHERE role = 'support'
            ORDER BY created_at DESC
            """
        )

        return {
            "staff": [
                {
                    "id": str(s['id']),
                    "email": s['email'],
                    "full_name": s['full_name'],
                    "created_at": s['created_at'].isoformat() if s['created_at'] else None,
                    "updated_at": s['updated_at'].isoformat() if s['updated_at'] else None,
                }
                for s in staff
            ],
            "total": len(staff)
        }


@router.post("/support-staff", status_code=status.HTTP_201_CREATED)
async def create_support_staff(
    request: dict,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Create a new support staff account (admin only)"""
    email = request.get("email")
    password = request.get("password")
    full_name = request.get("full_name", "Support Staff")

    if not email or not password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email and password are required"
        )

    if len(password) < 6:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password must be at least 6 characters"
        )

    async with pool.acquire() as conn:
        # Check if user already exists
        existing = await conn.fetchrow(
            "SELECT id FROM users WHERE email = $1",
            email
        )

        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User with this email already exists"
            )

        # Hash password
        password_hash = get_password_hash(password)

        # Create support user
        user = await conn.fetchrow(
            """
            INSERT INTO users (email, password_hash, full_name, role)
            VALUES ($1, $2, $3, 'support')
            RETURNING id, email, full_name, role, created_at
            """,
            email,
            password_hash,
            full_name
        )

        return {
            "id": str(user['id']),
            "email": user['email'],
            "full_name": user['full_name'],
            "role": user['role'],
            "created_at": user['created_at'].isoformat() if user['created_at'] else None
        }


@router.get("/stores", response_model=StoreListResponse)
async def list_stores(
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    page: int = 1,
    page_size: int = 50
):
    """List all stores with user information (admin only)"""
    offset = (page - 1) * page_size

    async with pool.acquire() as conn:
        # Get total count
        total = await conn.fetchval("SELECT COUNT(*) FROM kaspi_stores")

        # Get stores with user info
        stores = await conn.fetch(
            """
            SELECT
                k.id, k.user_id, k.merchant_id, k.name, k.products_count,
                k.is_active, k.last_sync, k.created_at,
                u.email as user_email, u.full_name as user_name
            FROM kaspi_stores k
            JOIN users u ON u.id = k.user_id
            ORDER BY k.created_at DESC
            LIMIT $1 OFFSET $2
            """,
            page_size,
            offset
        )

        store_responses = [
            StoreAdminResponse(
                id=str(s['id']),
                user_id=str(s['user_id']),
                user_email=s['user_email'],
                user_name=s['user_name'],
                merchant_id=s['merchant_id'],
                name=s['name'],
                products_count=s['products_count'],
                is_active=s['is_active'],
                last_sync=s['last_sync'],
                created_at=s['created_at']
            )
            for s in stores
        ]

        return StoreListResponse(
            stores=store_responses,
            total=total,
            page=page,
            page_size=page_size
        )


@router.get("/payments", response_model=PaymentListResponse)
async def list_payments(
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    page: int = 1,
    page_size: int = 50
):
    """List all payments with user information (admin only)"""
    offset = (page - 1) * page_size

    async with pool.acquire() as conn:
        # Get total count
        total = await conn.fetchval("SELECT COUNT(*) FROM payments")

        # Get payments with user info
        payments = await conn.fetch(
            """
            SELECT
                p.id, p.user_id, p.amount, p.status, p.plan, p.created_at,
                u.email as user_email
            FROM payments p
            JOIN users u ON u.id = p.user_id
            ORDER BY p.created_at DESC
            LIMIT $1 OFFSET $2
            """,
            page_size,
            offset
        )

        payment_responses = [
            PaymentAdminResponse(
                id=str(p['id']),
                user_id=str(p['user_id']),
                user_email=p['user_email'],
                amount=p['amount'],
                status=p['status'],
                plan=p['plan'],
                created_at=p['created_at']
            )
            for p in payments
        ]

        return PaymentListResponse(
            payments=payment_responses,
            total=total,
            page=page,
            page_size=page_size
        )


# --- New tariff system subscription management ---

from pydantic import BaseModel
from typing import Optional
from ..services.feature_access import get_feature_access_service


class AssignSubscriptionRequest(BaseModel):
    """Request to assign subscription to user"""
    plan_code: str  # 'basic', 'standard', 'premium'
    days: int = 30  # Duration in days
    is_trial: bool = False
    notes: Optional[str] = None
    ends_at: Optional[str] = None  # ISO datetime, overrides days if set


class AssignAddonRequest(BaseModel):
    """Request to assign add-on to user"""
    addon_code: str  # 'ai_salesman', 'demping_100', etc.
    quantity: int = 1  # For stackable add-ons
    days: int = 30  # Duration in days


@router.post("/users/{user_id}/subscription", status_code=status.HTTP_201_CREATED)
async def assign_subscription(
    user_id: str,
    request: AssignSubscriptionRequest,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Assign subscription to user (admin only) - New tariff system"""
    async with pool.acquire() as conn:
        # Get plan
        plan = await conn.fetchrow(
            "SELECT * FROM plans WHERE code = $1 AND is_active = true",
            request.plan_code
        )
        if not plan:
            raise HTTPException(status_code=404, detail="Plan not found")

        # Check if user exists
        user = await conn.fetchrow("SELECT id FROM users WHERE id = $1", uuid.UUID(user_id))
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Deactivate existing subscriptions
        await conn.execute(
            "UPDATE subscriptions SET status = 'cancelled' WHERE user_id = $1 AND status = 'active'",
            uuid.UUID(user_id)
        )

        # Create new subscription
        now = datetime.now()
        if request.ends_at:
            period_end = datetime.fromisoformat(request.ends_at.replace('Z', '+00:00')).replace(tzinfo=None)
        else:
            period_end = now + timedelta(days=request.days)
        is_trial = request.is_trial and plan['trial_days'] > 0
        trial_ends_at = now + timedelta(days=plan['trial_days']) if is_trial else None

        subscription = await conn.fetchrow("""
            INSERT INTO subscriptions (
                user_id, plan_id, plan, status, products_limit,
                analytics_limit, demping_limit,
                current_period_start, current_period_end,
                is_trial, trial_ends_at, assigned_by, notes
            )
            VALUES ($1, $2, $3, 'active', $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING id, plan, status, current_period_end
        """,
            uuid.UUID(user_id),
            plan['id'],
            plan['code'],
            plan['demping_limit'],  # Legacy products_limit
            plan['analytics_limit'],
            plan['demping_limit'],
            now,
            period_end,
            is_trial,
            trial_ends_at,
            current_admin['id'],
            request.notes
        )

    return {
        "status": "success",
        "subscription_id": str(subscription['id']),
        "plan": subscription['plan'],
        "expires_at": subscription['current_period_end'].isoformat()
    }


@router.post("/users/{user_id}/subscription/cancel")
async def cancel_user_subscription(
    user_id: str,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Cancel user's active subscription (admin only)"""
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE subscriptions SET status = 'cancelled' WHERE user_id = $1 AND status = 'active'",
            uuid.UUID(user_id)
        )

        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="No active subscription found")

    return {"status": "success"}


@router.post("/users/{user_id}/addon", status_code=status.HTTP_201_CREATED)
async def assign_addon(
    user_id: str,
    request: AssignAddonRequest,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Assign add-on to user (admin only) - New tariff system"""
    async with pool.acquire() as conn:
        # Get addon
        addon = await conn.fetchrow(
            "SELECT * FROM addons WHERE code = $1 AND is_active = true",
            request.addon_code
        )
        if not addon:
            raise HTTPException(status_code=404, detail="Add-on not found")

        # Check if user exists
        user = await conn.fetchrow("SELECT id FROM users WHERE id = $1", uuid.UUID(user_id))
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.now()
        expires_at = now + timedelta(days=request.days) if addon['is_recurring'] else None

        # Upsert: update quantity if exists, insert otherwise
        result = await conn.fetchrow("""
            INSERT INTO user_addons (user_id, addon_id, quantity, status, starts_at, expires_at)
            VALUES ($1, $2, $3, 'active', $4, $5)
            ON CONFLICT (user_id, addon_id)
            DO UPDATE SET
                quantity = user_addons.quantity + $3,
                expires_at = GREATEST(user_addons.expires_at, $5),
                status = 'active',
                updated_at = NOW()
            RETURNING id, quantity
        """,
            uuid.UUID(user_id),
            addon['id'],
            request.quantity,
            now,
            expires_at
        )

    return {
        "status": "success",
        "user_addon_id": str(result['id']),
        "addon_code": request.addon_code,
        "quantity": result['quantity'],
        "expires_at": expires_at.isoformat() if expires_at else None
    }


@router.get("/users/{user_id}/subscription-details")
async def get_user_subscription_details(
    user_id: str,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Get detailed subscription info for user (admin only) - New tariff system"""
    # Check if user exists
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id, email FROM users WHERE id = $1", uuid.UUID(user_id))
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

    # Get features from service
    service = get_feature_access_service()
    features = await service.get_user_features(pool, uuid.UUID(user_id))

    # Get subscription record
    async with pool.acquire() as conn:
        subscription = await conn.fetchrow("""
            SELECT s.*, p.name as plan_name, p.price_tiyns, p.features as plan_features
            FROM subscriptions s
            LEFT JOIN plans p ON p.id = s.plan_id
            WHERE s.user_id = $1 AND s.status = 'active'
            ORDER BY s.created_at DESC LIMIT 1
        """, uuid.UUID(user_id))

        # Get user add-ons
        addons = await conn.fetch("""
            SELECT ua.*, a.code as addon_code, a.name as addon_name, a.price_tiyns
            FROM user_addons ua
            JOIN addons a ON a.id = ua.addon_id
            WHERE ua.user_id = $1 AND ua.status = 'active'
            AND (ua.expires_at IS NULL OR ua.expires_at >= NOW())
        """, uuid.UUID(user_id))

    return {
        "user_id": str(user_id),
        "user_email": user['email'],
        "subscription": {
            "id": str(subscription['id']) if subscription else None,
            "plan_id": str(subscription['plan_id']) if subscription and subscription.get('plan_id') else None,
            "plan_code": features['plan_code'],
            "plan_name": features['plan_name'],
            "status": subscription['status'] if subscription else None,
            "analytics_limit": features['analytics_limit'],
            "demping_limit": features['demping_limit'],
            "is_trial": features['is_trial'],
            "trial_ends_at": features['trial_ends_at'].isoformat() if features.get('trial_ends_at') else None,
            "ends_at": features['subscription_ends_at'].isoformat() if features.get('subscription_ends_at') else None,
            "notes": subscription['notes'] if subscription else None,
        } if subscription else None,
        "addons": [
            {
                "id": str(a['id']),
                "code": a['addon_code'],
                "name": a['addon_name'],
                "quantity": a['quantity'],
                "status": a['status'],
                "starts_at": a['starts_at'].isoformat() if a['starts_at'] else None,
                "expires_at": a['expires_at'].isoformat() if a['expires_at'] else None,
            }
            for a in addons
        ],
        "computed_features": features['features'],
        "computed_limits": {
            "analytics_limit": features['analytics_limit'],
            "demping_limit": features['demping_limit'],
        },
    }


@router.delete("/users/{user_id}/addon/{addon_code}", status_code=status.HTTP_200_OK)
async def remove_addon(
    user_id: str,
    addon_code: str,
    current_admin: Annotated[dict, Depends(get_current_admin_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Remove add-on from user (admin only)"""
    async with pool.acquire() as conn:
        # Get addon
        addon = await conn.fetchrow("SELECT id FROM addons WHERE code = $1", addon_code)
        if not addon:
            raise HTTPException(status_code=404, detail="Add-on not found")

        result = await conn.execute("""
            UPDATE user_addons SET status = 'cancelled', updated_at = NOW()
            WHERE user_id = $1 AND addon_id = $2 AND status = 'active'
        """, uuid.UUID(user_id), addon['id'])

        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="User does not have this add-on")

    return {"status": "success", "message": f"Add-on {addon_code} removed from user"}
