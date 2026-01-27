"""Billing router - subscriptions and payments (TipTopPay integration)"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import Annotated
import asyncpg
import uuid
from datetime import datetime, timedelta

from ..schemas.billing import (
    SubscriptionResponse,
    CreateSubscriptionRequest,
    PaymentResponse,
    PaymentListResponse,
    SubscriptionPlan,
)
from ..core.database import get_db_pool
from ..dependencies import get_current_user
from ..config import settings

router = APIRouter()


@router.get("/plans", response_model=list[SubscriptionPlan])
async def get_subscription_plans():
    """Get available subscription plans"""
    return [
        SubscriptionPlan(
            name="free",
            price_tiyns=0,
            products_limit=settings.plan_free_products_limit,
            features=[
                "Up to 100 products",
                "Basic price monitoring",
                "Email support"
            ]
        ),
        SubscriptionPlan(
            name="basic",
            price_tiyns=settings.plan_basic_price_tiyns,
            products_limit=settings.plan_basic_products_limit,
            features=[
                "Up to 500 products",
                "Automatic price demping",
                "WhatsApp notifications",
                "Priority support"
            ]
        ),
        SubscriptionPlan(
            name="pro",
            price_tiyns=settings.plan_pro_price_tiyns,
            products_limit=settings.plan_pro_products_limit,
            features=[
                "Up to 5000 products",
                "Advanced analytics",
                "AI assistants (Lawyer, Accountant, Salesman)",
                "WhatsApp integration",
                "Priority support",
                "Custom integrations"
            ]
        )
    ]


@router.get("/subscription", response_model=SubscriptionResponse)
async def get_current_subscription(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Get current user's subscription"""
    async with pool.acquire() as conn:
        subscription = await conn.fetchrow(
            """
            SELECT id, user_id, plan, status, products_limit,
                   current_period_start, current_period_end,
                   created_at, updated_at
            FROM subscriptions
            WHERE user_id = $1 AND status = 'active'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            current_user['id']
        )

        if not subscription:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No active subscription found"
            )

        return SubscriptionResponse(
            id=str(subscription['id']),
            user_id=str(subscription['user_id']),
            plan=subscription['plan'],
            status=subscription['status'],
            products_limit=subscription['products_limit'],
            current_period_start=subscription['current_period_start'],
            current_period_end=subscription['current_period_end'],
            created_at=subscription['created_at'],
            updated_at=subscription['updated_at']
        )


@router.post("/subscribe", response_model=SubscriptionResponse, status_code=status.HTTP_201_CREATED)
async def create_subscription(
    subscription_request: CreateSubscriptionRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Create new subscription (upgrade/downgrade)"""
    # Validate plan
    if subscription_request.plan not in ['basic', 'pro']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid plan. Must be 'basic' or 'pro'"
        )

    # Get plan details
    if subscription_request.plan == 'basic':
        products_limit = settings.plan_basic_products_limit
        price_tiyns = settings.plan_basic_price_tiyns
    else:  # pro
        products_limit = settings.plan_pro_products_limit
        price_tiyns = settings.plan_pro_price_tiyns

    # TODO: Create TipTopPay payment
    # For now, just create subscription directly

    async with pool.acquire() as conn:
        # Deactivate old subscriptions
        await conn.execute(
            "UPDATE subscriptions SET status = 'cancelled' WHERE user_id = $1 AND status = 'active'",
            current_user['id']
        )

        # Create new subscription
        now = datetime.now()
        subscription = await conn.fetchrow(
            """
            INSERT INTO subscriptions (
                user_id, plan, status, products_limit,
                current_period_start, current_period_end
            )
            VALUES ($1, $2, 'active', $3, $4, $5)
            RETURNING *
            """,
            current_user['id'],
            subscription_request.plan,
            products_limit,
            now,
            now + timedelta(days=30)
        )

        # Create payment record
        await conn.execute(
            """
            INSERT INTO payments (user_id, amount, status, plan)
            VALUES ($1, $2, 'completed', $3)
            """,
            current_user['id'],
            price_tiyns,
            subscription_request.plan
        )

        return SubscriptionResponse(
            id=str(subscription['id']),
            user_id=str(subscription['user_id']),
            plan=subscription['plan'],
            status=subscription['status'],
            products_limit=subscription['products_limit'],
            current_period_start=subscription['current_period_start'],
            current_period_end=subscription['current_period_end'],
            created_at=subscription['created_at'],
            updated_at=subscription['updated_at']
        )


@router.get("/payments", response_model=PaymentListResponse)
async def list_payments(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    page: int = 1,
    page_size: int = 20
):
    """List payment history"""
    offset = (page - 1) * page_size

    async with pool.acquire() as conn:
        # Get total count
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM payments WHERE user_id = $1",
            current_user['id']
        )

        # Get payments
        payments = await conn.fetch(
            """
            SELECT id, user_id, amount, status, plan,
                   tiptoppay_transaction_id, created_at, updated_at
            FROM payments
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            """,
            current_user['id'],
            page_size,
            offset
        )

        payment_responses = [
            PaymentResponse(
                id=str(p['id']),
                user_id=str(p['user_id']),
                amount=p['amount'],
                status=p['status'],
                plan=p['plan'],
                tiptoppay_transaction_id=p['tiptoppay_transaction_id'],
                created_at=p['created_at'],
                updated_at=p['updated_at']
            )
            for p in payments
        ]

        return PaymentListResponse(
            payments=payment_responses,
            total=total,
            page=page,
            page_size=page_size
        )


@router.post("/cancel", status_code=status.HTTP_200_OK)
async def cancel_subscription(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Cancel current subscription"""
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE subscriptions
            SET status = 'cancelled'
            WHERE user_id = $1 AND status = 'active'
            """,
            current_user['id']
        )

        if result == "UPDATE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No active subscription to cancel"
            )

    return {"status": "success", "message": "Subscription cancelled"}
