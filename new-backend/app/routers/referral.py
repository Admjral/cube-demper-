"""User referral router - реферальная система для обычных пользователей."""

from fastapi import APIRouter, Depends, Query
from typing import Annotated
import asyncpg
import uuid

from ..core.database import get_db_pool
from ..core.exceptions import AuthenticationError
from ..dependencies import get_current_user

router = APIRouter()


@router.get("/stats")
async def referral_stats(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Статистика рефералов пользователя: регистрации, оплаты, заработок.
    """
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        # Количество приведённых пользователей (регистрации)
        registrations = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE referred_by = $1",
            user_id
        ) or 0

        # Количество оплативших (у кого есть активная подписка)
        paid_users = await conn.fetchval(
            """
            SELECT COUNT(DISTINCT u.id)
            FROM users u
            JOIN subscriptions s ON s.user_id = u.id
            WHERE u.referred_by = $1 AND s.status = 'active'
            """,
            user_id
        ) or 0

        # Общий заработок пользователя от рефералов
        total_earned = await conn.fetchval(
            """
            SELECT COALESCE(SUM(amount), 0)
            FROM referral_transactions
            WHERE user_id = $1 AND type = 'income'
            """,
            user_id
        ) or 0

        # Выведенная сумма
        total_withdrawn = await conn.fetchval(
            """
            SELECT COALESCE(SUM(ABS(amount)), 0)
            FROM referral_transactions
            WHERE user_id = $1 AND type = 'payout' AND status = 'completed'
            """,
            user_id
        ) or 0

        # Клики по ссылке (если отслеживаем)
        clicks = await conn.fetchval(
            "SELECT COALESCE(referral_clicks, 0) FROM users WHERE id = $1",
            user_id
        ) or 0

    return {
        "clicks": clicks,
        "registrations": registrations,
        "paid_users": paid_users,
        "total_earned": total_earned,
        "available_balance": total_earned - total_withdrawn,
        "total_withdrawn": total_withdrawn,
    }


@router.get("/leads")
async def referral_leads(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """
    Список приведённых пользователей.
    """
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        leads = await conn.fetch(
            """
            SELECT
                u.id,
                u.email,
                u.full_name,
                u.created_at as registered_at,
                CASE
                    WHEN s.id IS NOT NULL AND s.status = 'active' THEN 'paid'
                    ELSE 'registered'
                END as status,
                COALESCE(rt.amount, 0) as partner_earned
            FROM users u
            LEFT JOIN subscriptions s ON s.user_id = u.id AND s.status = 'active'
            LEFT JOIN referral_transactions rt ON rt.referred_user_id = u.id AND rt.user_id = $1 AND rt.type = 'income'
            WHERE u.referred_by = $1
            ORDER BY u.created_at DESC
            LIMIT $2 OFFSET $3
            """,
            user_id, limit, offset
        )

        total = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE referred_by = $1",
            user_id
        ) or 0

    return {
        "leads": [
            {
                "id": str(lead["id"]),
                "email": lead["email"],
                "full_name": lead["full_name"],
                "registered_at": lead["registered_at"].isoformat() if lead["registered_at"] else None,
                "status": lead["status"],
                "partner_earned": lead["partner_earned"],
            }
            for lead in leads
        ],
        "total": total,
    }


@router.get("/transactions")
async def referral_transactions(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """
    История транзакций реферальной программы (начисления и выплаты).
    """
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        transactions = await conn.fetch(
            """
            SELECT
                id,
                type,
                amount,
                description,
                status,
                created_at
            FROM referral_transactions
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            """,
            user_id, limit, offset
        )

        total = await conn.fetchval(
            "SELECT COUNT(*) FROM referral_transactions WHERE user_id = $1",
            user_id
        ) or 0

    return {
        "transactions": [
            {
                "id": str(t["id"]),
                "type": t["type"],
                "amount": t["amount"],
                "description": t["description"],
                "status": t["status"],
                "created_at": t["created_at"].isoformat() if t["created_at"] else None,
            }
            for t in transactions
        ],
        "total": total,
    }


@router.post("/payout")
async def request_payout(
    request: dict,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Запрос на выплату реферального заработка.
    """
    user_id = current_user["id"]
    amount = request.get("amount")
    requisites = request.get("requisites")

    if not amount or amount < 500000:  # 5000 тенге в тиынах
        raise AuthenticationError("Минимальная сумма выплаты: 5000 тенге")

    if not requisites:
        raise AuthenticationError("Укажите реквизиты для выплаты")

    async with pool.acquire() as conn:
        # Проверяем доступный баланс
        total_earned = await conn.fetchval(
            """
            SELECT COALESCE(SUM(amount), 0)
            FROM referral_transactions
            WHERE user_id = $1 AND type = 'income'
            """,
            user_id
        ) or 0

        total_withdrawn = await conn.fetchval(
            """
            SELECT COALESCE(SUM(ABS(amount)), 0)
            FROM referral_transactions
            WHERE user_id = $1 AND type = 'payout'
            """,
            user_id
        ) or 0

        available = total_earned - total_withdrawn

        if amount > available:
            raise AuthenticationError(f"Недостаточно средств. Доступно: {available / 100} ₸")

        # Создаём запрос на выплату
        payout_id = await conn.fetchval(
            """
            INSERT INTO referral_transactions (id, user_id, type, amount, description, status, created_at)
            VALUES ($1, $2, 'payout', $3, $4, 'pending', NOW())
            RETURNING id
            """,
            uuid.uuid4(),
            user_id,
            -amount,  # Отрицательная сумма для выплаты
            f"Запрос на выплату: {requisites}",
        )

    return {
        "success": True,
        "payout_id": str(payout_id),
        "message": f"Запрос на выплату {amount / 100} ₸ отправлен. Обработка до 24 часов.",
    }


@router.get("/link")
async def get_referral_link(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
    """
    Получить реферальную ссылку пользователя.
    """
    user_id = current_user["id"]

    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT referral_code FROM users WHERE id = $1",
            user_id
        )

        referral_code = user["referral_code"] if user and user["referral_code"] else str(user_id)[:8]

        # Если нет кода - генерируем и сохраняем
        if not user or not user["referral_code"]:
            referral_code = str(user_id)[:8].upper()
            await conn.execute(
                "UPDATE users SET referral_code = $1 WHERE id = $2",
                referral_code,
                user_id
            )

    # Генерируем ссылку
    base_url = "https://cube-demper.shop"
    referral_link = f"{base_url}/register?ref={referral_code}"

    return {
        "promo_code": referral_code,
        "referral_link": referral_link,
    }
