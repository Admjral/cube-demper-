"""Partner authentication router - вход в партнёрский кабинет."""

from fastapi import APIRouter, Depends, Query
from typing import Annotated, Optional
import asyncpg
from datetime import timedelta, datetime
import uuid

from ..core.database import get_db_pool
from ..core.security import verify_password, create_access_token
from ..core.exceptions import AuthenticationError
from ..dependencies import get_current_partner

router = APIRouter()


@router.post("/login")
async def partner_login(
    credentials: dict,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
  """
  Логин партнёра по email и паролю.
  Возвращает JWT-токен, который используется в partner-cabinet.
  """
  email = credentials.get("email")
  password = credentials.get("password")

  if not email or not password:
    raise AuthenticationError("Email и пароль обязательны")

  async with pool.acquire() as conn:
    partner = await conn.fetchrow(
      "SELECT id, email, password_hash, full_name FROM partners WHERE email = $1",
      email,
    )

  if not partner:
    raise AuthenticationError("Неверный логин или пароль")

  if not verify_password(password, partner["password_hash"]):
    raise AuthenticationError("Неверный логин или пароль")

  access_token = create_access_token(
    user_id=partner["id"],
    role="partner",
    expires_delta=timedelta(hours=24),
  )

  return {
    "access_token": access_token,
    "token_type": "bearer",
  }


@router.get("/me")
async def partner_me(
    current_partner: Annotated[dict, Depends(get_current_partner)],
):
  """
  Текущий партнёр по JWT-токену.
  Используется partner-cabinet для отображения данных в кабинете.
  """
  return {
    "id": str(current_partner["id"]),
    "email": current_partner["email"],
    "full_name": current_partner.get("full_name"),
    "created_at": current_partner["created_at"],
    "updated_at": current_partner["updated_at"],
  }


@router.get("/stats")
async def partner_stats(
    current_partner: Annotated[dict, Depends(get_current_partner)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
  """
  Статистика партнёра: клики, регистрации, оплаты, заработок.
  """
  partner_id = current_partner["id"]

  async with pool.acquire() as conn:
    # Количество приведённых пользователей (регистрации)
    registrations = await conn.fetchval(
      "SELECT COUNT(*) FROM users WHERE partner_id = $1",
      partner_id
    )

    # Количество оплативших (у кого есть активная подписка)
    paid_users = await conn.fetchval(
      """
      SELECT COUNT(DISTINCT u.id)
      FROM users u
      JOIN subscriptions s ON s.user_id = u.id
      WHERE u.partner_id = $1 AND s.status = 'active'
      """,
      partner_id
    )

    # Общий заработок партнёра (сумма из partner_transactions)
    total_earned = await conn.fetchval(
      """
      SELECT COALESCE(SUM(amount), 0)
      FROM partner_transactions
      WHERE partner_id = $1 AND type = 'income'
      """,
      partner_id
    ) or 0

    # Доступный баланс (заработано минус выведено)
    total_withdrawn = await conn.fetchval(
      """
      SELECT COALESCE(SUM(ABS(amount)), 0)
      FROM partner_transactions
      WHERE partner_id = $1 AND type = 'payout' AND status = 'completed'
      """,
      partner_id
    ) or 0

    # Клики по реферальной ссылке
    clicks = await conn.fetchval(
      "SELECT COALESCE(clicks_count, 0) FROM partners WHERE id = $1",
      partner_id
    ) or 0

  return {
    "clicks": clicks,
    "registrations": registrations or 0,
    "paid_users": paid_users or 0,
    "total_earned": total_earned,
    "available_balance": total_earned - total_withdrawn,
    "total_withdrawn": total_withdrawn,
  }


@router.get("/leads")
async def partner_leads(
    current_partner: Annotated[dict, Depends(get_current_partner)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
  """
  Список приведённых клиентов партнёра.
  """
  partner_id = current_partner["id"]

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
        COALESCE(pt.amount, 0) as partner_earned
      FROM users u
      LEFT JOIN subscriptions s ON s.user_id = u.id AND s.status = 'active'
      LEFT JOIN partner_transactions pt ON pt.user_id = u.id AND pt.partner_id = $1 AND pt.type = 'income'
      WHERE u.partner_id = $1
      ORDER BY u.created_at DESC
      LIMIT $2 OFFSET $3
      """,
      partner_id, limit, offset
    )

    total = await conn.fetchval(
      "SELECT COUNT(*) FROM users WHERE partner_id = $1",
      partner_id
    )

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
    "total": total or 0,
  }


@router.get("/transactions")
async def partner_transactions(
    current_partner: Annotated[dict, Depends(get_current_partner)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
  """
  История транзакций партнёра (начисления и выплаты).
  """
  partner_id = current_partner["id"]

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
      FROM partner_transactions
      WHERE partner_id = $1
      ORDER BY created_at DESC
      LIMIT $2 OFFSET $3
      """,
      partner_id, limit, offset
    )

    total = await conn.fetchval(
      "SELECT COUNT(*) FROM partner_transactions WHERE partner_id = $1",
      partner_id
    )

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
    "total": total or 0,
  }


@router.post("/payout")
async def request_payout(
    request: dict,
    current_partner: Annotated[dict, Depends(get_current_partner)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
  """
  Запрос на выплату партнёру.
  """
  partner_id = current_partner["id"]
  amount = request.get("amount")
  requisites = request.get("requisites")

  if not amount or amount < 5000:
    raise AuthenticationError("Минимальная сумма выплаты: 5000 тенге")

  if not requisites:
    raise AuthenticationError("Укажите реквизиты для выплаты")

  async with pool.acquire() as conn:
    # Проверяем доступный баланс
    total_earned = await conn.fetchval(
      """
      SELECT COALESCE(SUM(amount), 0)
      FROM partner_transactions
      WHERE partner_id = $1 AND type = 'income'
      """,
      partner_id
    ) or 0

    total_withdrawn = await conn.fetchval(
      """
      SELECT COALESCE(SUM(ABS(amount)), 0)
      FROM partner_transactions
      WHERE partner_id = $1 AND type = 'payout'
      """,
      partner_id
    ) or 0

    available = total_earned - total_withdrawn

    if amount > available:
      raise AuthenticationError(f"Недостаточно средств. Доступно: {available} тенге")

    # Создаём запрос на выплату
    payout_id = await conn.fetchval(
      """
      INSERT INTO partner_transactions (id, partner_id, type, amount, description, status, requisites, created_at)
      VALUES ($1, $2, 'payout', $3, $4, 'pending', $5, NOW())
      RETURNING id
      """,
      uuid.uuid4(),
      partner_id,
      -amount,  # Отрицательная сумма для выплаты
      f"Запрос на выплату: {requisites}",
      requisites,
    )

  return {
    "success": True,
    "payout_id": str(payout_id),
    "message": f"Запрос на выплату {amount} тенге отправлен. Обработка до 24 часов.",
  }


@router.get("/promo-code")
async def partner_promo_code(
    current_partner: Annotated[dict, Depends(get_current_partner)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
):
  """
  Получить промокод и реферальную ссылку партнёра.
  """
  partner_id = current_partner["id"]

  async with pool.acquire() as conn:
    partner = await conn.fetchrow(
      "SELECT promo_code, referral_link FROM partners WHERE id = $1",
      partner_id
    )

  return {
    "promo_code": partner["promo_code"] if partner else None,
    "referral_link": partner["referral_link"] if partner else None,
  }

