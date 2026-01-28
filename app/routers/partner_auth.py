"""Partner authentication router - вход в партнёрский кабинет."""

from fastapi import APIRouter, Depends
from typing import Annotated
import asyncpg
from datetime import timedelta
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

