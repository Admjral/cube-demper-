from fastapi import Depends, Header  # pyright: ignore[reportMissingImports]
from typing import Optional
import uuid

from .core.database import get_db_pool
from .core.redis import get_redis
from .core.security import decode_access_token
from .core.exceptions import AuthenticationError, AuthorizationError
import asyncpg  # pyright: ignore[reportMissingImports]
import redis.asyncio as redis  # pyright: ignore[reportMissingImports]


async def get_current_user(
    authorization: Optional[str] = Header(None),
    pool: asyncpg.Pool = Depends(get_db_pool)
) -> dict:
    """Dependency to get current authenticated user"""
    if not authorization or not authorization.startswith("Bearer "):
        raise AuthenticationError("Missing or invalid authorization header")

    token = authorization.replace("Bearer ", "")
    payload = decode_access_token(token)

    if not payload:
        raise AuthenticationError("Invalid or expired token")

    user_id = payload.get("sub")
    if not user_id:
        raise AuthenticationError("Invalid token payload")

    # Parse UUID with error handling
    try:
        user_uuid = uuid.UUID(user_id)
    except (ValueError, TypeError):
        raise AuthenticationError("Invalid user ID format in token")

    # Fetch user from database
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id, email, full_name, role, created_at, updated_at FROM users WHERE id = $1",
            user_uuid
        )

    if not user:
        raise AuthenticationError("User not found")

    return dict(user)


async def get_current_admin_user(
    current_user: dict = Depends(get_current_user)
) -> dict:
    """Dependency to ensure user is admin"""
    if current_user.get("role") != "admin":
        raise AuthorizationError("Admin access required")
    return current_user


async def get_current_partner(
    authorization: Optional[str] = Header(None),
    pool: asyncpg.Pool = Depends(get_db_pool)
) -> dict:
    """Dependency to get current authenticated partner"""
    if not authorization or not authorization.startswith("Bearer "):
        raise AuthenticationError("Missing or invalid authorization header")

    token = authorization.replace("Bearer ", "")
    payload = decode_access_token(token)

    if not payload:
        raise AuthenticationError("Invalid or expired token")

    # Check role is partner
    if payload.get("role") != "partner":
        raise AuthorizationError("Partner access required")

    partner_id = payload.get("sub")
    if not partner_id:
        raise AuthenticationError("Invalid token payload")

    # Parse UUID with error handling
    try:
        partner_uuid = uuid.UUID(partner_id)
    except (ValueError, TypeError):
        raise AuthenticationError("Invalid partner ID format in token")

    # Fetch partner from database
    async with pool.acquire() as conn:
        partner = await conn.fetchrow(
            "SELECT id, email, full_name, created_at, updated_at FROM partners WHERE id = $1",
            partner_uuid
        )

    if not partner:
        raise AuthenticationError("Partner not found")

    return dict(partner)


async def get_db() -> asyncpg.Pool:
    """Dependency alias for database pool"""
    return await get_db_pool()


async def get_redis_client() -> redis.Redis:
    """Dependency alias for Redis client"""
    return await get_redis()
