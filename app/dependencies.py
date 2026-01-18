from fastapi import Depends, Header
from typing import Optional
import uuid

from .core.database import get_db_pool
from .core.redis import get_redis
from .core.security import decode_access_token
from .core.exceptions import AuthenticationError, AuthorizationError
import asyncpg
import redis.asyncio as redis


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


async def get_db() -> asyncpg.Pool:
    """Dependency alias for database pool"""
    return await get_db_pool()


async def get_redis_client() -> redis.Redis:
    """Dependency alias for Redis client"""
    return await get_redis()
