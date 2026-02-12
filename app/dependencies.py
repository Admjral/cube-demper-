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
            "SELECT id, email, full_name, phone, phone_verified, company_name, bin, tax_type, role, created_at, updated_at FROM users WHERE id = $1",
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


# Feature access dependencies
from fastapi import HTTPException, status
from .services.feature_access import get_feature_access_service


async def get_user_with_features(
    current_user: dict = Depends(get_current_user),
    pool: asyncpg.Pool = Depends(get_db_pool)
) -> dict:
    """Get current user with their feature access info attached."""
    service = get_feature_access_service()
    features = await service.get_user_features(pool, current_user['id'])
    return {**current_user, 'subscription': features}


def require_feature(feature: str):
    """
    Dependency factory for feature-gated endpoints.

    Usage:
        @router.get("/preorders")
        async def list_preorders(
            current_user: Annotated[dict, require_feature("preorder")],
            ...
        ):
    """
    async def dependency(
        current_user: dict = Depends(get_current_user),
        pool: asyncpg.Pool = Depends(get_db_pool)
    ) -> dict:
        service = get_feature_access_service()
        has_access, message = await service.check_feature_access(
            pool, current_user['id'], feature
        )
        if not has_access:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "error": "feature_not_available",
                    "feature": feature,
                    "message": message
                }
            )
        return current_user
    return Depends(dependency)
