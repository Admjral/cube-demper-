"""Authentication router - handles user registration, login, and password management"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import Annotated
import asyncpg
import uuid
from datetime import timedelta

from ..schemas.auth import (
    UserRegister,
    UserLogin,
    Token,
    UserResponse,
    ForgotPasswordRequest,
    ResetPasswordRequest,
)
from ..core.database import get_db_pool
from ..core.security import (
    get_password_hash,
    verify_password,
    create_access_token,
)
from ..core.exceptions import AuthenticationError
from ..dependencies import get_current_user
from ..config import settings

router = APIRouter()


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(
    user_data: UserRegister,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Register a new user"""
    async with pool.acquire() as conn:
        # Check if user already exists
        existing_user = await conn.fetchrow(
            "SELECT id FROM users WHERE email = $1",
            user_data.email
        )

        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User with this email already exists"
            )

        # Hash password
        password_hash = get_password_hash(user_data.password)

        # Create user
        user = await conn.fetchrow(
            """
            INSERT INTO users (email, password_hash, full_name, role)
            VALUES ($1, $2, $3, 'user')
            RETURNING id, email, full_name, role, created_at, updated_at
            """,
            user_data.email,
            password_hash,
            user_data.full_name
        )

        # Create free subscription for new user
        await conn.execute(
            """
            INSERT INTO subscriptions (user_id, plan, status, products_limit, current_period_start, current_period_end)
            VALUES ($1, 'free', 'active', $2, NOW(), NOW() + INTERVAL '1 year')
            """,
            user['id'],
            settings.plan_free_products_limit
        )

        return UserResponse(
            id=str(user['id']),
            email=user['email'],
            full_name=user['full_name'],
            role=user['role'],
            created_at=user['created_at'],
            updated_at=user['updated_at']
        )


@router.post("/login", response_model=Token)
async def login(
    credentials: UserLogin,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Login user and return JWT token"""
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id, email, password_hash, role, is_blocked FROM users WHERE email = $1",
            credentials.email
        )

        if not user:
            raise AuthenticationError("Invalid email or password")

        # Verify password
        if not verify_password(credentials.password, user['password_hash']):
            raise AuthenticationError("Invalid email or password")

        # Check if user is blocked
        if user.get('is_blocked', False):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Your account has been blocked. Please contact support."
            )

        # Create access token
        access_token = create_access_token(
            user_id=user['id'],
            role=user['role'],
            expires_delta=timedelta(hours=settings.access_token_expire_hours)
        )

        return Token(access_token=access_token, token_type="bearer")


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: Annotated[dict, Depends(get_current_user)]
):
    """Get current user information"""
    return UserResponse(
        id=str(current_user['id']),
        email=current_user['email'],
        full_name=current_user['full_name'],
        role=current_user['role'],
        created_at=current_user['created_at'],
        updated_at=current_user['updated_at']
    )


@router.post("/forgot-password", status_code=status.HTTP_200_OK)
async def forgot_password(
    request: ForgotPasswordRequest,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Request password reset (sends email in production)"""
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id FROM users WHERE email = $1",
            request.email
        )

        # Always return success to prevent email enumeration
        # In production, send email with reset token here
        if user:
            # Generate password reset token (valid for 1 hour)
            reset_token = create_access_token(
                user_id=user['id'],
                role='password_reset',
                expires_delta=timedelta(hours=1)
            )
            # TODO: Send email with reset_token
            # For now, just log it (in production, use email service)
            print(f"Password reset token for {request.email}: {reset_token}")

        return {"message": "If the email exists, a password reset link has been sent"}


@router.post("/reset-password", status_code=status.HTTP_200_OK)
async def reset_password(
    request: ResetPasswordRequest,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Reset password using reset token"""
    from ..core.security import decode_access_token

    # Decode reset token
    payload = decode_access_token(request.token)
    if not payload or payload.get('role') != 'password_reset':
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token"
        )

    user_id = payload.get('sub')
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid reset token"
        )

    # Update password
    password_hash = get_password_hash(request.new_password)

    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE users SET password_hash = $1 WHERE id = $2",
            password_hash,
            uuid.UUID(user_id)
        )

        if result == "UPDATE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

    return {"message": "Password has been reset successfully"}
