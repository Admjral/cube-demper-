"""Authentication router - handles user registration, login, password management, and phone OTP verification"""

from fastapi import APIRouter, Depends, HTTPException, Request, status
from typing import Annotated
from collections import defaultdict
from time import time
import asyncpg
import uuid
import random
import logging
from datetime import timedelta, datetime

from ..schemas.auth import (
    UserRegister,
    UserLogin,
    Token,
    UserResponse,
    VerifyOtpRequest,
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

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory rate limiting for auth endpoints
_login_attempts: dict[str, list[float]] = defaultdict(list)
_register_attempts: dict[str, list[float]] = defaultdict(list)


def _check_rate_limit(store: dict, key: str, max_attempts: int, window: int):
    """Check IP-based rate limit. Raises 429 if exceeded."""
    now = time()
    store[key] = [t for t in store[key] if now - t < window]
    if len(store[key]) >= max_attempts:
        raise HTTPException(status_code=429, detail="Too many attempts. Please try again later.")
    store[key].append(now)


async def _send_otp(conn, user_id: uuid.UUID, phone: str):
    """Generate and send OTP code via WhatsApp. Returns the verification ID."""
    code = f"{random.randint(0, 999999):06d}"
    expires_at = datetime.utcnow() + timedelta(minutes=5)

    row = await conn.fetchrow(
        """
        INSERT INTO phone_verifications (user_id, phone, code, expires_at)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        user_id, phone, code, expires_at
    )

    # Send via WAHA
    try:
        from ..services.waha_service import get_waha_service
        waha = get_waha_service()
        await waha.send_text(phone, f"Cube Demper: Ваш код подтверждения: {code}", session=settings.waha_otp_session)
        logger.info(f"OTP sent to {phone[:4]}***{phone[-2:]} for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to send OTP via WhatsApp to {phone}: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to send verification code via WhatsApp. Please try again."
        )

    return row['id']


@router.post("/register", response_model=Token, status_code=status.HTTP_201_CREATED)
async def register(
    request: Request,
    user_data: UserRegister,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Register a new user and return JWT token"""
    _check_rate_limit(_register_attempts, request.client.host, max_attempts=3, window=60)
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

        # Create user with phone
        user = await conn.fetchrow(
            """
            INSERT INTO users (email, password_hash, full_name, phone, phone_verified, role)
            VALUES ($1, $2, $3, $4, FALSE, 'user')
            RETURNING id, email, full_name, role, created_at, updated_at
            """,
            user_data.email,
            password_hash,
            user_data.full_name,
            user_data.phone
        )

        # Create free subscription for new user (with plan_id pointing to free plan)
        free_plan_id = await conn.fetchval(
            "SELECT id FROM plans WHERE code = 'free' AND is_active = true"
        )
        await conn.execute(
            """
            INSERT INTO subscriptions (user_id, plan, plan_id, status, products_limit,
                                       analytics_limit, demping_limit,
                                       current_period_start, current_period_end)
            VALUES ($1, 'free', $2, 'active', $3, 0, 0, NOW(), NOW() + INTERVAL '1 year')
            """,
            user['id'],
            free_plan_id,
            settings.plan_free_products_limit
        )

        # Send OTP
        await _send_otp(conn, user['id'], user_data.phone)

        # Return JWT token (auto-login)
        access_token = create_access_token(
            user_id=user['id'],
            role=user['role'],
            expires_delta=timedelta(hours=settings.access_token_expire_hours)
        )

        return Token(access_token=access_token, token_type="bearer")


@router.post("/login", response_model=Token)
async def login(
    request: Request,
    credentials: UserLogin,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Login user and return JWT token"""
    _check_rate_limit(_login_attempts, request.client.host, max_attempts=5, window=60)
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
        phone=current_user.get('phone'),
        phone_verified=current_user.get('phone_verified', False),
        role=current_user['role'],
        created_at=current_user['created_at'],
        updated_at=current_user['updated_at']
    )


@router.post("/send-otp")
async def send_otp(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Send or resend OTP code via WhatsApp"""
    user_id = current_user['id']
    phone = current_user.get('phone')

    if not phone:
        raise HTTPException(status_code=400, detail="No phone number on account")

    if current_user.get('phone_verified', False):
        raise HTTPException(status_code=400, detail="Phone already verified")

    async with pool.acquire() as conn:
        # Rate limit: 1 OTP per 60 seconds
        recent = await conn.fetchval(
            """
            SELECT id FROM phone_verifications
            WHERE user_id = $1 AND created_at > NOW() - INTERVAL '60 seconds'
            ORDER BY created_at DESC LIMIT 1
            """,
            user_id
        )
        if recent:
            raise HTTPException(status_code=429, detail="Please wait 60 seconds before requesting a new code")

        await _send_otp(conn, user_id, phone)

    return {"status": "sent"}


@router.post("/verify-otp")
async def verify_otp(
    request: VerifyOtpRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Verify OTP code and mark phone as verified"""
    user_id = current_user['id']

    async with pool.acquire() as conn:
        # Get the latest unverified OTP
        verification = await conn.fetchrow(
            """
            SELECT id, code, attempts, expires_at FROM phone_verifications
            WHERE user_id = $1 AND verified_at IS NULL
            ORDER BY created_at DESC LIMIT 1
            """,
            user_id
        )

        if not verification:
            raise HTTPException(status_code=400, detail="No pending verification. Request a new code.")

        # Check expiry
        if datetime.utcnow() > verification['expires_at']:
            raise HTTPException(status_code=400, detail="Code expired. Request a new code.")

        # Check attempts
        if verification['attempts'] >= 5:
            raise HTTPException(status_code=400, detail="Too many attempts. Request a new code.")

        # Increment attempts
        await conn.execute(
            "UPDATE phone_verifications SET attempts = attempts + 1 WHERE id = $1",
            verification['id']
        )

        # Verify code
        if verification['code'] != request.code:
            remaining = 4 - verification['attempts']  # already incremented above conceptually
            raise HTTPException(status_code=400, detail=f"Invalid code. {max(remaining, 0)} attempts remaining.")

        # Mark verified
        await conn.execute(
            "UPDATE phone_verifications SET verified_at = NOW() WHERE id = $1",
            verification['id']
        )
        await conn.execute(
            "UPDATE users SET phone_verified = TRUE WHERE id = $1",
            user_id
        )

    return {"status": "verified"}


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
            # Token generated but not logged for security
            logger.info(f"Password reset requested for user_id={user['id']}")

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
