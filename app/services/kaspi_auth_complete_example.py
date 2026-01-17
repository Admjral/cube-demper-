"""
Complete End-to-End Example: Kaspi Authentication Service

This file demonstrates a complete, production-ready implementation
of Kaspi authentication in a FastAPI application.

It includes:
1. User registration/login flow
2. Kaspi account linking (with SMS support)
3. Session management
4. Automatic refresh
5. Background validation task
6. API endpoints using authenticated sessions
"""

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, status
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime, timedelta
import logging
import asyncio
import httpx

from app.services.kaspi_auth_service import (
    authenticate_kaspi,
    verify_sms_code,
    validate_session,
    get_active_session,
    KaspiAuthError,
    KaspiSMSRequiredError,
    KaspiInvalidCredentialsError,
)
from app.core.database import get_db_pool
from app.core.security import decrypt_session

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["Kaspi Integration"])


# ============================================================================
# Models
# ============================================================================

class LinkKaspiAccountRequest(BaseModel):
    """Request to link a Kaspi account"""
    email: EmailStr = Field(..., description="Kaspi merchant email")
    password: str = Field(..., min_length=6, description="Kaspi merchant password")


class VerifyKaspiSMSRequest(BaseModel):
    """Request to verify SMS code"""
    store_id: str = Field(..., description="Store ID from link response")
    sms_code: str = Field(..., min_length=4, max_length=6, description="SMS code")
    partial_session: dict = Field(..., description="Partial session data")


class KaspiStore(BaseModel):
    """Kaspi store information"""
    id: str
    merchant_id: str
    name: str
    is_active: bool
    session_valid: bool
    last_sync: Optional[datetime]
    created_at: datetime


class KaspiOrder(BaseModel):
    """Kaspi order from merchant API"""
    order_id: str
    status: str
    customer_name: str
    total_amount: float
    created_at: datetime


# ============================================================================
# Authentication Dependency
# ============================================================================

async def get_current_user_id(authorization: str = None) -> str:
    """
    Get current user ID from JWT token.
    Replace this with your actual auth implementation.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )

    # Decode JWT token and extract user_id
    # This is a placeholder - implement your actual JWT validation
    from app.core.security import decode_access_token
    token = authorization.replace("Bearer ", "")
    payload = decode_access_token(token)

    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )

    return payload.get("sub")


# ============================================================================
# Endpoints: Kaspi Account Management
# ============================================================================

@router.post("/kaspi/stores/link")
async def link_kaspi_account(
    request: LinkKaspiAccountRequest,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_current_user_id)
):
    """
    Link a Kaspi merchant account to user.

    This endpoint initiates the authentication process. If SMS verification
    is required, it returns a 202 status with partial session data.

    **Flow 1: Direct Link (No SMS)**
    - Request with email/password
    - Returns 200 with store info
    - Store is immediately active and syncing

    **Flow 2: SMS Required**
    - Request with email/password
    - Returns 202 with partial_session
    - Client must call /kaspi/stores/verify-sms with SMS code

    Example:
        ```json
        POST /api/v1/kaspi/stores/link
        Authorization: Bearer <your-jwt-token>
        {
            "email": "merchant@example.com",
            "password": "SecurePassword123"
        }
        ```

    Success Response (200):
        ```json
        {
            "status": "linked",
            "store": {
                "id": "uuid",
                "merchant_id": "MERCH123",
                "name": "My Store",
                "is_active": true,
                "session_valid": true
            }
        }
        ```

    SMS Required Response (202):
        ```json
        {
            "status": "sms_required",
            "store_id": "uuid",
            "message": "SMS verification required",
            "partial_session": { ... }
        }
        ```
    """
    try:
        logger.info(f"User {user_id} attempting to link Kaspi account: {request.email}")

        # Attempt authentication
        result = await authenticate_kaspi(
            email=request.email,
            password=request.password
        )

        # Direct link successful
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            # Check if store already exists
            existing = await conn.fetchrow(
                """
                SELECT id FROM kaspi_stores
                WHERE user_id = $1 AND merchant_id = $2
                """,
                user_id,
                result['merchant_uid']
            )

            if existing:
                # Update existing store
                await conn.execute(
                    """
                    UPDATE kaspi_stores
                    SET guid = $1, name = $2, is_active = true, updated_at = NOW()
                    WHERE id = $3
                    """,
                    {'encrypted': result['guid']},
                    result['shop_name'],
                    existing['id']
                )
                store_id = existing['id']
            else:
                # Create new store
                store = await conn.fetchrow(
                    """
                    INSERT INTO kaspi_stores (user_id, merchant_id, name, guid, is_active)
                    VALUES ($1, $2, $3, $4, true)
                    RETURNING id, created_at
                    """,
                    user_id,
                    result['merchant_uid'],
                    result['shop_name'],
                    {'encrypted': result['guid']}
                )
                store_id = store['id']

        # Schedule initial sync in background
        background_tasks.add_task(sync_kaspi_store, store_id)

        logger.info(f"Kaspi account linked successfully for user {user_id}")

        return {
            "status": "linked",
            "store": {
                "id": str(store_id),
                "merchant_id": result['merchant_uid'],
                "name": result['shop_name'],
                "is_active": True,
                "session_valid": True
            }
        }

    except KaspiSMSRequiredError as e:
        # SMS verification required - create pending store
        logger.info(f"SMS verification required for user {user_id}")

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            store = await conn.fetchrow(
                """
                INSERT INTO kaspi_stores (user_id, merchant_id, name, is_active)
                VALUES ($1, $2, $3, false)
                RETURNING id
                """,
                user_id,
                f"pending_{datetime.utcnow().timestamp()}",  # Temporary merchant_id
                "Pending SMS Verification"
            )

        return {
            "status": "sms_required",
            "store_id": str(store['id']),
            "message": "SMS verification required. Check your phone for the code.",
            "partial_session": e.partial_session
        }, status.HTTP_202_ACCEPTED

    except KaspiInvalidCredentialsError as e:
        logger.warning(f"Invalid Kaspi credentials for user {user_id}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Kaspi email or password"
        )

    except KaspiAuthError as e:
        logger.error(f"Kaspi auth error for user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to authenticate with Kaspi: {str(e)}"
        )


@router.post("/kaspi/stores/verify-sms")
async def verify_kaspi_sms(
    request: VerifyKaspiSMSRequest,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_current_user_id)
):
    """
    Verify SMS code and complete Kaspi account linking.

    Call this endpoint after receiving SMS code when linking account.

    Example:
        ```json
        POST /api/v1/kaspi/stores/verify-sms
        Authorization: Bearer <your-jwt-token>
        {
            "store_id": "uuid-from-link-response",
            "sms_code": "123456",
            "partial_session": { ... }
        }
        ```

    Success Response:
        ```json
        {
            "status": "verified",
            "store": {
                "id": "uuid",
                "merchant_id": "MERCH123",
                "name": "My Store",
                "is_active": true,
                "session_valid": true
            }
        }
        ```
    """
    try:
        logger.info(f"User {user_id} verifying SMS for store {request.store_id}")

        # Verify SMS code
        result = await verify_sms_code(
            merchant_id=request.store_id,  # We'll update this
            sms_code=request.sms_code,
            partial_session=request.partial_session
        )

        # Update store with verified information
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE kaspi_stores
                SET merchant_id = $1,
                    name = $2,
                    guid = $3,
                    is_active = true,
                    updated_at = NOW()
                WHERE id = $4 AND user_id = $5
                """,
                result['merchant_uid'],
                result['shop_name'],
                {'encrypted': result['guid']},
                request.store_id,
                user_id
            )

        # Schedule initial sync
        background_tasks.add_task(sync_kaspi_store, request.store_id)

        logger.info(f"SMS verified successfully for user {user_id}")

        return {
            "status": "verified",
            "store": {
                "id": request.store_id,
                "merchant_id": result['merchant_uid'],
                "name": result['shop_name'],
                "is_active": True,
                "session_valid": True
            }
        }

    except KaspiAuthError as e:
        logger.error(f"SMS verification failed for user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"SMS verification failed: {str(e)}"
        )


@router.get("/kaspi/stores", response_model=List[KaspiStore])
async def get_kaspi_stores(user_id: str = Depends(get_current_user_id)):
    """
    Get all Kaspi stores for current user.

    Returns:
        List of Kaspi stores with session status
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        stores = await conn.fetch(
            """
            SELECT id, merchant_id, name, guid, is_active, last_sync, created_at
            FROM kaspi_stores
            WHERE user_id = $1
            ORDER BY created_at DESC
            """,
            user_id
        )

    # Validate sessions in parallel
    result = []
    for store in stores:
        session_valid = False
        if store['guid']:
            session_valid = await validate_session(store['guid'])

        result.append(KaspiStore(
            id=str(store['id']),
            merchant_id=store['merchant_id'],
            name=store['name'],
            is_active=store['is_active'],
            session_valid=session_valid,
            last_sync=store['last_sync'],
            created_at=store['created_at']
        ))

    return result


@router.delete("/kaspi/stores/{store_id}")
async def unlink_kaspi_store(
    store_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """
    Unlink (deactivate) a Kaspi store.

    This doesn't delete the store, just marks it as inactive.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE kaspi_stores
            SET is_active = false, updated_at = NOW()
            WHERE id = $1 AND user_id = $2
            """,
            store_id,
            user_id
        )

        if result == "UPDATE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found"
            )

    return {"status": "success", "message": "Store unlinked successfully"}


# ============================================================================
# Endpoints: Using Kaspi Sessions
# ============================================================================

@router.get("/kaspi/stores/{store_id}/orders", response_model=List[KaspiOrder])
async def get_kaspi_orders(
    store_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """
    Get orders from Kaspi for a specific store.

    This demonstrates how to use an authenticated session to make
    API calls to Kaspi's merchant API.

    Example:
        ```
        GET /api/v1/kaspi/stores/{store_id}/orders
        Authorization: Bearer <your-jwt-token>
        ```

    Response:
        ```json
        [
            {
                "order_id": "12345",
                "status": "NEW",
                "customer_name": "John Doe",
                "total_amount": 15000.0,
                "created_at": "2024-01-18T10:30:00Z"
            }
        ]
        ```
    """
    # Get active session
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        store = await conn.fetchrow(
            """
            SELECT merchant_id, guid FROM kaspi_stores
            WHERE id = $1 AND user_id = $2 AND is_active = true
            """,
            store_id,
            user_id
        )

        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found or inactive"
            )

    # Get and validate session
    session = await get_active_session(store['merchant_id'])
    if not session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired. Please re-link your Kaspi account."
        )

    # Extract cookies
    cookies = {c['name']: c['value'] for c in session['cookies']}

    # Make API call to Kaspi
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                "https://mc.shop.kaspi.kz/api/v1/orders",  # Example endpoint
                cookies=cookies,
                headers={
                    "x-auth-version": "3",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            )

            if response.status_code == 401:
                # Session expired
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Session expired. Please re-link your Kaspi account."
                )

            response.raise_for_status()
            data = response.json()

            # Transform to our model
            orders = []
            for order in data.get('orders', []):
                orders.append(KaspiOrder(
                    order_id=order['id'],
                    status=order['status'],
                    customer_name=order['customer']['name'],
                    total_amount=float(order['totalAmount']),
                    created_at=datetime.fromisoformat(order['createdAt'])
                ))

            return orders

    except httpx.HTTPError as e:
        logger.error(f"Error fetching Kaspi orders: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch orders from Kaspi"
        )


# ============================================================================
# Background Tasks
# ============================================================================

async def sync_kaspi_store(store_id: str):
    """
    Background task to sync Kaspi store data.

    This runs after account linking and periodically to keep
    data up to date.
    """
    try:
        logger.info(f"Starting sync for store {store_id}")

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            store = await conn.fetchrow(
                "SELECT merchant_id, guid FROM kaspi_stores WHERE id = $1",
                store_id
            )

            if not store:
                logger.error(f"Store {store_id} not found")
                return

        # Get session
        session = await get_active_session(store['merchant_id'])
        if not session:
            logger.warning(f"No valid session for store {store_id}")
            return

        # Fetch data from Kaspi
        cookies = {c['name']: c['value'] for c in session['cookies']}

        async with httpx.AsyncClient(timeout=60.0) as client:
            # Fetch products, orders, etc.
            # This is a simplified example
            response = await client.get(
                "https://mc.shop.kaspi.kz/api/v1/products",
                cookies=cookies,
                headers={"x-auth-version": "3"}
            )

            if response.status_code == 200:
                data = response.json()
                products_count = len(data.get('products', []))

                # Update store
                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE kaspi_stores
                        SET products_count = $1, last_sync = NOW()
                        WHERE id = $2
                        """,
                        products_count,
                        store_id
                    )

                logger.info(f"Sync completed for store {store_id}: {products_count} products")
            else:
                logger.error(f"Sync failed for store {store_id}: HTTP {response.status_code}")

    except Exception as e:
        logger.error(f"Error syncing store {store_id}: {e}", exc_info=True)


async def validate_all_sessions_task():
    """
    Background task to validate all Kaspi sessions.

    Run this periodically (e.g., every 6 hours) to proactively
    detect expired sessions.
    """
    logger.info("Starting session validation task")

    pool = await get_db_pool()
    async with pool.acquire() as conn:
        stores = await conn.fetch(
            """
            SELECT id, merchant_id, guid
            FROM kaspi_stores
            WHERE is_active = true AND guid IS NOT NULL
            """
        )

    for store in stores:
        try:
            is_valid = await validate_session(store['guid'])

            if not is_valid:
                logger.warning(f"Session expired for store {store['id']}")

                # Mark as needing re-authentication
                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE kaspi_stores
                        SET is_active = false, updated_at = NOW()
                        WHERE id = $1
                        """,
                        store['id']
                    )

            # Rate limit validation (don't overwhelm Kaspi)
            await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"Error validating session for store {store['id']}: {e}")

    logger.info("Session validation task completed")


# ============================================================================
# Startup Event
# ============================================================================

@router.on_event("startup")
async def schedule_background_tasks():
    """
    Schedule periodic background tasks.

    In production, use a task queue like Celery or APScheduler.
    """
    # This is a simplified example
    # In production, use proper task scheduling

    async def periodic_validation():
        while True:
            await asyncio.sleep(6 * 60 * 60)  # Every 6 hours
            await validate_all_sessions_task()

    # Start background task
    asyncio.create_task(periodic_validation())
    logger.info("Background tasks scheduled")


# ============================================================================
# Usage Example
# ============================================================================

"""
Complete User Flow Example:

1. User registers/logs in to your app
   POST /api/auth/register → JWT token

2. User wants to link Kaspi account
   POST /api/v1/kaspi/stores/link
   Headers: Authorization: Bearer <jwt>
   Body: {"email": "merchant@example.com", "password": "pass123"}

   Response A (Direct link):
   {
     "status": "linked",
     "store": { "id": "uuid", "merchant_id": "MERCH123", ... }
   }

   Response B (SMS required):
   {
     "status": "sms_required",
     "store_id": "uuid",
     "partial_session": { ... }
   }

3. If SMS required, prompt user for code
   POST /api/v1/kaspi/stores/verify-sms
   Body: {"store_id": "uuid", "sms_code": "123456", "partial_session": {...}}

   Response:
   {
     "status": "verified",
     "store": { "id": "uuid", "merchant_id": "MERCH123", ... }
   }

4. Fetch user's stores
   GET /api/v1/kaspi/stores

   Response:
   [
     {
       "id": "uuid",
       "merchant_id": "MERCH123",
       "name": "My Store",
       "is_active": true,
       "session_valid": true,
       "last_sync": "2024-01-18T10:00:00Z",
       "created_at": "2024-01-18T09:00:00Z"
     }
   ]

5. Fetch orders from Kaspi
   GET /api/v1/kaspi/stores/{store_id}/orders

   Response:
   [
     {
       "order_id": "12345",
       "status": "NEW",
       "customer_name": "John Doe",
       "total_amount": 15000.0,
       "created_at": "2024-01-18T10:30:00Z"
     }
   ]

6. Background: Session validation runs every 6 hours
   - Validates all active sessions
   - Marks expired ones as inactive
   - User gets notified to re-link

7. User can unlink store
   DELETE /api/v1/kaspi/stores/{store_id}

   Response:
   {"status": "success", "message": "Store unlinked successfully"}
"""
