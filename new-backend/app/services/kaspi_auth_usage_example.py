"""
Kaspi Authentication Service - FastAPI Integration Examples

This file demonstrates how to use the kaspi_auth_service in FastAPI endpoints.
It shows both authentication flows:
1. Direct login (no SMS)
2. Login with SMS verification (2-step process)
"""

from fastapi import APIRouter, HTTPException, Depends, Body
from pydantic import BaseModel, Field
from typing import Optional
import logging

from .kaspi_auth_service import (
    authenticate_kaspi,
    verify_sms_code,
    validate_session,
    get_active_session,
    KaspiAuthError,
    KaspiSMSRequiredError,
    KaspiInvalidCredentialsError,
)
from ..core.database import get_db_pool

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/kaspi/auth", tags=["Kaspi Authentication"])


# ============================================================================
# Request/Response Models
# ============================================================================

class KaspiAuthRequest(BaseModel):
    """Request for Kaspi authentication"""
    email: str = Field(..., description="Kaspi account email")
    password: str = Field(..., description="Kaspi account password")
    merchant_id: Optional[str] = Field(None, description="Optional merchant ID for updates")


class KaspiSMSVerifyRequest(BaseModel):
    """Request for SMS verification"""
    merchant_id: str = Field(..., description="Merchant ID")
    sms_code: str = Field(..., min_length=4, max_length=6, description="SMS verification code")
    partial_session: dict = Field(..., description="Partial session from initial auth")


class KaspiAuthResponse(BaseModel):
    """Response for successful authentication"""
    success: bool
    merchant_uid: str
    shop_name: str
    requires_sms: bool = False
    message: str


class KaspiSMSRequiredResponse(BaseModel):
    """Response when SMS verification is required"""
    success: bool = False
    requires_sms: bool = True
    message: str
    partial_session: dict
    merchant_id: Optional[str] = None


class KaspiSessionStatusResponse(BaseModel):
    """Response for session status check"""
    is_valid: bool
    merchant_uid: Optional[str] = None
    shop_name: Optional[str] = None
    message: str


# ============================================================================
# Endpoints - Flow 1: Direct Login (No SMS)
# ============================================================================

@router.post("/login", response_model=KaspiAuthResponse)
async def kaspi_login(
    request: KaspiAuthRequest
):
    """
    Authenticate with Kaspi.

    This endpoint initiates authentication. It can result in:
    1. Immediate success (no SMS) → Returns KaspiAuthResponse
    2. SMS required → Returns KaspiSMSRequiredResponse with partial_session

    Example Direct Login (no SMS):
        POST /api/kaspi/auth/login
        {
            "email": "merchant@example.com",
            "password": "SecurePass123"
        }

        Response 200:
        {
            "success": true,
            "merchant_uid": "MERCH123",
            "shop_name": "My Store",
            "requires_sms": false,
            "message": "Authentication successful"
        }

    Example SMS Required:
        POST /api/kaspi/auth/login
        {
            "email": "merchant@example.com",
            "password": "SecurePass123"
        }

        Response 202:
        {
            "success": false,
            "requires_sms": true,
            "message": "SMS verification required",
            "partial_session": { ... },
            "merchant_id": null
        }
    """
    try:
        logger.info(f"Authentication attempt for {request.email}")

        # Attempt authentication
        result = await authenticate_kaspi(
            email=request.email,
            password=request.password,
            merchant_id=request.merchant_id
        )

        # Direct login successful
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            # Store session in database
            await conn.execute(
                """
                UPDATE kaspi_stores
                SET guid = $1, name = $2, updated_at = NOW()
                WHERE merchant_id = $3
                """,
                {'encrypted': result['guid']},
                result['shop_name'],
                result['merchant_uid']
            )

        logger.info(f"Authentication successful for {request.email}")
        return KaspiAuthResponse(
            success=True,
            merchant_uid=result['merchant_uid'],
            shop_name=result['shop_name'],
            requires_sms=False,
            message="Authentication successful"
        )

    except KaspiSMSRequiredError as e:
        # SMS verification required - return partial session
        logger.info(f"SMS verification required for {request.email}")
        return KaspiSMSRequiredResponse(
            success=False,
            requires_sms=True,
            message="SMS verification required. Please check your phone.",
            partial_session=e.partial_session,
            merchant_id=request.merchant_id
        )

    except KaspiInvalidCredentialsError as e:
        logger.warning(f"Invalid credentials for {request.email}")
        raise HTTPException(status_code=401, detail=str(e))

    except KaspiAuthError as e:
        logger.error(f"Authentication error for {request.email}: {e}")
        raise HTTPException(status_code=500, detail=f"Authentication failed: {str(e)}")

    except Exception as e:
        logger.error(f"Unexpected error during authentication: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Endpoints - Flow 2: SMS Verification
# ============================================================================

@router.post("/verify-sms", response_model=KaspiAuthResponse)
async def kaspi_verify_sms(
    request: KaspiSMSVerifyRequest
):
    """
    Verify SMS code and complete authentication.

    This endpoint completes the authentication process when SMS verification
    is required. Use the partial_session from the initial login response.

    Example:
        POST /api/kaspi/auth/verify-sms
        {
            "merchant_id": "MERCH123",
            "sms_code": "123456",
            "partial_session": {
                "cookies": [...],
                "email": "merchant@example.com",
                "password": "...",
                "page_url": "..."
            }
        }

        Response 200:
        {
            "success": true,
            "merchant_uid": "MERCH123",
            "shop_name": "My Store",
            "requires_sms": false,
            "message": "SMS verification successful"
        }
    """
    try:
        logger.info(f"SMS verification for merchant {request.merchant_id}")

        # Verify SMS code
        result = await verify_sms_code(
            merchant_id=request.merchant_id,
            sms_code=request.sms_code,
            partial_session=request.partial_session
        )

        logger.info(f"SMS verification successful for merchant {request.merchant_id}")
        return KaspiAuthResponse(
            success=True,
            merchant_uid=result['merchant_uid'],
            shop_name=result['shop_name'],
            requires_sms=False,
            message="SMS verification successful"
        )

    except KaspiAuthError as e:
        logger.error(f"SMS verification error: {e}")
        raise HTTPException(status_code=400, detail=f"SMS verification failed: {str(e)}")

    except Exception as e:
        logger.error(f"Unexpected error during SMS verification: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Endpoints - Session Management
# ============================================================================

@router.get("/session/{merchant_id}", response_model=KaspiSessionStatusResponse)
async def check_kaspi_session(merchant_id: str):
    """
    Check if merchant has a valid Kaspi session.

    Example:
        GET /api/kaspi/auth/session/MERCH123

        Response 200:
        {
            "is_valid": true,
            "merchant_uid": "MERCH123",
            "shop_name": "My Store",
            "message": "Session is active"
        }
    """
    try:
        logger.info(f"Checking session for merchant {merchant_id}")

        # Get session from database
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT merchant_id, name, guid
                FROM kaspi_stores
                WHERE merchant_id = $1 AND is_active = true
                """,
                merchant_id
            )

            if not row or not row['guid']:
                return KaspiSessionStatusResponse(
                    is_valid=False,
                    message="No session found"
                )

            # Validate session
            is_valid = await validate_session(row['guid'])

            if is_valid:
                return KaspiSessionStatusResponse(
                    is_valid=True,
                    merchant_uid=row['merchant_id'],
                    shop_name=row['name'],
                    message="Session is active"
                )
            else:
                return KaspiSessionStatusResponse(
                    is_valid=False,
                    merchant_uid=row['merchant_id'],
                    shop_name=row['name'],
                    message="Session has expired"
                )

    except Exception as e:
        logger.error(f"Error checking session: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/session/{merchant_id}/refresh", response_model=KaspiAuthResponse)
async def refresh_kaspi_session(
    merchant_id: str,
    credentials: KaspiAuthRequest = Body(...)
):
    """
    Refresh expired session by re-authenticating.

    Example:
        POST /api/kaspi/auth/session/MERCH123/refresh
        {
            "email": "merchant@example.com",
            "password": "SecurePass123"
        }

        Response 200:
        {
            "success": true,
            "merchant_uid": "MERCH123",
            "shop_name": "My Store",
            "requires_sms": false,
            "message": "Session refreshed successfully"
        }
    """
    try:
        logger.info(f"Refreshing session for merchant {merchant_id}")

        # Re-authenticate
        result = await authenticate_kaspi(
            email=credentials.email,
            password=credentials.password,
            merchant_id=merchant_id
        )

        # Update database
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE kaspi_stores
                SET guid = $1, updated_at = NOW()
                WHERE merchant_id = $2
                """,
                {'encrypted': result['guid']},
                result['merchant_uid']
            )

        logger.info(f"Session refreshed for merchant {merchant_id}")
        return KaspiAuthResponse(
            success=True,
            merchant_uid=result['merchant_uid'],
            shop_name=result['shop_name'],
            requires_sms=False,
            message="Session refreshed successfully"
        )

    except KaspiSMSRequiredError as e:
        # If SMS required, client needs to use verify-sms endpoint
        logger.info(f"SMS required for session refresh of {merchant_id}")
        raise HTTPException(
            status_code=202,
            detail={
                "message": "SMS verification required",
                "partial_session": e.partial_session
            }
        )

    except Exception as e:
        logger.error(f"Error refreshing session: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Usage in Other Services
# ============================================================================

async def example_usage_in_sync_service():
    """
    Example of using kaspi_auth_service in a sync/parsing service.

    This demonstrates how to get an active session and use it for API calls.
    """
    merchant_id = "MERCH123"

    # Get active session
    session = await get_active_session(merchant_id)

    if not session:
        logger.error(f"No valid session for merchant {merchant_id}")
        # Re-authentication needed
        return None

    # Use session cookies for API calls
    cookies = session.get('cookies', [])
    cookies_dict = {c['name']: c['value'] for c in cookies}

    # Make authenticated request to Kaspi API
    import httpx
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://mc.shop.kaspi.kz/s/m",
            cookies=cookies_dict,
            headers={
                "x-auth-version": "3",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }
        )

        if response.status_code == 401:
            # Session expired - needs refresh
            logger.warning(f"Session expired for merchant {merchant_id}")
            return None

        return response.json()


# ============================================================================
# Client-Side Integration Example (JavaScript/TypeScript)
# ============================================================================

CLIENT_INTEGRATION_EXAMPLE = """
// TypeScript/JavaScript client example

class KaspiAuthClient {
  private baseUrl = '/api/kaspi/auth';

  // Flow 1: Direct login (no SMS)
  async login(email: string, password: string) {
    const response = await fetch(`${this.baseUrl}/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password })
    });

    const data = await response.json();

    if (data.requires_sms) {
      // SMS verification required
      return {
        success: false,
        requiresSMS: true,
        partialSession: data.partial_session
      };
    }

    // Direct login success
    return {
      success: true,
      merchantUid: data.merchant_uid,
      shopName: data.shop_name
    };
  }

  // Flow 2: SMS verification
  async verifySMS(merchantId: string, smsCode: string, partialSession: any) {
    const response = await fetch(`${this.baseUrl}/verify-sms`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        merchant_id: merchantId,
        sms_code: smsCode,
        partial_session: partialSession
      })
    });

    if (!response.ok) {
      throw new Error('SMS verification failed');
    }

    return await response.json();
  }

  // Check session status
  async checkSession(merchantId: string) {
    const response = await fetch(`${this.baseUrl}/session/${merchantId}`);
    return await response.json();
  }

  // Complete authentication flow with SMS handling
  async authenticateWithSMSSupport(
    email: string,
    password: string,
    onSMSRequired: (partialSession: any) => Promise<string>
  ) {
    // Step 1: Initial login
    const loginResult = await this.login(email, password);

    if (!loginResult.requiresSMS) {
      // Direct login success
      return loginResult;
    }

    // Step 2: SMS verification required
    const smsCode = await onSMSRequired(loginResult.partialSession);

    // Step 3: Verify SMS
    const verifyResult = await this.verifySMS(
      'merchant_id', // You'd get this from context
      smsCode,
      loginResult.partialSession
    );

    return verifyResult;
  }
}

// Usage example
const authClient = new KaspiAuthClient();

// With SMS handling
await authClient.authenticateWithSMSSupport(
  'merchant@example.com',
  'password123',
  async (partialSession) => {
    // Show SMS input dialog to user
    const smsCode = await showSMSDialog();
    return smsCode;
  }
);
"""
