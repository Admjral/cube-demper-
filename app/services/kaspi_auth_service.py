"""
Kaspi Authentication Service - FastAPI async version

Handles Kaspi merchant authentication with dual-flow support:
1. Email + Password → Direct login (if no SMS required)
2. Email + Password → SMS verification → Complete login

Uses BrowserFarmSharded for managed browser contexts and encrypts
session data before storage in the database.
"""

import asyncio
import logging
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
import json

from playwright.async_api import Page, BrowserContext, Cookie
import httpx

from ..core.browser_farm import BrowserFarmSharded, get_browser_farm
from ..core.security import encrypt_session, decrypt_session
from ..core.database import get_db_pool
from ..core.http_client import get_http_client
from ..core.circuit_breaker import get_kaspi_auth_circuit_breaker, CircuitOpenError

logger = logging.getLogger(__name__)

# Per-merchant login locks to prevent concurrent Playwright logins for the same account
_merchant_login_locks: dict[str, asyncio.Lock] = {}


def _get_merchant_lock(merchant_id: str) -> asyncio.Lock:
    """Get or create an asyncio Lock for a merchant to prevent concurrent logins."""
    if merchant_id not in _merchant_login_locks:
        _merchant_login_locks[merchant_id] = asyncio.Lock()
    return _merchant_login_locks[merchant_id]


class KaspiAuthError(Exception):
    """Base exception for Kaspi authentication errors"""
    pass


class KaspiSMSRequiredError(KaspiAuthError):
    """Raised when SMS verification is required"""
    def __init__(self, message: str, partial_session: dict):
        super().__init__(message)
        self.partial_session = partial_session


class KaspiInvalidCredentialsError(KaspiAuthError):
    """Raised when credentials are invalid"""
    pass


class KaspiSessionExpiredError(KaspiAuthError):
    """Raised when session has expired"""
    pass


async def _login_to_kaspi_page(
    page: Page,
    email: str,
    password: str
) -> Tuple[bool, list, bool]:
    """
    Perform login on Kaspi page.

    Returns:
        Tuple[success, cookies, sms_required]
    """
    try:
        logger.info(f"Starting Kaspi login for {email}")

        # Navigate to login page
        await page.goto("https://idmc.shop.kaspi.kz/login", timeout=30000)
        await page.wait_for_load_state('domcontentloaded')

        # Step 1: Enter email
        logger.debug("Step 1: Entering email")
        await page.wait_for_selector('#user_email_field', timeout=30000)
        await page.fill('#user_email_field', email)
        await page.click('.button.is-primary')

        # Step 2: Wait for email and password fields
        logger.debug("Step 2: Waiting for email and password fields")
        await page.wait_for_selector('#user_email_field', timeout=30000)
        await page.wait_for_selector('#password_field', timeout=30000)

        # Step 3: Enter email and password
        logger.debug("Step 3: Entering credentials")
        await page.fill('#user_email_field', email)
        await page.fill('#password_field', password)
        await page.click('.button.is-primary')

        # Wait a bit for response
        await asyncio.sleep(2)

        # Check for SMS verification requirement
        sms_field = await page.query_selector('#sms_code_field')
        if sms_field:
            logger.info("SMS verification required")
            # Get current cookies (partial session)
            cookies = await page.context.cookies()
            return True, cookies, True

        # Step 4: Wait for navigation bar (successful login)
        logger.debug("Step 4: Waiting for successful login")
        try:
            await page.wait_for_selector('nav.navbar', timeout=10000)
        except Exception:
            # Check for error notification
            error_element = await page.query_selector('.notification.is-danger')
            if error_element:
                error_text = await error_element.text_content()
                logger.error(f"Login error: {error_text}")
                raise KaspiInvalidCredentialsError(f"Invalid credentials: {error_text}")
            raise

        # Check for errors even after navbar appears
        error_element = await page.query_selector('.notification.is-danger')
        if error_element:
            error_text = await error_element.text_content()
            raise KaspiInvalidCredentialsError(f"Login error: {error_text}")

        # Get cookies
        cookies = await page.context.cookies()
        logger.info("Login successful")
        return True, cookies, False

    except KaspiAuthError:
        raise
    except Exception as e:
        logger.error(f"Error during login: {str(e)}")
        raise KaspiAuthError(f"Login failed: {str(e)}")


async def _verify_sms_on_page(
    page: Page,
    sms_code: str,
    cookies: list
) -> Tuple[bool, list]:
    """
    Verify SMS code on Kaspi page.

    Returns:
        Tuple[success, cookies]
    """
    try:
        logger.info("Verifying SMS code")

        # Restore cookies
        await page.context.add_cookies(cookies)

        # Should already be on the page with SMS field
        await page.wait_for_selector('#sms_code_field', timeout=10000)

        # Enter SMS code
        await page.fill('#sms_code_field', sms_code)
        await page.click('.button.is-primary')

        # Wait for navigation bar (successful login)
        await page.wait_for_selector('nav.navbar', timeout=30000)

        # Check for errors
        error_element = await page.query_selector('.notification.is-danger')
        if error_element:
            error_text = await error_element.text_content()
            raise KaspiAuthError(f"SMS verification error: {error_text}")

        # Get updated cookies
        cookies = await page.context.cookies()
        logger.info("SMS verification successful")
        return True, cookies

    except Exception as e:
        logger.error(f"Error during SMS verification: {str(e)}")
        raise KaspiAuthError(f"SMS verification failed: {str(e)}")


def _format_cookies(cookies: list) -> dict:
    """Convert cookies list to dict format"""
    formatted_cookies = {}
    for cookie in cookies:
        if isinstance(cookie, dict):
            formatted_cookies[cookie.get('name', '')] = cookie.get('value', '')
    return formatted_cookies


async def _get_merchant_info(
    cookies: list
) -> Tuple[str, str]:
    """
    Get merchant UID and name from Kaspi API.

    Returns:
        Tuple[merchant_uid, shop_name]
    """
    try:
        cookies_dict = _format_cookies(cookies)

        headers = {
            "x-auth-version": "3",
            "Origin": "https://kaspi.kz",
            "Referer": "https://kaspi.kz/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
        }

        client = await get_http_client()
        breaker = get_kaspi_auth_circuit_breaker()

        # Get merchant list
        logger.debug("Fetching merchant list")
        async with breaker:
            response = await client.get(
                "https://mc.shop.kaspi.kz/s/m",
                headers=headers,
                cookies=cookies_dict
            )
        response.raise_for_status()
        merchants_data = response.json()

        # Extract merchant UID
        merchants = merchants_data.get('merchants', [])
        if not merchants or not isinstance(merchants, list):
            raise KaspiAuthError("No merchants found in response")

        merchant_uid = merchants[0]['uid']
        logger.debug(f"Found merchant UID: {merchant_uid}")

        # Get merchant details
        payload = {
            "operationName": "getMerchant",
            "variables": {"id": merchant_uid},
            "query": """
                query getMerchant($id: String!) {
                    merchant(id: $id) {
                        id
                        name
                        logo {
                            url
                        }
                    }
                }
            """
        }

        async with breaker:
            response = await client.post(
                "https://mc.shop.kaspi.kz/mc/facade/graphql?opName=getMerchant",
                json=payload,
                headers=headers,
                cookies=cookies_dict
            )
        response.raise_for_status()
        shop_data = response.json()

        shop_name = shop_data['data']['merchant']['name']
        logger.info(f"Retrieved merchant: {shop_name} ({merchant_uid})")

        return merchant_uid, shop_name

    except httpx.HTTPError as e:
        logger.error(f"HTTP error getting merchant info: {e}")
        raise KaspiAuthError(f"Failed to get merchant info: {e}")
    except (KeyError, IndexError, TypeError) as e:
        logger.error(f"Error parsing merchant info: {e}")
        raise KaspiAuthError(f"Invalid merchant info response: {e}")


async def authenticate_kaspi(
    email: str,
    password: str,
    merchant_id: Optional[str] = None
) -> dict:
    """
    Authenticate with Kaspi using email and password.

    If SMS verification is required, raises KaspiSMSRequiredError with partial session.
    Otherwise returns complete session data.

    Args:
        email: Kaspi account email
        password: Kaspi account password
        merchant_id: Optional merchant ID for database updates

    Returns:
        dict: Session data with structure:
            {
                'merchant_uid': str,
                'shop_name': str,
                'guid': dict,  # Encrypted session data
                'requires_sms': False
            }

    Raises:
        KaspiSMSRequiredError: If SMS verification is needed
        KaspiInvalidCredentialsError: If credentials are invalid
        KaspiAuthError: For other authentication errors
    """
    browser_farm = await get_browser_farm()

    # Get a browser context from the farm
    shard = browser_farm.shards[0]  # Use first shard for auth
    await shard.initialize()

    context = await shard.get_context()
    page = await context.new_page()

    try:
        # Attempt login
        success, cookies, sms_required = await _login_to_kaspi_page(page, email, password)

        if sms_required:
            # SMS verification required
            partial_session = {
                'cookies': [dict(c) for c in cookies],
                'email': email,
                'password': password,
                'page_url': page.url
            }

            logger.info("SMS verification required - returning partial session")
            raise KaspiSMSRequiredError(
                "SMS verification required",
                partial_session=partial_session
            )

        # Complete login - get merchant info
        merchant_uid, shop_name = await _get_merchant_info(cookies)

        # Build GUID (session data)
        guid = {
            'cookies': [dict(c) for c in cookies],
            'email': email,
            'password': password,
            'merchant_uid': merchant_uid,
            'authenticated_at': datetime.utcnow().isoformat()
        }

        # Encrypt GUID
        encrypted_guid = encrypt_session(guid)

        return {
            'merchant_uid': merchant_uid,
            'shop_name': shop_name,
            'guid': encrypted_guid,
            'requires_sms': False
        }

    finally:
        await page.close()


async def verify_sms_code(
    merchant_id: str,
    sms_code: str,
    partial_session: dict
) -> dict:
    """
    Verify SMS code and complete authentication.

    Args:
        merchant_id: Merchant ID for database updates
        sms_code: SMS verification code
        partial_session: Partial session data from authenticate_kaspi

    Returns:
        dict: Complete session data with structure:
            {
                'merchant_uid': str,
                'shop_name': str,
                'guid': dict  # Encrypted session data
            }

    Raises:
        KaspiAuthError: If verification fails
    """
    browser_farm = await get_browser_farm()

    # Get a browser context from the farm
    shard = browser_farm.shards[0]
    await shard.initialize()

    context = await shard.get_context()
    page = await context.new_page()

    try:
        # Navigate to the page where we left off
        cookies = partial_session.get('cookies', [])
        page_url = partial_session.get('page_url', 'https://idmc.shop.kaspi.kz/login')

        await page.goto(page_url, timeout=30000)

        # Verify SMS code
        success, updated_cookies = await _verify_sms_on_page(page, sms_code, cookies)

        # Get merchant info
        merchant_uid, shop_name = await _get_merchant_info(updated_cookies)

        # Build complete GUID
        guid = {
            'cookies': [dict(c) for c in updated_cookies],
            'email': partial_session.get('email'),
            'password': partial_session.get('password'),
            'merchant_uid': merchant_uid,
            'authenticated_at': datetime.utcnow().isoformat(),
            'sms_verified': True
        }

        # Encrypt GUID
        encrypted_guid = encrypt_session(guid)

        # Update database if merchant_id provided
        if merchant_id:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE kaspi_stores
                    SET guid = $1, name = $2, updated_at = NOW()
                    WHERE merchant_id = $3
                    """,
                    json.dumps({'encrypted': encrypted_guid}),
                    shop_name,
                    merchant_uid
                )
                logger.info(f"Updated store {merchant_uid} with verified session")

        return {
            'merchant_uid': merchant_uid,
            'shop_name': shop_name,
            'guid': encrypted_guid
        }

    finally:
        await page.close()


async def validate_session(guid: dict) -> bool:
    """
    Validate if a Kaspi session is still active.

    Args:
        guid: Session data (can be encrypted string or decrypted dict)

    Returns:
        bool: True if session is valid, False otherwise
    """
    try:
        # Decrypt if needed
        if isinstance(guid, str):
            session_data = decrypt_session(guid)
        elif isinstance(guid, dict):
            # Check if it's the database format with 'encrypted' key
            if 'encrypted' in guid:
                session_data = decrypt_session(guid['encrypted'])
            else:
                session_data = guid
        else:
            logger.warning(f"Invalid guid type: {type(guid)}")
            return False

        cookies = session_data.get('cookies', [])
        if not cookies:
            logger.warning("No cookies in session data")
            return False

        # Test session by making API call
        cookies_dict = _format_cookies(cookies)

        headers = {
            "x-auth-version": "3",
            "Origin": "https://kaspi.kz",
            "Referer": "https://kaspi.kz/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
        }

        client = await get_http_client()
        breaker = get_kaspi_auth_circuit_breaker()

        try:
            async with breaker:
                response = await client.get(
                    "https://mc.shop.kaspi.kz/s/m",
                    headers=headers,
                    cookies=cookies_dict,
                    timeout=10.0  # Override default timeout for validation
                )
        except CircuitOpenError:
            logger.warning("Kaspi auth circuit is open, assuming session invalid")
            return False

        if response.status_code == 200:
            data = response.json()
            merchants = data.get('merchants', [])
            if merchants:
                logger.debug("Session is valid")
                return True

        logger.warning(f"Session validation failed: status={response.status_code}")
        return False

    except Exception as e:
        logger.error(f"Error validating session: {e}")
        return False


async def refresh_session(guid: dict, email: str, password: str) -> dict:
    """
    Refresh expired session by re-authenticating.

    Args:
        guid: Old session data
        email: Kaspi account email
        password: Kaspi account password

    Returns:
        dict: New session data

    Raises:
        KaspiAuthError: If refresh fails
    """
    logger.info("Refreshing expired session")
    return await authenticate_kaspi(email, password)


async def get_active_session(merchant_id: str) -> Optional[dict]:
    """
    Get active session for a merchant from database.

    Args:
        merchant_id: Merchant ID

    Returns:
        Optional[dict]: Decrypted session data or None if not found/invalid
    """
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT guid FROM kaspi_stores
                WHERE merchant_id = $1 AND is_active = true
                """,
                merchant_id
            )

            if not row or not row['guid']:
                return None

            # Validate session
            is_valid = await validate_session(row['guid'])
            if not is_valid:
                logger.warning(f"Session for merchant {merchant_id} is invalid")
                return None

            # Decrypt and return
            if isinstance(row['guid'], dict) and 'encrypted' in row['guid']:
                return decrypt_session(row['guid']['encrypted'])
            elif isinstance(row['guid'], str):
                return decrypt_session(row['guid'])
            else:
                return row['guid']

    except Exception as e:
        logger.error(f"Error getting active session: {e}")
        return None


async def get_active_session_with_refresh(
    merchant_id: str,
    auto_refresh: bool = True,
    skip_validation: bool = False
) -> Optional[dict]:
    """
    Get active session for a merchant, with automatic refresh if expired.

    This function will:
    1. Get the session from database
    2. Validate it (unless skip_validation=True)
    3. If invalid and auto_refresh=True, attempt to re-authenticate using stored credentials
    4. If SMS required, mark store as needing re-auth and return None

    Args:
        merchant_id: Merchant ID
        auto_refresh: Whether to attempt automatic re-authentication
        skip_validation: If True, skip HTTP validation and trust the session from DB.
                        Useful for workers to avoid rate limiting.

    Returns:
        Optional[dict]: Decrypted session data or None if not found/invalid/requires SMS
    """
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            # Try to get credentials from separate columns first
            try:
                row = await conn.fetchrow(
                    """
                    SELECT id, guid, name, kaspi_email, kaspi_password FROM kaspi_stores
                    WHERE merchant_id = $1 AND is_active = true
                    """,
                    merchant_id
                )
            except Exception:
                # Fallback for old schema
                row = await conn.fetchrow(
                    """
                    SELECT id, guid, name FROM kaspi_stores
                    WHERE merchant_id = $1 AND is_active = true
                    """,
                    merchant_id
                )

            if not row or not row['guid']:
                logger.warning(f"No session found for merchant {merchant_id}")
                return None

            # Decrypt session data to get credentials
            session_data = None
            if isinstance(row['guid'], dict) and 'encrypted' in row['guid']:
                session_data = decrypt_session(row['guid']['encrypted'])
            elif isinstance(row['guid'], str):
                session_data = decrypt_session(row['guid'])
            else:
                session_data = row['guid']

            # Validate session (even if decryption failed, we might still refresh)
            # Skip validation if requested (workers use this to avoid rate limiting)
            is_valid = False
            if session_data:
                if skip_validation:
                    # Trust the session from DB without HTTP validation
                    is_valid = True
                    logger.debug(f"Skipping session validation for merchant {merchant_id}")
                else:
                    is_valid = await validate_session(row['guid'])

            if is_valid and session_data:
                return session_data

            # Session expired or decryption failed - attempt refresh if enabled
            if not auto_refresh:
                logger.warning(f"Session for merchant {merchant_id} expired/invalid, auto_refresh disabled")
                return None

            logger.info(f"Session for merchant {merchant_id} expired/invalid, attempting auto-refresh...")

            # Use per-merchant lock to prevent concurrent Playwright logins
            lock = _get_merchant_lock(merchant_id)

            if lock.locked():
                # Another coroutine is already logging in — wait for it, then re-read from DB
                logger.info(f"Login already in progress for merchant {merchant_id}, waiting...")
                async with lock:
                    pass  # just wait for the other login to finish

                # Re-read the refreshed session from DB
                refreshed_row = await conn.fetchrow(
                    "SELECT guid FROM kaspi_stores WHERE merchant_id = $1 AND is_active = true",
                    merchant_id
                )
                if refreshed_row and refreshed_row['guid']:
                    refreshed_data = None
                    if isinstance(refreshed_row['guid'], dict) and 'encrypted' in refreshed_row['guid']:
                        refreshed_data = decrypt_session(refreshed_row['guid']['encrypted'])
                    elif isinstance(refreshed_row['guid'], str):
                        refreshed_data = decrypt_session(refreshed_row['guid'])
                    if refreshed_data:
                        logger.info(f"Got refreshed session for merchant {merchant_id} from concurrent login")
                        return refreshed_data

                logger.warning(f"Concurrent login for merchant {merchant_id} did not produce a valid session")
                return None

            # We're the first — acquire lock and do the actual login
            async with lock:
                # Get stored credentials - try separate columns first, then from session
                email = row.get('kaspi_email') if row else None
                password = row.get('kaspi_password') if row else None

                # Fallback to credentials from session data if available
                if (not email or not password) and session_data:
                    email = email or session_data.get('email')
                    password = password or session_data.get('password')

                if not email or not password:
                    logger.error(f"No credentials stored for merchant {merchant_id}, cannot auto-refresh")
                    try:
                        await conn.execute(
                            """
                            UPDATE kaspi_stores
                            SET needs_reauth = true, reauth_reason = 'credentials_missing', updated_at = NOW()
                            WHERE merchant_id = $1
                            """,
                            merchant_id
                        )
                    except Exception as e:
                        logger.warning(f"Could not update needs_reauth (migration may be pending): {e}")
                    return None

                try:
                    # Attempt to re-authenticate
                    new_session = await authenticate_kaspi(email, password, merchant_id)

                    # If successful, update database
                    await conn.execute(
                        """
                        UPDATE kaspi_stores
                        SET guid = $1, needs_reauth = false, reauth_reason = NULL, updated_at = NOW()
                        WHERE merchant_id = $2
                        """,
                        json.dumps({'encrypted': new_session['guid']}),
                        merchant_id
                    )

                    logger.info(f"Successfully refreshed session for merchant {merchant_id}")

                    # Return decrypted session
                    return decrypt_session(new_session['guid'])

                except KaspiSMSRequiredError as e:
                    # SMS verification needed - mark store and notify
                    logger.warning(f"SMS verification required for merchant {merchant_id}")
                    try:
                        await conn.execute(
                            """
                            UPDATE kaspi_stores
                            SET needs_reauth = true, reauth_reason = 'sms_required', updated_at = NOW()
                            WHERE merchant_id = $1
                            """,
                            merchant_id
                        )
                    except Exception as upd_err:
                        logger.warning(f"Could not update needs_reauth: {upd_err}")
                    return None

                except KaspiInvalidCredentialsError as e:
                    # Credentials no longer valid
                    logger.error(f"Invalid credentials for merchant {merchant_id}")
                    try:
                        await conn.execute(
                            """
                            UPDATE kaspi_stores
                            SET needs_reauth = true, reauth_reason = 'invalid_credentials', updated_at = NOW()
                            WHERE merchant_id = $1
                            """,
                            merchant_id
                        )
                    except Exception as upd_err:
                        logger.warning(f"Could not update needs_reauth: {upd_err}")
                    return None

                except KaspiAuthError as e:
                    # Other auth error
                    logger.error(f"Auth error for merchant {merchant_id}: {e}")
                    try:
                        await conn.execute(
                            """
                            UPDATE kaspi_stores
                            SET needs_reauth = true, reauth_reason = $1, updated_at = NOW()
                            WHERE merchant_id = $2
                            """,
                            str(e)[:200],
                            merchant_id
                        )
                    except Exception as upd_err:
                        logger.warning(f"Could not update needs_reauth: {upd_err}")
                    return None

    except Exception as e:
        logger.error(f"Error getting active session with refresh: {e}")
        return None
