"""
Kaspi API Parser Service - FastAPI async version

Provides async functions for interacting with Kaspi merchant API:
- Fetching product lists
- Parsing products by SKU
- Syncing product prices
- Getting competitor prices
- Fetching preorders

All functions use async patterns with httpx.AsyncClient and BrowserFarmSharded.
"""

import asyncio
import json
import logging
import random
import re
import uuid as uuid_module
from datetime import datetime
from typing import Optional, Dict, Any, List, Union
from decimal import Decimal
from uuid import UUID

import httpx  # pyright: ignore[reportMissingImports]
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from ..config import settings
from ..core.browser_farm import get_browser_farm
from ..core.rate_limiter import (
    get_global_rate_limiter,
    get_offers_rate_limiter,
    get_pricefeed_rate_limiter,
    offers_ban_pause,
    wait_for_offers_ban,
    is_merchant_cooled_down,
    mark_pricefeed_cooldown,
)
from ..core.database import get_db_pool
from ..core.http_client import get_http_client
from ..core.circuit_breaker import get_kaspi_circuit_breaker, CircuitOpenError
from ..core.proxy_rotator import get_user_proxy_rotator, NoProxiesAllocatedError, NoProxiesAvailableError
from .kaspi_auth_service import get_active_session, validate_session, KaspiAuthError

logger = logging.getLogger(__name__)


# ============================================================================
# Session Helper Functions
# ============================================================================

def _format_cookies(cookies: list) -> dict:
    """Convert cookies list to dictionary format for requests"""
    formatted_cookies = {}
    for cookie in cookies:
        if isinstance(cookie, dict):
            formatted_cookies[cookie.get('name', '')] = cookie.get('value', '')
        else:
            logger.warning(f"Invalid cookie format: {cookie}")
    return formatted_cookies


def _get_cookies_from_session(session: dict) -> dict:
    """Extract and format cookies from session data"""
    if not session:
        return {}

    cookies = session.get('cookies', [])
    if isinstance(cookies, list):
        return _format_cookies(cookies)
    elif isinstance(cookies, dict):
        return cookies

    return {}


def _get_merchant_uid_from_session(session: dict) -> Optional[str]:
    """Extract merchant UID from session data"""
    return session.get('merchant_uid')


# ============================================================================
# User Agent and Header Utilities
# ============================================================================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
]

ACCEPT_ENCODINGS = [
    "gzip, deflate, br",
    "gzip, deflate, br, zstd",
]

ACCEPT_LANGUAGES = [
    "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "ru-RU,ru;q=0.8,en-US;q=0.7,en;q=0.6",
]

X_KS_CITY = [
    "750000000",  # Almaty
    "770000000",  # Astana
    "730000000",  # Shymkent
]

# Default city for API requests
DEFAULT_CITY_ID = "750000000"  # Almaty


def _get_random_headers(sku: Optional[str] = None, city_id: Optional[str] = None) -> dict:
    """Generate random headers to avoid detection
    
    Args:
        sku: Optional SKU for referer header
        city_id: City ID for x-ks-city header (defaults to Almaty)
    """
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": random.choice(ACCEPT_ENCODINGS),
        "accept-language": random.choice(ACCEPT_LANGUAGES),
        "cache-control": random.choice(["no-cache", "max-age=0"]),
        "connection": "keep-alive",
        "content-type": "application/json",
        "pragma": random.choice(["no-cache", ""]),
        "x-requested-with": "XMLHttpRequest",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "user-agent": random.choice(USER_AGENTS),
        "x-ks-city": city_id or DEFAULT_CITY_ID,
    }

    if sku:
        headers["referer"] = f"https://kaspi.kz/shop/p/{sku}/?c=710000000"
    else:
        headers["referer"] = "https://kaspi.kz/"

    return headers


def _get_merchant_headers() -> dict:
    """Get standard headers for merchant API requests"""
    return {
        "x-auth-version": "3",
        "Origin": "https://kaspi.kz",
        "Referer": "https://kaspi.kz/",
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": random.choice(ACCEPT_ENCODINGS),
        "Accept-Language": random.choice(ACCEPT_LANGUAGES),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


# ============================================================================
# Product Mapping Functions
# ============================================================================

def _map_offer(raw_offer: dict) -> dict:
    """Map raw Kaspi API offer to internal product format"""
    # Extract product ID from URL
    product_url = raw_offer.get("shopLink", "")
    match = re.search(r'/p\/.*-(\d+)/', product_url)
    external_kaspi_id = match.group(1) if match else None

    # Parse availabilities
    availabilities = raw_offer.get("availabilities", [])
    availability_info = {}

    for availability in availabilities:
        store_id = availability.get("storeId", "")
        if store_id:
            pp_match = re.search(r'PP(\d+)', store_id)
            if pp_match:
                pp_number = pp_match.group(1)
                pp_key = f"PP{pp_number}"

                available = availability.get("available", "no")
                stock_count = availability.get("stockCount")
                if stock_count is not None:
                    stock_count = int(stock_count)

                pre_order = availability.get("preOrder", 0)
                if pre_order is not None:
                    pre_order = int(pre_order)

                availability_info[pp_key] = {
                    "available": available,
                    "stock_count": stock_count,
                    "pre_order": pre_order
                }

    # Use offerId if available, otherwise fall back to sku
    kaspi_product_id = raw_offer.get("offerId") or raw_offer.get("sku")

    return {
        "kaspi_product_id": kaspi_product_id,
        "kaspi_sku": raw_offer.get("sku"),
        "name": raw_offer.get("masterTitle"),
        "brand": raw_offer.get("brand"),
        "category": raw_offer.get("masterCategory"),
        "price": raw_offer.get("minPrice", 0),
        "image_url": (
            f"https://resources.cdn-kaspi.kz/img/m/p/{raw_offer.get('images', [])[0]}"
            if raw_offer.get('images')
            else None
        ),
        "external_kaspi_id": external_kaspi_id,
        "availabilities": availability_info,
        "updated_at": raw_offer.get("updatedAt")
    }


# ============================================================================
# Main API Functions
# ============================================================================

async def get_products(
    merchant_id: str,
    session: dict,
    page_size: int = 100,
    max_retries: int = 3,
    user_id: Optional[UUID] = None,
    use_proxy: bool = False
) -> List[dict]:
    """
    Fetch all products for a merchant using pagination.

    Args:
        merchant_id: Merchant UID
        session: Session data with cookies
        page_size: Products per page (max 100)
        max_retries: Maximum retry attempts on rate limit
        user_id: User UUID (required if use_proxy=True)
        use_proxy: Whether to use user's proxy pool (module='catalog')

    Returns:
        List of products in internal format

    Raises:
        KaspiAuthError: If session is invalid
        httpx.HTTPError: If API request fails
    """
    cookies = _get_cookies_from_session(session)
    if not cookies:
        raise KaspiAuthError("No cookies found in session")

    merchant_uid = _get_merchant_uid_from_session(session) or merchant_id
    headers = _get_merchant_headers()

    all_offers = []
    page = 0
    rate_limiter = get_global_rate_limiter()

    # Get proxy rotator if using proxies
    rotator = None
    proxy_url = None
    if use_proxy:
        if not user_id:
            raise ValueError("user_id required when use_proxy=True")

        try:
            rotator = await get_user_proxy_rotator(user_id, module='catalog')
            proxy = await rotator.get_current_proxy()
            proxy_url = f"{proxy.protocol}://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
            logger.debug(f"Using proxy {proxy.id} for catalog sync (merchant {merchant_id})")
        except (NoProxiesAllocatedError, NoProxiesAvailableError) as e:
            logger.warning(f"No proxies available for user {user_id}, module 'catalog': {e}")
            use_proxy = False

    # Create HTTP client with or without proxy
    if use_proxy and proxy_url:
        client = httpx.AsyncClient(
            proxies={"http://": proxy_url, "https://": proxy_url},
            timeout=httpx.Timeout(30.0),
        )
    else:
        client = await get_http_client()

    breaker = get_kaspi_circuit_breaker()

    while True:
        url = (
            f"https://mc.shop.kaspi.kz/bff/offer-view/list"
            f"?m={merchant_uid}&p={page}&l={page_size}&a=true"
        )

        retries = 0
        while retries < max_retries:
            try:
                # Acquire rate limit token (skip if using proxy)
                if not use_proxy:
                    await rate_limiter.acquire()

                # Use circuit breaker to prevent cascading failures
                async with breaker:
                    response = await client.get(
                        url,
                        headers=headers,
                        cookies=cookies
                    )

                if response.status_code == 401:
                    if rotator:
                        await rotator.record_request(success=False)
                    raise KaspiAuthError("Authentication failed - session expired")

                if response.status_code == 429:
                    # Rate limited - wait and retry
                    if rotator:
                        await rotator.record_request(success=False)
                    wait_time = random.uniform(0.5, 2.0)
                    logger.warning(f"Rate limited, waiting {wait_time:.2f}s (retry {retries + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    retries += 1
                    continue

                response.raise_for_status()

                data = response.json()
                offers = data.get('data', [])

                # ✅ Record successful request with proxy
                if rotator:
                    await rotator.record_request(success=True)

                if not offers:
                    # No more products
                    logger.info(f"Retrieved {len(all_offers)} total products")
                    if use_proxy and proxy_url:
                        await client.aclose()
                    return all_offers

                # Map offers to internal format
                for offer in offers:
                    all_offers.append(_map_offer(offer))

                logger.info(f"Retrieved {len(offers)} products from page {page}")
                page += 1
                break  # Success, move to next page

            except CircuitOpenError:
                logger.warning("Kaspi API circuit is open, aborting product fetch")
                if rotator:
                    await rotator.record_request(success=False)
                if use_proxy and proxy_url:
                    await client.aclose()
                return all_offers  # Return what we have so far
            except httpx.HTTPStatusError as e:
                if rotator:
                    await rotator.record_request(success=False)
                if e.response.status_code == 429 and retries < max_retries:
                    retries += 1
                    continue
                logger.error(f"HTTP error fetching products: {e}")
                raise
            except httpx.HTTPError as e:
                if rotator:
                    await rotator.record_request(success=False)
                logger.error(f"Error fetching products: {e}")
                raise

        if retries >= max_retries:
            logger.error("Max retries exceeded for rate limiting")
            if use_proxy and proxy_url:
                await client.aclose()
            raise httpx.HTTPError("Too many rate limit retries")

    # Cleanup client if needed
    if use_proxy and proxy_url:
        await client.aclose()

    return all_offers


async def parse_product_by_sku(
    product_id: str,
    session: dict = None,
    city_id: Optional[str] = None,
    user_id: Optional[UUID] = None,
    use_proxy: bool = False,
    module: Optional[str] = None,
) -> dict:
    """
    Parse product details by product ID using Kaspi public offers API.

    This uses the public yml/offer-view API which doesn't require authentication.
    Rate limited at 8 RPS per IP. On 403 (IP ban), pauses 15s globally.

    Args:
        product_id: Kaspi product ID (external_kaspi_id)
        session: Optional session data (not required for public API)
        city_id: City ID for getting city-specific prices (defaults to Almaty)
        user_id: User UUID (reserved for future proxy support)
        use_proxy: Whether to use proxy (reserved for future)
        module: Proxy module name (reserved for future)

    Returns:
        Product data with offers and prices

    Raises:
        Exception: If parsing fails
    """
    effective_city_id = city_id or DEFAULT_CITY_ID
    logger.info(f"Fetching offers for product ID: {product_id}, city: {effective_city_id}")

    # Wait if we're in a 403 ban period
    await wait_for_offers_ban()

    # Acquire offers-specific rate limit token (8 RPS per IP)
    offers_limiter = get_offers_rate_limiter()
    await offers_limiter.acquire()

    # Use public offers API (no auth required)
    url = f"https://kaspi.kz/yml/offer-view/offers/{product_id}"

    # Standard headers for public API with city
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "ru-RU,ru;q=0.9",
        "content-type": "application/json",
        "user-agent": random.choice(USER_AGENTS),
        "x-ks-city": effective_city_id,
        "origin": "https://kaspi.kz",
        "referer": f"https://kaspi.kz/shop/p/-{product_id}/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }

    # Request body with city
    body = {"cityId": effective_city_id}

    client = await get_http_client()
    breaker = get_kaspi_circuit_breaker()

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Use circuit breaker to prevent cascading failures
            async with breaker:
                response = await client.post(
                    url,
                    json=body,
                    headers=headers
                )

            logger.debug(f"Response status: {response.status_code}")

            if response.status_code == 429:
                # Rate limited - wait and retry with jitter to prevent thundering herd
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Rate limited, waiting {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(wait_time)
                continue

            if response.status_code == 403:
                # IP banned - pause globally and retry
                await offers_ban_pause()
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Offers API 403 for product {product_id}, "
                        f"pausing 15s then retry (attempt {attempt + 1}/{max_retries})"
                    )
                    await wait_for_offers_ban()
                    continue
                else:
                    logger.error(f"Offers API 403 for product {product_id}, max retries exhausted")
                    return None

            if response.status_code == 400:
                logger.warning(f"Bad request for product {product_id}: {response.text}")
                return None

            response.raise_for_status()
            result = response.json()
            logger.debug(f"Successfully fetched offers for product {product_id}: {len(result.get('offers', []))} offers")
            return result

        except CircuitOpenError:
            logger.warning(f"Kaspi API circuit is open, skipping product {product_id}")
            return None
        except httpx.HTTPError as e:
            if attempt < max_retries - 1:
                wait_time = 1 + attempt
                logger.warning(f"Request error (attempt {attempt + 1}/{max_retries}): {e}")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Error fetching offers for product {product_id}: {e}")
                raise

    raise Exception(f"Failed to fetch offers for product {product_id} after {max_retries} attempts")


async def sync_product(
    product_id: Union[str, uuid_module.UUID],
    new_price: int,
    session: dict,
    user_id: Optional[UUID] = None,
    use_proxy: bool = False,
    module: Optional[str] = None,
) -> dict:
    """
    Sync product price to Kaspi.

    Rate limited at 1.5 RPS per merchant account. On 429 (30-min ban),
    marks the merchant for cooldown and returns failure immediately.

    Args:
        product_id: Internal product UUID (can be string or UUID)
        new_price: New price to set
        session: Session data with cookies
        user_id: User UUID (reserved for future proxy support)
        use_proxy: Whether to use proxy (reserved, proxies don't help for pricefeed)
        module: Proxy module name (reserved for future)

    Returns:
        Success response

    Raises:
        KaspiAuthError: If session is invalid
        httpx.HTTPError: If API request fails
    """
    logger.info(f"Syncing product {product_id} with price {new_price}")

    # Convert product_id to UUID if it's a string
    product_uuid = uuid_module.UUID(product_id) if isinstance(product_id, str) else product_id

    # Get product data from database
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, kaspi_product_id, kaspi_sku, price, store_id
            FROM products
            WHERE id = $1
            """,
            product_uuid
        )

    if not row:
        raise ValueError(f"Product {product_id} not found")

    # Get session data
    cookies = _get_cookies_from_session(session)
    if not cookies:
        raise KaspiAuthError("No cookies found in session")

    merchant_uid = _get_merchant_uid_from_session(session)
    if not merchant_uid:
        raise KaspiAuthError("No merchant UID in session")

    # Prepare price update request
    headers = _get_merchant_headers()
    url = "https://mc.shop.kaspi.kz/pricefeed/upload/merchant/process"

    body = {
        "merchantUid": merchant_uid,
        "availabilities": [
            {
                "available": "yes",
                "storeId": f"{merchant_uid}_PP1",
                "stockEnabled": False
            }
        ],
        "sku": row["kaspi_sku"],
        "price": new_price
    }

    # Check if this merchant is in pricefeed cooldown (30-min ban)
    if is_merchant_cooled_down(merchant_uid):
        logger.warning(f"Merchant {merchant_uid} is in pricefeed cooldown, skipping price sync")
        return {"success": False, "error": "pricefeed_cooldown", "product_id": str(product_uuid)}

    # Per-merchant rate limiting for pricefeed (1.5 RPS per account)
    pricefeed_limiter = get_pricefeed_rate_limiter(merchant_uid)
    await pricefeed_limiter.acquire()

    client = await get_http_client()
    breaker = get_kaspi_circuit_breaker()

    try:
        # Use circuit breaker to prevent cascading failures
        async with breaker:
            response = await client.post(
                url,
                json=body,
                headers=headers,
                cookies=cookies
            )

        if response.status_code == 401:
            raise KaspiAuthError("Authentication failed - session expired")

        if response.status_code == 429:
            # Pricefeed 429 = 30-minute ban per merchant account!
            mark_pricefeed_cooldown(merchant_uid)
            logger.error(
                f"Pricefeed 429 for merchant {merchant_uid}: "
                f"30-minute cooldown activated"
            )
            return {
                "success": False,
                "error": "pricefeed_rate_limited",
                "product_id": str(product_uuid),
                "cooldown_seconds": 1800,
            }

        response.raise_for_status()
        response_data = response.json()

        # Update price in database
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE products
                SET price = $1, updated_at = NOW()
                WHERE id = $2
                """,
                new_price,
                product_uuid
            )

        logger.info(f"Successfully synced product {product_uuid} with price {new_price}")

        return {
            "success": True,
            "product_id": str(product_uuid),
            "new_price": new_price,
            "response": response_data
        }

    except CircuitOpenError:
        logger.warning(f"Kaspi API circuit is open, cannot sync product {product_uuid}")
        return {"success": False, "error": "circuit_open", "product_id": str(product_uuid)}
    except httpx.HTTPError as e:
        logger.error(f"Error syncing product: {e}")
        raise


async def get_competitor_price(product_id: str, city_id: Optional[str] = None) -> Optional[int]:
    """
    Get lowest competitor price for a product.

    Args:
        product_id: Kaspi product ID (external_kaspi_id)
        city_id: City ID for getting city-specific prices

    Returns:
        Lowest competitor price or None if not found
    """
    effective_city_id = city_id or DEFAULT_CITY_ID
    logger.info(f"Fetching competitor price for product ID: {product_id}, city: {effective_city_id}")

    try:
        # Parse product to get all offers
        product_data = await parse_product_by_sku(product_id, city_id=effective_city_id)

        if not product_data or 'offers' not in product_data:
            logger.warning(f"No offers found for product {product_id}")
            return None

        offers = product_data['offers']
        if not offers:
            return None

        # Find minimum price from offers
        prices = [offer.get('price') for offer in offers if offer.get('price')]
        if not prices:
            return None

        min_price = int(min(prices))
        logger.info(f"Lowest competitor price for product {product_id}: {min_price}")
        return min_price

    except Exception as e:
        logger.error(f"Error getting competitor price for {product_id}: {e}")
        return None


async def fetch_preorders(
    merchant_id: str,
    session: dict,
    limit: Optional[int] = None,
    offset: int = 0
) -> List[dict]:
    """
    Fetch preorders for a store from database.

    Args:
        merchant_id: Merchant/Store ID
        session: Session data (for future API integration)
        limit: Maximum number of preorders to fetch
        offset: Offset for pagination

    Returns:
        List of preorders

    Raises:
        Exception: If database query fails
    """
    logger.info(f"Fetching preorders for merchant {merchant_id}")

    pool = await get_db_pool()

    try:
        # Get store_id from merchant_id
        async with pool.acquire() as conn:
            store_row = await conn.fetchrow(
                """
                SELECT id FROM kaspi_stores
                WHERE merchant_id = $1
                """,
                merchant_id
            )

            if not store_row:
                logger.warning(f"Store not found for merchant {merchant_id}")
                return []

            store_id = store_row['id']

            # Fetch preorders
            rows = await conn.fetch(
                """
                SELECT
                    id,
                    product_id,
                    store_id,
                    article,
                    name,
                    brand,
                    price,
                    status,
                    warehouses,
                    delivery_days,
                    created_at,
                    updated_at
                FROM preorders
                WHERE store_id = $1
                ORDER BY created_at DESC
                OFFSET $2
                LIMIT COALESCE($3, 9223372036854775807)
                """,
                store_id,
                offset,
                limit
            )

        # Format results
        result = []
        for row in rows:
            item = dict(row)

            # Parse warehouses JSON if needed
            if isinstance(item.get('warehouses'), str):
                try:
                    item['warehouses'] = json.loads(item['warehouses'])
                except (json.JSONDecodeError, TypeError):
                    item['warehouses'] = []
            elif item.get('warehouses') is None:
                item['warehouses'] = []

            result.append(item)

        logger.info(f"Retrieved {len(result)} preorders for merchant {merchant_id}")
        return result

    except Exception as e:
        logger.error(f"Error fetching preorders: {e}")
        raise


# ============================================================================
# Orders Fetching
# ============================================================================

async def fetch_orders(
    merchant_id: str,
    session: dict,
    status: str = "ARCHIVE",
    days_back: int = 30
) -> List[dict]:
    """
    Fetch orders from Kaspi merchant API.

    Args:
        merchant_id: Merchant UID
        session: Session data with cookies and GUID
        status: Order status filter (ARCHIVE, NEW, ACCEPTED_BY_MERCHANT, etc.)
        days_back: How many days back to fetch orders

    Returns:
        List of order dictionaries

    Raises:
        Exception: If API call fails
    """
    logger.info(f"Fetching orders for merchant {merchant_id}, status={status}, days_back={days_back}")

    rate_limiter = get_global_rate_limiter()
    await rate_limiter.acquire()

    # Get cookies and GUID from session
    cookies = _get_cookies_from_session(session)
    guid = session.get('guid', '')

    # Calculate date range
    from datetime import timedelta
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Format dates for API
    start_ts = int(start_date.timestamp() * 1000)
    end_ts = int(end_date.timestamp() * 1000)

    # Kaspi orders API endpoint
    url = "https://kaspi.kz/shop/api/v2/orders"

    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "ru-RU,ru;q=0.9",
        "content-type": "application/json",
        "user-agent": random.choice(USER_AGENTS),
        "x-ks-city": "750000000",
        "origin": "https://kaspi.kz",
        "referer": "https://kaspi.kz/mc/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }

    # Build cookie string
    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
    if cookie_str:
        headers["cookie"] = cookie_str

    params = {
        "filter": status,
        "page[number]": 0,
        "page[size]": 100,
        "filter[orders][state]": status,
        "filter[orders][creationDate][$ge]": start_ts,
        "filter[orders][creationDate][$le]": end_ts,
    }

    all_orders = []
    page = 0
    max_pages = 50  # Safety limit

    client = await get_http_client()
    breaker = get_kaspi_circuit_breaker()

    try:
        while page < max_pages:
            params["page[number]"] = page

            try:
                async with breaker:
                    response = await client.get(
                        url,
                        params=params,
                        headers=headers
                    )
            except CircuitOpenError:
                logger.warning(f"Kaspi API circuit is open, returning {len(all_orders)} orders fetched so far")
                break

            if response.status_code == 401:
                logger.warning(f"Unauthorized when fetching orders for merchant {merchant_id}")
                raise KaspiAuthError("Session expired")

            if response.status_code == 429:
                logger.warning("Rate limited, waiting...")
                await asyncio.sleep(2)
                continue

            response.raise_for_status()
            data = response.json()

            orders = data.get("data", [])
            if not orders:
                break

            all_orders.extend(orders)
            logger.debug(f"Fetched page {page}: {len(orders)} orders")

            # Check if there are more pages
            meta = data.get("meta", {})
            total_pages = meta.get("pageCount", 1)
            if page >= total_pages - 1:
                break

            page += 1
            await asyncio.sleep(0.3)  # Rate limiting

    except httpx.HTTPError as e:
        logger.error(f"Error fetching orders: {e}")
        raise

    logger.info(f"Fetched {len(all_orders)} orders for merchant {merchant_id}")
    return all_orders


async def parse_order_details(order_data: dict) -> dict:
    """
    Parse order data from Kaspi API response.

    Args:
        order_data: Raw order data from API

    Returns:
        Parsed order dictionary
    """
    attributes = order_data.get("attributes", {})

    # Parse order items/entries
    entries = []
    for entry in attributes.get("entries", []):
        entries.append({
            "kaspi_product_id": entry.get("product", {}).get("code", ""),
            "name": entry.get("product", {}).get("name", ""),
            "sku": entry.get("product", {}).get("sku", ""),
            "quantity": entry.get("quantity", 1),
            "price": int(entry.get("basePrice", 0)),  # Price in tenge
        })

    # Parse customer info
    customer = attributes.get("customer", {})

    return {
        "kaspi_order_id": order_data.get("id", ""),
        "kaspi_order_code": attributes.get("code", ""),
        "status": attributes.get("state", ""),
        "total_price": int(attributes.get("totalPrice", 0)),
        "delivery_cost": int(attributes.get("deliveryCost", 0)),
        "customer_name": f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip(),
        "customer_phone": customer.get("cellPhone", ""),
        "delivery_address": attributes.get("deliveryAddress", {}).get("formattedAddress", ""),
        "delivery_mode": attributes.get("deliveryMode", ""),
        "payment_mode": attributes.get("paymentMode", ""),
        "order_date": datetime.fromtimestamp(attributes.get("creationDate", 0) / 1000) if attributes.get("creationDate") else datetime.utcnow(),
        "entries": entries,
    }


async def sync_orders_to_db(
    store_id: str,
    orders: List[dict]
) -> dict:
    """
    Sync orders to database.

    Args:
        store_id: Store UUID
        orders: List of parsed order dictionaries

    Returns:
        Sync result summary
    """
    import uuid as uuid_module
    logger.info(f"Syncing {len(orders)} orders to database for store {store_id}")

    pool = await get_db_pool()
    inserted = 0
    updated = 0
    errors = 0

    try:
        async with pool.acquire() as conn:
            for order in orders:
                try:
                    # Parse order details
                    parsed = await parse_order_details(order)

                    # Upsert order
                    result = await conn.fetchrow(
                        """
                        INSERT INTO orders (
                            store_id, kaspi_order_id, kaspi_order_code, status,
                            total_price, delivery_cost, customer_name, customer_phone,
                            delivery_address, delivery_mode, payment_mode, order_date
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        ON CONFLICT (store_id, kaspi_order_id)
                        DO UPDATE SET
                            status = $4,
                            total_price = $5,
                            updated_at = NOW()
                        RETURNING id, (xmax = 0) as inserted
                        """,
                        uuid_module.UUID(store_id),
                        parsed["kaspi_order_id"],
                        parsed["kaspi_order_code"],
                        parsed["status"],
                        parsed["total_price"],
                        parsed["delivery_cost"],
                        parsed["customer_name"],
                        parsed["customer_phone"],
                        parsed["delivery_address"],
                        parsed["delivery_mode"],
                        parsed["payment_mode"],
                        parsed["order_date"],
                    )

                    order_id = result["id"]
                    if result["inserted"]:
                        inserted += 1
                    else:
                        updated += 1

                    # Upsert order items (only for new orders)
                    if result["inserted"]:
                        for entry in parsed["entries"]:
                            # Try to find matching product
                            product = await conn.fetchrow(
                                """
                                SELECT id FROM products
                                WHERE store_id = $1 AND (kaspi_product_id = $2 OR kaspi_sku = $3)
                                LIMIT 1
                                """,
                                uuid_module.UUID(store_id),
                                entry["kaspi_product_id"],
                                entry["sku"]
                            )

                            await conn.execute(
                                """
                                INSERT INTO order_items (
                                    order_id, product_id, kaspi_product_id,
                                    name, sku, quantity, price
                                )
                                VALUES ($1, $2, $3, $4, $5, $6, $7)
                                """,
                                order_id,
                                product["id"] if product else None,
                                entry["kaspi_product_id"],
                                entry["name"],
                                entry["sku"],
                                entry["quantity"],
                                entry["price"],
                            )

                            # Update sales_count for matched product
                            if product:
                                await conn.execute(
                                    "UPDATE products SET sales_count = COALESCE(sales_count, 0) + $1 WHERE id = $2",
                                    entry["quantity"], product["id"]
                                )

                except Exception as e:
                    logger.error(f"Error syncing order: {e}")
                    errors += 1

            # Update last_orders_sync
            await conn.execute(
                """
                UPDATE kaspi_stores
                SET last_orders_sync = NOW()
                WHERE id = $1
                """,
                uuid_module.UUID(store_id)
            )

    except Exception as e:
        logger.error(f"Error in sync_orders_to_db: {e}")
        raise

    logger.info(f"Orders sync complete: {inserted} inserted, {updated} updated, {errors} errors")
    return {"inserted": inserted, "updated": updated, "errors": errors}


# ============================================================================
# Additional Helper Functions
# ============================================================================

async def validate_merchant_session(merchant_id: str) -> bool:
    """
    Validate that a merchant's session is still active.

    Args:
        merchant_id: Merchant UID

    Returns:
        True if session is valid, False otherwise
    """
    try:
        session = await get_active_session(merchant_id)
        if not session:
            return False

        return await validate_session(session)

    except Exception as e:
        logger.error(f"Error validating merchant session: {e}")
        return False


async def get_merchant_session(merchant_id: str) -> Optional[dict]:
    """
    Get active session for a merchant.

    Args:
        merchant_id: Merchant UID

    Returns:
        Session data or None if not found/invalid
    """
    try:
        return await get_active_session(merchant_id)
    except Exception as e:
        logger.error(f"Error getting merchant session: {e}")
        return None


# ============================================================================
# Batch Operations
# ============================================================================

async def batch_sync_products(
    product_updates: List[Dict[str, Any]],
    session: dict,
    batch_size: int = 10
) -> Dict[str, Any]:
    """
    Sync multiple products in batches.

    Args:
        product_updates: List of {product_id, new_price} dicts
        session: Session data with cookies
        batch_size: Number of products to sync concurrently

    Returns:
        Summary of sync results
    """
    logger.info(f"Batch syncing {len(product_updates)} products")

    results = {
        'success': [],
        'failed': [],
        'total': len(product_updates)
    }

    # Process in batches
    for i in range(0, len(product_updates), batch_size):
        batch = product_updates[i:i + batch_size]

        tasks = [
            sync_product(
                product_id=update['product_id'],
                new_price=update['new_price'],
                session=session
            )
            for update in batch
        ]

        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for update, result in zip(batch, batch_results):
            if isinstance(result, Exception):
                logger.error(f"Failed to sync {update['product_id']}: {result}")
                results['failed'].append({
                    'product_id': update['product_id'],
                    'error': str(result)
                })
            else:
                results['success'].append(update['product_id'])

        # Small delay between batches to avoid rate limiting
        if i + batch_size < len(product_updates):
            await asyncio.sleep(0.5)

    logger.info(
        f"Batch sync complete: {len(results['success'])} successful, "
        f"{len(results['failed'])} failed"
    )

    return results
