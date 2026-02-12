"""
Global HTTP Client with connection pooling.

Reuses TCP connections for better performance and to prevent
connection leaks when making many requests to Kaspi API.
"""
import httpx
import logging
from typing import Optional

from ..config import settings

logger = logging.getLogger(__name__)

# Global HTTP client (singleton)
_http_client: Optional[httpx.AsyncClient] = None

# Separate HTTP/1.1 client for Kaspi offers API (avoids 405 with HTTP/2)
_offers_http_client: Optional[httpx.AsyncClient] = None


async def get_http_client() -> httpx.AsyncClient:
    """
    Get global HTTP client with connection pooling.

    Uses connection pooling to reuse TCP connections.
    Should be closed on application shutdown via close_http_client().

    Returns:
        Shared httpx.AsyncClient instance
    """
    global _http_client

    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30.0
            ),
            http2=True,  # Enable HTTP/2 for better multiplexing
        )
        logger.info("HTTP client created with connection pooling")

    return _http_client


async def get_offers_http_client() -> httpx.AsyncClient:
    """
    Get HTTP/1.1 client for Kaspi offers API.

    Uses HTTP/1.1 (not HTTP/2) because Kaspi's WAF may return 405 for HTTP/2 POST.
    Optionally routes through offers_proxy if configured (for banned VPS IPs).

    Returns:
        httpx.AsyncClient configured for offers API
    """
    global _offers_http_client

    if _offers_http_client is None or _offers_http_client.is_closed:
        kwargs = {
            "timeout": httpx.Timeout(30.0, connect=10.0),
            "limits": httpx.Limits(
                max_connections=50,
                max_keepalive_connections=10,
                keepalive_expiry=30.0
            ),
            "http2": False,  # HTTP/1.1 only — Kaspi WAF may 405 on HTTP/2 POST
        }

        proxy = settings.offers_proxy
        if proxy:
            kwargs["proxy"] = proxy
            logger.info(f"Offers HTTP client created with proxy")
        else:
            logger.info("Offers HTTP client created (HTTP/1.1, no proxy)")

        _offers_http_client = httpx.AsyncClient(**kwargs)

    return _offers_http_client


async def close_http_client():
    """
    Close global HTTP clients on shutdown.

    Should be called during application shutdown to properly
    close all connections and release resources.
    """
    global _http_client, _offers_http_client

    if _http_client is not None and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None
        logger.info("HTTP client closed")

    if _offers_http_client is not None and not _offers_http_client.is_closed:
        await _offers_http_client.aclose()
        _offers_http_client = None
        logger.info("Offers HTTP client closed")
