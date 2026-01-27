"""
Global HTTP Client with connection pooling.

Reuses TCP connections for better performance and to prevent
connection leaks when making many requests to Kaspi API.
"""
import httpx
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Global HTTP client (singleton)
_http_client: Optional[httpx.AsyncClient] = None


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


async def close_http_client():
    """
    Close global HTTP client on shutdown.

    Should be called during application shutdown to properly
    close all connections and release resources.
    """
    global _http_client

    if _http_client is not None and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None
        logger.info("HTTP client closed")
