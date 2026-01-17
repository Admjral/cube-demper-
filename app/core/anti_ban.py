"""Anti-ban utilities for scraping protection"""

import random
import asyncio
import logging
from typing import List

logger = logging.getLogger(__name__)


# Realistic User-Agent rotation pool
USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    # Chrome on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    # Firefox on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]


def get_random_user_agent() -> str:
    """Get random realistic User-Agent"""
    return random.choice(USER_AGENTS)


async def random_delay(min_ms: float = 500, max_ms: float = 2000):
    """
    Add random delay to mimic human behavior.

    Args:
        min_ms: Minimum delay in milliseconds
        max_ms: Maximum delay in milliseconds
    """
    delay_seconds = random.uniform(min_ms / 1000, max_ms / 1000)
    await asyncio.sleep(delay_seconds)
    logger.debug(f"Added random delay: {delay_seconds:.2f}s")


async def exponential_backoff_delay(attempt: int, base_ms: float = 1000, max_ms: float = 60000):
    """
    Exponential backoff delay for retries.

    Args:
        attempt: Current attempt number (0-indexed)
        base_ms: Base delay in milliseconds
        max_ms: Maximum delay cap in milliseconds
    """
    # 2^attempt * base_ms with jitter
    delay = min(base_ms * (2 ** attempt), max_ms)
    # Add ±20% jitter
    jitter = random.uniform(0.8, 1.2)
    delay_seconds = (delay * jitter) / 1000

    logger.info(f"Exponential backoff (attempt {attempt}): {delay_seconds:.2f}s")
    await asyncio.sleep(delay_seconds)


def should_retry_on_status(status_code: int) -> bool:
    """
    Determine if request should be retried based on status code.

    Args:
        status_code: HTTP status code

    Returns:
        True if should retry
    """
    # Retry on rate limiting and server errors
    retry_codes = {
        429,  # Too Many Requests
        500,  # Internal Server Error
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
    }
    return status_code in retry_codes


def is_potential_ban(status_code: int, response_text: str = "") -> bool:
    """
    Detect potential IP ban or blocking.

    Args:
        status_code: HTTP status code
        response_text: Response body text

    Returns:
        True if likely banned/blocked
    """
    # Ban indicators
    ban_status_codes = {403, 429}  # Forbidden, Too Many Requests

    ban_keywords = [
        "blocked",
        "banned",
        "captcha",
        "access denied",
        "too many requests",
        "rate limit exceeded",
    ]

    if status_code in ban_status_codes:
        return True

    # Check response text for ban keywords
    response_lower = response_text.lower()
    for keyword in ban_keywords:
        if keyword in response_lower:
            logger.warning(f"Potential ban detected: keyword '{keyword}' in response")
            return True

    return False


class RequestThrottler:
    """
    Throttle requests with adaptive rate limiting based on response codes.

    Automatically slows down when receiving 429/503 errors.
    """

    def __init__(self, initial_delay_ms: float = 500):
        self.delay_ms = initial_delay_ms
        self.min_delay_ms = 200
        self.max_delay_ms = 5000
        self._lock = asyncio.Lock()

    async def wait(self):
        """Wait before next request with current throttle delay"""
        delay_seconds = self.delay_ms / 1000
        await asyncio.sleep(delay_seconds)

    async def increase_delay(self, multiplier: float = 2.0):
        """Increase delay when rate limited"""
        async with self._lock:
            old_delay = self.delay_ms
            self.delay_ms = min(self.delay_ms * multiplier, self.max_delay_ms)
            logger.warning(
                f"Throttle increased: {old_delay:.0f}ms → {self.delay_ms:.0f}ms"
            )

    async def decrease_delay(self, multiplier: float = 0.9):
        """Decrease delay when successful"""
        async with self._lock:
            self.delay_ms = max(self.delay_ms * multiplier, self.min_delay_ms)

    async def on_success(self):
        """Called after successful request - gradually decrease delay"""
        await self.decrease_delay(0.95)

    async def on_rate_limit(self):
        """Called when rate limited - increase delay significantly"""
        await self.increase_delay(2.0)

    async def on_server_error(self):
        """Called on server error - increase delay moderately"""
        await self.increase_delay(1.5)


# Global throttler instance
_global_throttler: RequestThrottler | None = None


def get_global_throttler() -> RequestThrottler:
    """Get global request throttler instance"""
    global _global_throttler
    if _global_throttler is None:
        _global_throttler = RequestThrottler(initial_delay_ms=500)
    return _global_throttler
