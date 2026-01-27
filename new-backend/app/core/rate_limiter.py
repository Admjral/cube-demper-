"""Token Bucket Rate Limiter - controls global request rate"""

import asyncio
import time
from typing import Optional


class TokenBucket:
    """
    Token Bucket rate limiter for controlling request throughput.

    Used to enforce global RPS limits across all demper workers.
    Example: 120 RPS global limit across 4 worker instances.
    """

    def __init__(self, rate: float, capacity: Optional[float] = None):
        """
        Initialize token bucket.

        Args:
            rate: Tokens per second (RPS limit)
            capacity: Maximum bucket capacity (defaults to rate)
        """
        self.rate = rate
        self.capacity = capacity if capacity is not None else rate
        self.tokens = self.capacity
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        """
        Acquire tokens from bucket, waiting if necessary.

        Blocks until sufficient tokens are available.

        Args:
            tokens: Number of tokens to acquire
        """
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.last_update

                # Refill tokens based on elapsed time
                self.tokens = min(
                    self.capacity,
                    self.tokens + elapsed * self.rate
                )
                self.last_update = now

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

                # Calculate wait time if not enough tokens
                needed = tokens - self.tokens
                wait_time = needed / self.rate

            # Wait outside the lock to allow other coroutines
            await asyncio.sleep(wait_time)

    async def try_acquire(self, tokens: float = 1.0) -> bool:
        """
        Try to acquire tokens without blocking.

        Returns:
            True if tokens were acquired, False otherwise
        """
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update

            # Refill tokens
            self.tokens = min(
                self.capacity,
                self.tokens + elapsed * self.rate
            )
            self.last_update = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True

            return False

    def get_available_tokens(self) -> float:
        """Get current number of available tokens (for monitoring)"""
        now = time.monotonic()
        elapsed = now - self.last_update
        return min(self.capacity, self.tokens + elapsed * self.rate)


# Global rate limiter instance (shared across the application)
# Will be initialized in main.py
global_rate_limiter: Optional[TokenBucket] = None


def get_global_rate_limiter() -> TokenBucket:
    """Get global rate limiter instance"""
    global global_rate_limiter
    if global_rate_limiter is None:
        from ..config import settings
        global_rate_limiter = TokenBucket(rate=settings.global_rps)
    return global_rate_limiter
