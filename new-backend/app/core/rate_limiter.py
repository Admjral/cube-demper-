"""
Token Bucket Rate Limiter - controls request rates per endpoint.

Provides:
- Global rate limiter (legacy, used by browser_farm)
- Offers rate limiter (per IP, 8 RPS)
- Pricefeed rate limiter (per merchant account, 1.5 RPS)
- Pricefeed cooldown tracking (30-min ban after 429)
- Offers ban pause (15s after 403)
"""

import asyncio
import logging
import time
from typing import Optional, Dict


logger = logging.getLogger(__name__)


class TokenBucket:
    """
    Token Bucket rate limiter for controlling request throughput.

    Used to enforce RPS limits per endpoint/account.
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


# ============================================================================
# Global rate limiter (legacy, used by browser_farm and catalog endpoint)
# ============================================================================

global_rate_limiter: Optional[TokenBucket] = None


def get_global_rate_limiter() -> TokenBucket:
    """Get global rate limiter instance"""
    global global_rate_limiter
    if global_rate_limiter is None:
        from ..config import settings
        global_rate_limiter = TokenBucket(rate=settings.global_rps)
    return global_rate_limiter


# ============================================================================
# Offers rate limiter (public endpoint, per IP)
# ============================================================================

_offers_rate_limiter: Optional[TokenBucket] = None

# Monotonic timestamp until which all offers requests should pause (403 ban)
_offers_ban_until: float = 0.0


def get_offers_rate_limiter() -> TokenBucket:
    """Get rate limiter for offers API (public endpoint, per IP)"""
    global _offers_rate_limiter
    if _offers_rate_limiter is None:
        from ..config import settings
        _offers_rate_limiter = TokenBucket(rate=settings.offers_rps)
    return _offers_rate_limiter


async def offers_ban_pause():
    """Called when 403 received from offers API. Sets global pause."""
    global _offers_ban_until
    from ..config import settings
    _offers_ban_until = time.monotonic() + settings.offers_ban_pause_seconds
    logger.warning(
        f"[RATE_LIMIT] Offers API 403 detected, "
        f"pausing all offers requests for {settings.offers_ban_pause_seconds}s"
    )


async def wait_for_offers_ban():
    """Wait if currently in a 403 ban period for offers API."""
    global _offers_ban_until
    now = time.monotonic()
    if _offers_ban_until > now:
        wait_time = _offers_ban_until - now
        logger.debug(f"[RATE_LIMIT] Waiting {wait_time:.1f}s for offers 403 ban to expire")
        await asyncio.sleep(wait_time)


# ============================================================================
# Pricefeed rate limiter (per merchant account — NOT per IP!)
# ============================================================================

_pricefeed_rate_limiters: Dict[str, TokenBucket] = {}

# Cooldown tracking: merchant_uid -> monotonic timestamp when cooldown expires
_pricefeed_cooldowns: Dict[str, float] = {}


def get_pricefeed_rate_limiter(merchant_uid: str) -> TokenBucket:
    """Get rate limiter for pricefeed API for a specific merchant account."""
    if merchant_uid not in _pricefeed_rate_limiters:
        from ..config import settings
        # capacity=1 prevents bursts — at most 1 request can fire immediately
        _pricefeed_rate_limiters[merchant_uid] = TokenBucket(
            rate=settings.pricefeed_rps, capacity=1
        )
    return _pricefeed_rate_limiters[merchant_uid]


def mark_pricefeed_cooldown(merchant_uid: str):
    """Mark a merchant as cooled down after 429 from pricefeed (30-min ban)."""
    from ..config import settings
    cooldown_until = time.monotonic() + settings.pricefeed_cooldown_seconds
    _pricefeed_cooldowns[merchant_uid] = cooldown_until
    logger.warning(
        f"[RATE_LIMIT] Pricefeed 429 for merchant {merchant_uid}: "
        f"cooling down for {settings.pricefeed_cooldown_seconds}s (30 min)"
    )


def is_merchant_cooled_down(merchant_uid: str) -> bool:
    """Check if a merchant is in pricefeed cooldown (after 429)."""
    cooldown_until = _pricefeed_cooldowns.get(merchant_uid, 0.0)
    if cooldown_until > time.monotonic():
        return True
    # Expired — clean up
    _pricefeed_cooldowns.pop(merchant_uid, None)
    return False
