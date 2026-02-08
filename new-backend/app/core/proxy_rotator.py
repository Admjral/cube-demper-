"""
Proxy Rotator - Manages automatic proxy rotation every 249 requests

Supports per-module proxy pools:
- demper: 70 proxies (1000 req/cycle every 3 min, 5 proxies/cycle, 42 min rest)
- orders: 25 proxies (up to 1000 req/cycle every 10 min, 5 proxies/cycle, 50 min rest)
- catalog: 5 proxies (20 req rarely, 1 proxy sufficient)
- reserve: 0 proxies (uses catalog proxies when needed)
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Tuple
from uuid import UUID

from ..core.database import get_db_pool
from ..models.proxy import Proxy

logger = logging.getLogger(__name__)


class NoProxiesAllocatedError(Exception):
    """Raised when user has no proxies allocated for the specified module"""
    pass


class NoProxiesAvailableError(Exception):
    """Raised when all proxies are dead or unavailable"""
    pass


class ProxyRotator:
    """
    Manages proxy rotation for a user's specific module

    Key features:
    - Tracks request count in memory (fast!)
    - Rotates after 249 requests (not 250!)
    - Rests used proxies for 40 minutes
    - Automatically selects next available proxy
    """

    def __init__(self, user_id: UUID, module: str = 'demper'):
        """
        Initialize ProxyRotator for specific user and module

        Args:
            user_id: UUID of the user
            module: Module name ('demper', 'orders', 'catalog', 'reserve')
        """
        self.user_id = user_id
        self.module = module
        self.current_proxy: Optional[Proxy] = None
        self.current_requests_count = 0
        self.max_requests_per_proxy = 249  # ⚠️ Not 250!

        # In-memory cache of user's proxies for this module
        self.user_proxies: list[Proxy] = []
        self._initialized = False

    async def initialize(self):
        """
        Load user's proxies for this module from database into memory
        """
        if self._initialized:
            return

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            query = """
                SELECT * FROM proxies
                WHERE user_id = $1
                  AND module = $2
                  AND status IN ('allocated', 'resting')
                ORDER BY
                    CASE WHEN status = 'allocated' THEN 0 ELSE 1 END,
                    available_at ASC NULLS FIRST,
                    last_used_at ASC NULLS FIRST
            """
            rows = await conn.fetch(query, self.user_id, self.module)

            if not rows:
                raise NoProxiesAllocatedError(
                    f"User {self.user_id} has no proxies for module '{self.module}'"
                )

            self.user_proxies = [Proxy(**dict(row)) for row in rows]

        # Select first available proxy
        self.current_proxy = await self._get_next_available_proxy()
        self._initialized = True

        logger.info(
            f"ProxyRotator initialized for user {self.user_id}, module '{self.module}': "
            f"{len(self.user_proxies)} proxies loaded, current proxy: {self.current_proxy.id}"
        )

    async def _get_next_available_proxy(self) -> Proxy:
        """
        Get next available proxy from user's pool

        Priority:
        1. status='allocated' (never used yet)
        2. status='resting' but available_at <= NOW() (finished resting)
        3. If all resting → take closest to available_at (wait if needed)
        """
        now = datetime.now(timezone.utc)

        # Filter available proxies
        available = [
            p for p in self.user_proxies
            if p.status == 'allocated' or (
                p.status == 'resting' and
                (p.available_at is None or p.available_at <= now)
            )
        ]

        if available:
            return available[0]

        # All proxies are resting - wait for nearest one
        resting = [p for p in self.user_proxies if p.status == 'resting']
        if resting:
            next_proxy = min(resting, key=lambda p: p.available_at or now)
            wait_seconds = (next_proxy.available_at - now).total_seconds()

            if wait_seconds > 0:
                logger.warning(
                    f"All proxies resting for module '{self.module}', "
                    f"waiting {wait_seconds:.1f}s for proxy {next_proxy.id}"
                )
                await asyncio.sleep(wait_seconds)

            return next_proxy

        # All proxies are dead
        raise NoProxiesAvailableError(
            f"All proxies are dead or unavailable for user {self.user_id}, module '{self.module}'"
        )

    async def get_current_proxy(self) -> Proxy:
        """
        Get current proxy for use

        Automatically rotates after 249 requests!
        """
        if not self._initialized:
            await self.initialize()

        # Check if rotation needed
        if self.current_requests_count >= self.max_requests_per_proxy:
            await self._rotate_proxy()

        return self.current_proxy

    async def _rotate_proxy(self):
        """
        Rotate to next proxy

        Steps:
        1. Current proxy → resting (40 minutes)
        2. Get next available proxy
        3. Reset request counter
        """
        if self.current_proxy:
            # Send current proxy to rest
            await self._set_proxy_resting(
                self.current_proxy.id,
                duration_minutes=40
            )

            logger.info(
                f"Proxy {self.current_proxy.id} (module='{self.module}') rotated after "
                f"{self.current_requests_count} requests, resting 40 minutes"
            )

        # Get next proxy
        self.current_proxy = await self._get_next_available_proxy()
        self.current_requests_count = 0

        logger.info(
            f"Switched to proxy {self.current_proxy.id} (module='{self.module}')"
        )

    async def _set_proxy_resting(self, proxy_id: UUID, duration_minutes: int):
        """
        Set proxy to resting status

        Args:
            proxy_id: Proxy UUID
            duration_minutes: Rest duration (typically 40)
        """
        pool = await get_db_pool()
        available_at = datetime.now(timezone.utc) + timedelta(minutes=duration_minutes)

        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE proxies
                SET status = 'resting',
                    available_at = $1,
                    requests_count = 0
                WHERE id = $2
                """,
                available_at,
                proxy_id
            )

        # Update in-memory cache
        for p in self.user_proxies:
            if p.id == proxy_id:
                p.status = 'resting'
                p.available_at = available_at
                p.requests_count = 0

    async def record_request(self, success: bool):
        """
        Record request usage

        Call this AFTER each Kaspi API request

        Args:
            success: Whether request succeeded
        """
        self.current_requests_count += 1
        self.current_proxy.requests_count += 1

        if success:
            self.current_proxy.success_count += 1
        else:
            self.current_proxy.failure_count += 1

            # Check failure rate
            total_requests = self.current_proxy.success_count + self.current_proxy.failure_count
            failure_rate = self.current_proxy.failure_count / total_requests if total_requests > 0 else 0

            # Mark as dead if high failure rate
            if failure_rate > 0.5 and self.current_proxy.failure_count > 10:
                logger.error(
                    f"Proxy {self.current_proxy.id} (module='{self.module}') has high failure rate "
                    f"({failure_rate:.1%}), marking as dead"
                )
                await self._mark_proxy_dead(self.current_proxy.id)

                # Rotate immediately
                await self._rotate_proxy()

        # Update last_used_at in database periodically (every 50 requests)
        if self.current_requests_count % 50 == 0:
            await self._update_proxy_stats()

    async def _mark_proxy_dead(self, proxy_id: UUID):
        """Mark proxy as dead"""
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE proxies SET status = 'dead' WHERE id = $1",
                proxy_id
            )

        # Remove from cache
        self.user_proxies = [p for p in self.user_proxies if p.id != proxy_id]

        logger.error(f"Proxy {proxy_id} marked as dead")

    async def _update_proxy_stats(self):
        """Update proxy statistics in database"""
        if not self.current_proxy:
            return

        pool = await get_db_pool()

        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE proxies
                SET requests_count = $1,
                    success_count = $2,
                    failure_count = $3,
                    last_used_at = NOW()
                WHERE id = $4
                """,
                self.current_proxy.requests_count,
                self.current_proxy.success_count,
                self.current_proxy.failure_count,
                self.current_proxy.id
            )


# Global cache: (user_id, module) → ProxyRotator
_rotator_cache: Dict[Tuple[UUID, str], ProxyRotator] = {}


async def get_user_proxy_rotator(user_id: UUID, module: str = 'demper') -> ProxyRotator:
    """
    Get ProxyRotator for user and module (singleton per user-module pair)

    Each (user_id, module) combination has its own rotator instance!

    Args:
        user_id: User UUID
        module: Module name ('demper', 'orders', 'catalog', 'reserve')

    Returns:
        ProxyRotator instance
    """
    cache_key = (user_id, module)

    if cache_key not in _rotator_cache:
        rotator = ProxyRotator(user_id, module=module)
        await rotator.initialize()
        _rotator_cache[cache_key] = rotator

    return _rotator_cache[cache_key]


async def clear_rotator_cache(user_id: Optional[UUID] = None):
    """
    Clear rotator cache

    Args:
        user_id: If provided, clear only for this user. Otherwise clear all.
    """
    global _rotator_cache

    if user_id is None:
        _rotator_cache.clear()
        logger.info("Cleared all proxy rotator cache")
    else:
        keys_to_remove = [key for key in _rotator_cache if key[0] == user_id]
        for key in keys_to_remove:
            del _rotator_cache[key]
        logger.info(f"Cleared proxy rotator cache for user {user_id}")
