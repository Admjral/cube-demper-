"""Browser Farm - manages Playwright browser contexts with pooling and rate limiting"""

import asyncio
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
import httpx

from ..config import settings
from .rate_limiter import get_global_rate_limiter

logger = logging.getLogger(__name__)


class BrowserShard:
    """
    Single browser shard managing multiple contexts.

    Each shard maintains a pool of browser contexts and handles
    garbage collection of idle contexts.
    """

    def __init__(self, shard_id: str):
        self.shard_id = shard_id
        self.browser: Optional[Browser] = None
        self.contexts: Dict[str, tuple[BrowserContext, datetime]] = {}
        self._lock = asyncio.Lock()
        self._playwright = None
        self._initialized = False

    async def initialize(self):
        """Initialize Playwright and browser"""
        if self._initialized:
            return

        try:
            self._playwright = await async_playwright().start()
            self.browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                ]
            )
            self._initialized = True
            logger.info(f"Browser shard {self.shard_id} initialized")
        except Exception as e:
            logger.error(f"Failed to initialize browser shard {self.shard_id}: {e}")
            raise

    async def get_context(self, proxy: Optional[Dict[str, str]] = None) -> BrowserContext:
        """Get or create browser context with optional proxy"""
        if not self._initialized:
            await self.initialize()

        proxy_key = f"{proxy.get('server', 'direct')}" if proxy else "direct"

        async with self._lock:
            # Check if context exists and is still valid
            if proxy_key in self.contexts:
                context, last_used = self.contexts[proxy_key]
                self.contexts[proxy_key] = (context, datetime.now())
                return context

            # Create new context
            context = await self.browser.new_context(
                proxy=proxy,
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                viewport={'width': 1920, 'height': 1080},
                locale='ru-RU',
                timezone_id='Asia/Almaty',
            )

            self.contexts[proxy_key] = (context, datetime.now())
            logger.debug(f"Created new context for proxy {proxy_key}")
            return context

    async def post_json(
        self,
        url: str,
        json_body: dict,
        headers: Optional[dict] = None,
        proxy: Optional[Dict[str, str]] = None
    ) -> dict:
        """
        Make POST request with JSON body using browser context.

        Args:
            url: Request URL
            json_body: JSON body
            headers: Optional headers
            proxy: Optional proxy configuration

        Returns:
            Response JSON
        """
        # Acquire rate limit token
        rate_limiter = get_global_rate_limiter()
        await rate_limiter.acquire()

        context = await self.get_context(proxy)
        page = await context.new_page()

        try:
            # Set extra headers
            if headers:
                await page.set_extra_http_headers(headers)

            # Make request
            response = await page.request.post(
                url,
                data=json_body,
                timeout=settings.request_timeout_ms
            )

            # Parse JSON response
            result = await response.json()
            return result

        finally:
            await page.close()

    async def garbage_collect(self):
        """Close idle contexts older than TTL"""
        now = datetime.now()
        ttl = timedelta(seconds=settings.idle_context_ttl)

        async with self._lock:
            to_remove = []

            for proxy_key, (context, last_used) in self.contexts.items():
                if now - last_used > ttl:
                    try:
                        await context.close()
                        to_remove.append(proxy_key)
                        logger.debug(f"Closed idle context: {proxy_key}")
                    except Exception as e:
                        logger.error(f"Error closing context {proxy_key}: {e}")

            for key in to_remove:
                del self.contexts[key]

    async def close(self):
        """Close all contexts and browser"""
        async with self._lock:
            for context, _ in self.contexts.values():
                try:
                    await context.close()
                except Exception as e:
                    logger.error(f"Error closing context: {e}")

            self.contexts.clear()

            if self.browser:
                await self.browser.close()
                self.browser = None

            if self._playwright:
                await self._playwright.stop()
                self._playwright = None

            self._initialized = False
            logger.info(f"Browser shard {self.shard_id} closed")


class BrowserFarmSharded:
    """
    Sharded browser farm for distributed request handling.

    Manages multiple browser shards with automatic load balancing
    and garbage collection.
    """

    def __init__(self, num_shards: int = None):
        self.num_shards = num_shards or settings.browser_shards
        self.shards = [BrowserShard(f"shard-{i}") for i in range(self.num_shards)]
        self._gc_task: Optional[asyncio.Task] = None
        self._running = False

    async def initialize(self):
        """Initialize all shards"""
        await asyncio.gather(*[shard.initialize() for shard in self.shards])
        self._running = True

        # Start garbage collection task
        self._gc_task = asyncio.create_task(self._gc_loop())
        logger.info(f"Browser farm initialized with {self.num_shards} shards")

    def _pick_shard(self, proxy: Optional[Dict[str, str]] = None) -> BrowserShard:
        """Pick shard based on proxy (for consistent routing)"""
        if proxy and proxy.get('server'):
            # Hash proxy server to consistently route to same shard
            proxy_hash = hash(proxy['server'])
            shard_index = proxy_hash % self.num_shards
        else:
            # Round-robin for direct connections
            shard_index = hash(datetime.now()) % self.num_shards

        return self.shards[shard_index]

    async def post_json(
        self,
        url: str,
        json_body: dict,
        headers: Optional[dict] = None,
        proxy: Optional[Dict[str, str]] = None
    ) -> dict:
        """Make POST request through browser farm"""
        shard = self._pick_shard(proxy)
        return await shard.post_json(url, json_body, headers, proxy)

    async def _gc_loop(self):
        """Periodic garbage collection loop"""
        while self._running:
            try:
                await asyncio.sleep(60)  # Run GC every minute
                await asyncio.gather(*[shard.garbage_collect() for shard in self.shards])
            except Exception as e:
                logger.error(f"Error in GC loop: {e}")

    async def close(self):
        """Close all shards"""
        self._running = False

        if self._gc_task:
            self._gc_task.cancel()
            try:
                await self._gc_task
            except asyncio.CancelledError:
                pass

        await asyncio.gather(*[shard.close() for shard in self.shards])
        logger.info("Browser farm closed")


# Global browser farm instance
browser_farm: Optional[BrowserFarmSharded] = None


async def get_browser_farm() -> BrowserFarmSharded:
    """Get global browser farm instance"""
    global browser_farm
    if browser_farm is None:
        browser_farm = BrowserFarmSharded()
        await browser_farm.initialize()
    return browser_farm


async def close_browser_farm():
    """Close global browser farm"""
    global browser_farm
    if browser_farm:
        await browser_farm.close()
        browser_farm = None
