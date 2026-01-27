"""
Demper Worker - Core price automation engine for Kaspi product price management

This worker implements automatic price demping (price reduction) to stay competitive:
- Sharded architecture for horizontal scaling (multiple worker instances)
- Each instance processes a subset of products using hash-based distribution
- Monitors competitor prices and adjusts own prices to maintain competitiveness
- Respects minimum profit margins and rate limits
- Records all price changes to history table

Architecture:
    - Multiple worker instances can run in parallel
    - Each instance handles products where: mod(abs(hashtext(id::text)), INSTANCE_COUNT) = INSTANCE_INDEX
    - Global rate limiter ensures we don't exceed Kaspi API limits
    - Browser farm provides pooled browser contexts for scraping
    - Async/await throughout for optimal performance

Usage:
    As standalone worker:
        $ INSTANCE_INDEX=0 INSTANCE_COUNT=4 python -m app.workers.demper_instance

    Programmatically:
        from app.workers.demper_instance import DemperWorker
        worker = DemperWorker()
        await worker.start()
"""

import asyncio
import logging
import random
import signal
import sys
import time
from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
from uuid import UUID

from ..config import settings
from ..core.database import get_db_pool, close_pool
from ..core.browser_farm import get_browser_farm, close_browser_farm
from ..core.rate_limiter import get_global_rate_limiter
from ..services.api_parser import parse_product_by_sku, sync_product, get_merchant_session
from ..services.kaspi_auth_service import get_active_session_with_refresh

logger = logging.getLogger(__name__)


# ============================================================================
# Custom Logging Filter - Suppress HTTP Request Logs
# ============================================================================

class NoHttpRequestFilter(logging.Filter):
    """Filter out HTTP request logs to reduce noise"""

    def filter(self, record: logging.LogRecord) -> bool:
        return not record.getMessage().startswith("HTTP Request:")


class ShardContextFilter(logging.Filter):
    """Add shard context to all log records"""

    def __init__(self, shard_index: int, shard_count: int):
        super().__init__()
        self.shard_index = shard_index
        self.shard_count = shard_count

    def filter(self, record: logging.LogRecord) -> bool:
        record.shard_idx = self.shard_index
        record.shard_cnt = self.shard_count
        return True


# ============================================================================
# Demper Worker Class
# ============================================================================

class DemperWorker:
    """
    Main demper worker class that processes products and updates prices.

    The worker continuously monitors products assigned to its shard and
    adjusts prices based on competitor prices while respecting profit margins.
    """

    def __init__(
        self,
        instance_index: Optional[int] = None,
        instance_count: Optional[int] = None,
        max_concurrent_tasks: Optional[int] = None,
        check_interval: int = 5
    ):
        """
        Initialize demper worker.

        Args:
            instance_index: Shard index (0 to instance_count-1), defaults to settings.instance_index
            instance_count: Total number of shards, defaults to settings.instance_count
            max_concurrent_tasks: Max concurrent product processing, defaults to settings.max_concurrent_tasks
            check_interval: Seconds between check cycles, defaults to 5
        """
        self.instance_index = instance_index if instance_index is not None else settings.instance_index
        self.instance_count = instance_count if instance_count is not None else settings.instance_count
        self.max_concurrent_tasks = max_concurrent_tasks if max_concurrent_tasks is not None else settings.max_concurrent_tasks
        self.check_interval = check_interval

        # Validate shard configuration
        if self.instance_index >= self.instance_count:
            raise ValueError(
                f"instance_index ({self.instance_index}) must be less than "
                f"instance_count ({self.instance_count})"
            )

        # Concurrency control
        self.semaphore = asyncio.Semaphore(self.max_concurrent_tasks)

        # Worker state
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Setup logging with shard context
        self._setup_logging()

        logger.info(
            f"Demper worker initialized: shard {self.instance_index}/{self.instance_count}, "
            f"max_concurrent={self.max_concurrent_tasks}"
        )

    def _setup_logging(self):
        """Configure logging with shard context"""
        # Add custom filters
        shard_filter = ShardContextFilter(self.instance_index, self.instance_count)

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.addFilter(NoHttpRequestFilter())

        # Configure demper logger
        demper_logger = logging.getLogger("app.workers.demper_instance")
        demper_logger.addFilter(shard_filter)
        demper_logger.setLevel(logging.INFO)

    async def start(self):
        """Start the demper worker main loop"""
        if self._running:
            logger.warning("Worker already running")
            return

        self._running = True
        logger.info("Starting demper worker...")

        try:
            # Initialize infrastructure
            await self._initialize()

            # Run main loop
            await self._main_loop()

        except asyncio.CancelledError:
            logger.info("Worker cancelled")
        except Exception as e:
            logger.error(f"Worker error: {e}", exc_info=True)
            raise
        finally:
            await self._shutdown()

    async def stop(self):
        """Stop the demper worker gracefully"""
        logger.info("Stopping demper worker...")
        self._running = False
        self._shutdown_event.set()

    async def _initialize(self):
        """Initialize database and browser farm"""
        logger.info("Initializing infrastructure...")

        # Initialize database pool
        await get_db_pool()
        logger.info("Database pool initialized")

        # Initialize browser farm
        await get_browser_farm()
        logger.info("Browser farm initialized")

        # Initialize rate limiter
        get_global_rate_limiter()
        logger.info(f"Rate limiter initialized: {settings.global_rps} RPS")

    async def _shutdown(self):
        """Clean shutdown of resources"""
        logger.info("Shutting down worker...")

        # Close browser farm
        await close_browser_farm()
        logger.info("Browser farm closed")

        # Close database pool
        await close_pool()
        logger.info("Database pool closed")

        self._running = False
        logger.info("Worker shutdown complete")

    async def _main_loop(self):
        """Main processing loop"""
        cycle_count = 0

        while self._running:
            try:
                cycle_count += 1
                logger.info(f"Starting demper cycle #{cycle_count}")
                cycle_start = time.time()

                # Fetch products for this shard
                products = await self.fetch_products_for_instance()
                logger.info(f"Fetched {len(products)} active products for shard {self.instance_index}")

                if not products:
                    logger.info("No products to process in this cycle")
                else:
                    # Sync store sessions before processing
                    await self.sync_store_sessions()

                    # Process products concurrently
                    tasks = [
                        asyncio.create_task(self.process_product(product))
                        for product in products
                    ]

                    if tasks:
                        # Wait for all tasks to complete
                        results = await asyncio.gather(*tasks, return_exceptions=True)

                        # Count results by category
                        updated = sum(1 for r in results if r is True)
                        skipped = sum(1 for r in results if r is False)
                        errors = sum(1 for r in results if isinstance(r, Exception))

                        # Log errors for debugging
                        for r in results:
                            if isinstance(r, Exception):
                                logger.error(f"Product processing error: {r}")

                        logger.info(
                            f"Cycle #{cycle_count}: {updated} updated, "
                            f"{skipped} skipped, {errors} errors"
                        )

                cycle_duration = time.time() - cycle_start
                logger.info(
                    f"Cycle #{cycle_count} took {cycle_duration:.2f}s "
                    f"({len(products)} products)"
                )

            except Exception as e:
                logger.error(f"Error in main loop cycle #{cycle_count}: {e}", exc_info=True)

            # Wait before next cycle (or until shutdown)
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.check_interval
                )
                # If we get here, shutdown was requested
                break
            except asyncio.TimeoutError:
                # Normal timeout, continue to next cycle
                pass

    async def fetch_products_for_instance(self) -> List[Dict[str, Any]]:
        """
        Fetch products assigned to this worker instance.

        Uses hash-based sharding: mod(abs(hashtext(id::text)), INSTANCE_COUNT) = INSTANCE_INDEX

        Filters:
        - Only products with bot_active = true
        - Only stores that are active and don't need re-auth
        - Only within store's working hours
        - Only products that haven't been checked within check_interval_minutes

        Returns:
            List of product records
        """
        pool = await get_db_pool()

        try:
            async with pool.acquire() as conn:
                # Get current time in Almaty timezone (UTC+5 for Kazakhstan)
                # Note: You might want to use proper timezone handling
                query = """
                    SELECT
                        products.id,
                        products.store_id,
                        products.kaspi_product_id,
                        products.kaspi_sku,
                        products.external_kaspi_id,
                        products.price,
                        products.min_profit,
                        products.min_price,
                        products.max_price,
                        products.price_step_override,
                        products.demping_strategy,
                        products.strategy_params,
                        kaspi_stores.merchant_id,
                        kaspi_stores.guid,
                        COALESCE(ds.check_interval_minutes, 15) as check_interval_minutes,
                        COALESCE(ds.work_hours_start, '00:00') as work_hours_start,
                        COALESCE(ds.work_hours_end, '23:59') as work_hours_end,
                        COALESCE(ds.price_step, 1) as store_price_step,
                        COALESCE(ds.is_enabled, true) as demping_enabled,
                        COALESCE(ds.excluded_merchant_ids, '{}') as excluded_merchant_ids
                    FROM products
                    JOIN kaspi_stores ON kaspi_stores.id = products.store_id
                    LEFT JOIN demping_settings ds ON ds.store_id = products.store_id
                    WHERE products.bot_active = TRUE
                      AND kaspi_stores.is_active = TRUE
                      AND kaspi_stores.guid IS NOT NULL
                      AND products.external_kaspi_id IS NOT NULL
                      AND COALESCE(kaspi_stores.needs_reauth, false) = FALSE
                      AND COALESCE(ds.is_enabled, true) = TRUE
                      -- Check if within working hours (Kazakhstan time, UTC+5)
                      -- Default to 24/7 if not set (00:00-23:59)
                      AND (
                          COALESCE(ds.work_hours_start, '00:00')::time <= (NOW() AT TIME ZONE 'Asia/Almaty')::time
                          AND COALESCE(ds.work_hours_end, '23:59')::time >= (NOW() AT TIME ZONE 'Asia/Almaty')::time
                      )
                      -- Check if enough time passed since last check
                      AND (
                          products.last_check_time IS NULL
                          OR products.last_check_time < NOW() - (COALESCE(ds.check_interval_minutes, 15) || ' minutes')::interval
                      )
                      AND mod(abs(hashtext(products.id::text)), $1) = $2
                    ORDER BY products.last_check_time ASC NULLS FIRST
                    LIMIT 500
                """

                rows = await conn.fetch(query, self.instance_count, self.instance_index)

                if rows:
                    logger.info(
                        f"Found {len(rows)} products ready for checking "
                        f"(shard {self.instance_index}/{self.instance_count})"
                    )

                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"Error fetching products: {e}", exc_info=True)
            return []

    async def sync_store_sessions(self):
        """
        Refresh expired store sessions.

        Checks all stores and refreshes sessions that are no longer valid.
        Only runs on leader instance (index 0) or on shard mode.
        """
        # Determine if this instance should sync stores
        if settings.sync_stores_mode == "leader" and self.instance_index != 0:
            return  # Only leader syncs

        pool = await get_db_pool()

        try:
            async with pool.acquire() as conn:
                # Get all active stores
                if settings.sync_stores_mode == "shard":
                    # Each shard handles its own stores (hash-based distribution)
                    query = """
                        SELECT id, merchant_id, guid
                        FROM kaspi_stores
                        WHERE is_active = TRUE
                          AND guid IS NOT NULL
                          AND mod(abs(hashtext(id::text)), $1) = $2
                    """
                    rows = await conn.fetch(query, self.instance_count, self.instance_index)
                else:
                    # Leader mode - handle all stores
                    query = """
                        SELECT id, merchant_id, guid
                        FROM kaspi_stores
                        WHERE is_active = TRUE
                          AND guid IS NOT NULL
                    """
                    rows = await conn.fetch(query)

                logger.info(f"Checking {len(rows)} store sessions")

                # TODO: Implement session validation and refresh
                # This is a placeholder for future implementation
                # Each store's session should be validated and refreshed if needed

        except Exception as e:
            logger.error(f"Error syncing store sessions: {e}", exc_info=True)

    async def process_product(self, product: Dict[str, Any]) -> bool:
        """
        Process a single product and update its price if needed.

        Algorithm:
        1. Fetch competitor prices for the product SKU
        2. Find minimum competitor price
        3. Calculate target price based on strategy
        4. Apply price constraints (min/max)
        5. If price change needed, update via Kaspi API
        6. Record price change to price_history table
        7. Update last_check_time

        Args:
            product: Product record with all necessary fields

        Returns:
            True if successful, False otherwise
        """
        async with self.semaphore:
            product_id = product["id"]
            sku = product["kaspi_sku"]
            external_id = product["external_kaspi_id"]
            current_price = Decimal(str(product["price"]))
            merchant_id = product["merchant_id"]

            # Get price constraints from product-level or store-level settings
            min_price = Decimal(str(product.get("min_price") or product.get("min_profit") or 0))
            max_price = product.get("max_price")
            if max_price:
                max_price = Decimal(str(max_price))

            # Get price step (product override or store default)
            price_step = Decimal(str(product.get("price_step_override") or product.get("store_price_step") or 1))

            # Get strategy
            strategy = product.get("demping_strategy") or "standard"
            strategy_params = product.get("strategy_params") or {}

            # Get excluded merchant IDs (own stores that should not be considered as competitors)
            excluded_merchant_ids = set(product.get("excluded_merchant_ids") or [])
            # Always exclude our own merchant_id
            excluded_merchant_ids.add(merchant_id)

            try:
                # Small random delay to avoid synchronized bursts
                await asyncio.sleep(random.uniform(0.01, 0.1))

                # Get session for this store (skip validation to avoid rate limiting)
                session = await get_active_session_with_refresh(merchant_id, skip_validation=True)
                if not session:
                    logger.warning(f"[{sku}] No active session for merchant {merchant_id}")
                    return False
                logger.debug(f"[{sku}] Got session for merchant {merchant_id}")

                # Fetch competitor prices
                product_data = await parse_product_by_sku(str(external_id), session)

                # Update last_check_time regardless of result
                await self._update_last_check_time(product_id)

                if not product_data:
                    logger.debug(f"No competitor data for product {sku}")
                    return False

                # Extract offers from response
                offers = product_data.get("offers", []) if isinstance(product_data, dict) else product_data

                if not offers or len(offers) == 0:
                    logger.debug(f"No offers found for product {sku}")
                    return False

                # Log offers for debugging (only for specific merchant)
                if merchant_id != '30391544' and len(offers) > 0:
                    logger.info(f"Found {len(offers)} offers for SKU {sku} (merchant {merchant_id})")

                # Sort offers by price and find our position
                # Mark offers as excluded if they belong to excluded_merchant_ids
                sorted_offers = []
                our_price = None
                our_position = None

                for offer in offers:
                    offer_merchant_id = offer.get("merchantId")
                    offer_price = offer.get("price")
                    if offer_price is not None:
                        is_ours = offer_merchant_id == merchant_id
                        is_excluded = offer_merchant_id in excluded_merchant_ids
                        sorted_offers.append({
                            "merchant_id": offer_merchant_id,
                            "price": Decimal(str(offer_price)),
                            "is_ours": is_ours,
                            "is_excluded": is_excluded  # Own stores or excluded merchants
                        })
                        if is_ours:
                            our_price = Decimal(str(offer_price))

                sorted_offers.sort(key=lambda x: x["price"])

                # Find our position among ALL offers (including excluded)
                for i, offer in enumerate(sorted_offers):
                    if offer["is_ours"]:
                        our_position = i + 1
                        break

                # Find minimum competitor price (excluding our own and excluded merchants)
                min_competitor_price = None
                for offer in sorted_offers:
                    if not offer["is_excluded"]:
                        min_competitor_price = offer["price"]
                        break

                if min_competitor_price is None:
                    logger.debug(f"No competitor offers for product {sku} (all offers are from excluded merchants)")
                    return False

                # Calculate target price based on strategy
                target_price = self._calculate_target_price(
                    strategy=strategy,
                    strategy_params=strategy_params,
                    current_price=current_price,
                    min_competitor_price=min_competitor_price,
                    sorted_offers=sorted_offers,
                    our_position=our_position,
                    price_step=price_step,
                    merchant_id=merchant_id
                )

                if target_price is None:
                    logger.debug(f"No target price calculated for {sku}")
                    return False

                # Apply price constraints
                # Kaspi не позволяет выставлять цену ниже 10 тенге
                KASPI_MIN_PRICE = Decimal("10")
                effective_min_price = max(min_price, KASPI_MIN_PRICE) if min_price > 0 else KASPI_MIN_PRICE

                if target_price < effective_min_price:
                    # Конкурент ниже нашего минимума - ждём, не меняем цену
                    # Но если мы сами выше min_price, остаёмся на min_price
                    if current_price > effective_min_price:
                        target_price = effective_min_price
                        logger.debug(
                            f"Target price for {sku} adjusted to min_price: {target_price} "
                            f"(competitor below our min)"
                        )
                    else:
                        # Мы уже на минимуме или ниже, ждём повышения конкурента
                        logger.debug(
                            f"Competitor price for {sku} is below our min_price, waiting..."
                        )
                        return False

                if max_price and target_price > max_price:
                    target_price = max_price
                    logger.debug(
                        f"Target price for {sku} capped at max_price: {target_price}"
                    )

                # Only update if price changed
                if target_price == current_price:
                    logger.debug(f"No price change needed for {sku}: already at {current_price}")
                    return False

                # Update price via Kaspi API
                sync_result = await sync_product(
                    product_id=str(product_id),
                    new_price=int(target_price),
                    session=session
                )

                if not sync_result or not sync_result.get("success"):
                    logger.error(f"[{sku}] Failed to sync price: {sync_result}")
                    return False

                logger.info(f"[{sku}] sync_product success")

                # Explicitly update price in DB (backup in case sync_product didn't)
                await self._update_product_price(product_id, int(target_price))

                # Record price change to history
                await self._record_price_change(
                    product_id=product_id,
                    old_price=int(current_price),
                    new_price=int(target_price),
                    competitor_price=int(min_competitor_price),
                    change_reason=f"demper_{strategy}"
                )

                logger.info(
                    f"✓ Demper [{strategy}]: Updated {sku} from {current_price} to {target_price} "
                    f"(competitor: {min_competitor_price})"
                )

                return True

            except Exception as e:
                logger.error(f"Error processing product {sku}: {e}", exc_info=True)
                return False
            finally:
                # Random delay between product processing
                await asyncio.sleep(random.uniform(0.1, 0.3))

    def _calculate_target_price(
        self,
        strategy: str,
        strategy_params: dict,
        current_price: Decimal,
        min_competitor_price: Decimal,
        sorted_offers: List[Dict],
        our_position: Optional[int],
        price_step: Decimal,
        merchant_id: str
    ) -> Optional[Decimal]:
        """
        Calculate target price based on strategy.

        Логика работы:
        - Цена всегда идёт на заданный шаг ниже конкурента
        - Если конкурент поднял цену - мы тоже поднимаем (но не выше max_price - проверяется в process_product)
        - Если конкурент опустил цену ниже нашего min_price - мы ждём (не меняем цену)

        Strategies:
        - standard: Beat minimum competitor by price_step
        - always_first: Always be the cheapest (min_competitor - price_step)
        - stay_top_n: Stay within top N positions

        Args:
            strategy: Strategy name
            strategy_params: Strategy-specific parameters
            current_price: Current product price
            min_competitor_price: Minimum competitor price
            sorted_offers: All offers sorted by price
            our_position: Our current position (1-indexed)
            price_step: Price adjustment step
            merchant_id: Our merchant ID

        Returns:
            Target price or None if no change needed
        """
        if strategy == "standard" or strategy == "always_first":
            # Цена = минимальная цена конкурента - шаг
            # Это работает и для снижения, и для повышения цены
            target = min_competitor_price - price_step
            return target

        elif strategy == "stay_top_n":
            # Stay within top N positions
            top_n = strategy_params.get("top_position", 3)

            # Find price of N-th position competitor
            competitor_count = 0
            target_price = None

            for offer in sorted_offers:
                if not offer["is_ours"]:
                    competitor_count += 1
                    if competitor_count == top_n:
                        # Match this price (or go slightly below)
                        target_price = offer["price"] - price_step
                        break

            if target_price is None and competitor_count > 0:
                # Fewer than N competitors, match the last one
                for offer in reversed(sorted_offers):
                    if not offer["is_ours"]:
                        target_price = offer["price"] - price_step
                        break

            return target_price

        else:
            logger.warning(f"Unknown strategy: {strategy}, using standard")
            return min_competitor_price - price_step

    async def _update_last_check_time(self, product_id: UUID):
        """Update last_check_time for a product."""
        pool = await get_db_pool()

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE products SET last_check_time = NOW() WHERE id = $1",
                    product_id
                )
        except Exception as e:
            logger.error(f"Error updating last_check_time: {e}", exc_info=True)

    async def _update_product_price(self, product_id: UUID, new_price: int):
        """
        Update product price in database.

        This is a backup update in case sync_product didn't update the DB.

        Args:
            product_id: Product UUID
            new_price: New price in tenge (KZT)
        """
        pool = await get_db_pool()

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE products SET price = $1, updated_at = NOW() WHERE id = $2",
                    new_price,
                    product_id
                )
        except Exception as e:
            logger.error(f"Error updating product price: {e}", exc_info=True)

    async def _record_price_change(
        self,
        product_id: UUID,
        old_price: int,
        new_price: int,
        competitor_price: Optional[int],
        change_reason: str
    ):
        """
        Record price change to price_history table.

        Args:
            product_id: Product UUID
            old_price: Previous price (в тенге KZT)
            new_price: New price (в тенге KZT)
            competitor_price: Competitor price that triggered change (в тенге KZT)
            change_reason: Reason for change (e.g., "demper", "manual")
        """
        pool = await get_db_pool()

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO price_history (
                        id, product_id, old_price, new_price,
                        competitor_price, change_reason, created_at
                    )
                    VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, NOW())
                    """,
                    product_id,
                    old_price,
                    new_price,
                    competitor_price,
                    change_reason
                )
                logger.info(f"Recorded price history: {old_price} → {new_price} (reason: {change_reason})")
        except Exception as e:
            logger.error(f"Error recording price change for product {product_id}: {e}", exc_info=True)


# ============================================================================
# Standalone Entry Point
# ============================================================================

async def main():
    """
    Main entry point for running demper worker as standalone process.

    Handles graceful shutdown on SIGTERM/SIGINT.
    """
    # Setup signal handlers for graceful shutdown
    worker: Optional[DemperWorker] = None
    shutdown_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")
        shutdown_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Create and start worker
        worker = DemperWorker()
        logger.info(
            f"Starting Demper Worker: shard {worker.instance_index}/{worker.instance_count}"
        )

        # Run worker in background
        worker_task = asyncio.create_task(worker.start())

        # Wait for shutdown signal
        await shutdown_event.wait()

        # Stop worker gracefully
        if worker:
            await worker.stop()

        # Wait for worker to finish
        await worker_task

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Demper worker stopped")


if __name__ == "__main__":
    # Configure logging for standalone mode
    # Use simple format without shard info (filter adds it to relevant records)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )

    # Also create logs directory and file handler if possible
    import os
    os.makedirs("logs", exist_ok=True)
    try:
        file_handler = logging.FileHandler("logs/demper_worker.log", encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        logging.getLogger().addHandler(file_handler)
    except Exception:
        pass  # Ignore if can't create log file

    # Run worker
    asyncio.run(main())
