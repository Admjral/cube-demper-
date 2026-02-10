"""
Periodic orders sync service - fetches active orders from Kaspi MC GraphQL
and saves them to the database.

Runs as a background task in the backend (not in workers).
Cycle: every 60 minutes, sequential processing with delays.

MC GraphQL only shows active orders (NEW, DELIVERY, PICKUP, SIGN_REQUIRED).
Over time, the database accumulates full order history as orders pass through
active states before completion.
"""
import logging
import asyncio
from datetime import datetime

import asyncpg

from .kaspi_mc_service import get_kaspi_mc_service, KaspiMCError
from .api_parser import sync_orders_to_db
from .kaspi_auth_service import KaspiAuthError

logger = logging.getLogger(__name__)

# Sync interval in seconds (60 minutes)
SYNC_INTERVAL = 3600

# Delay between processing each store (seconds)
STORE_DELAY = 2.0

# Initial delay before first sync (let the app fully start)
INITIAL_DELAY = 60


async def periodic_orders_sync(pool: asyncpg.Pool):
    """
    Background task that periodically syncs active orders from Kaspi MC.

    Runs indefinitely, syncing all active stores every SYNC_INTERVAL seconds.
    Each store is processed sequentially with delays to respect rate limits.
    """
    logger.info(f"[ORDERS_SYNC] Starting periodic orders sync (interval: {SYNC_INTERVAL}s)")

    # Wait for app to fully initialize
    await asyncio.sleep(INITIAL_DELAY)

    while True:
        try:
            await _run_sync_cycle(pool)
        except Exception as e:
            logger.error(f"[ORDERS_SYNC] Unexpected error in sync cycle: {e}")

        logger.info(f"[ORDERS_SYNC] Next cycle in {SYNC_INTERVAL}s")
        await asyncio.sleep(SYNC_INTERVAL)


async def _run_sync_cycle(pool: asyncpg.Pool):
    """Run one full sync cycle across all active stores."""
    cycle_start = datetime.utcnow()

    # Get all active stores with valid sessions
    async with pool.acquire() as conn:
        stores = await conn.fetch("""
            SELECT id, merchant_id, name
            FROM kaspi_stores
            WHERE is_active = TRUE
              AND needs_reauth = FALSE
              AND guid IS NOT NULL
              AND merchant_id IS NOT NULL
        """)

    if not stores:
        logger.info("[ORDERS_SYNC] No active stores to sync")
        return

    logger.info(f"[ORDERS_SYNC] Starting cycle for {len(stores)} stores")

    mc = get_kaspi_mc_service()
    total_synced = 0
    total_errors = 0

    sem = asyncio.Semaphore(5)

    async def _sync_one(store):
        nonlocal total_synced, total_errors

        store_id = str(store['id'])
        merchant_id = store['merchant_id']
        store_name = store['name'] or merchant_id

        async with sem:
            try:
                orders = await mc.fetch_orders_for_sync(
                    merchant_id=merchant_id,
                    limit=200,
                )

                if orders:
                    result = await sync_orders_to_db(store_id, orders)
                    synced = result.get('inserted', 0) + result.get('updated', 0)
                    total_synced += synced
                    logger.info(
                        f"[ORDERS_SYNC] {store_name}: {synced} orders "
                        f"({result.get('inserted', 0)} new, {result.get('updated', 0)} updated)"
                    )
                else:
                    logger.debug(f"[ORDERS_SYNC] {store_name}: no active orders")

            except KaspiMCError as e:
                total_errors += 1
                logger.warning(f"[ORDERS_SYNC] {store_name}: MC error - {e}")
            except KaspiAuthError as e:
                total_errors += 1
                logger.warning(f"[ORDERS_SYNC] {store_name}: auth error - {e}")
            except Exception as e:
                total_errors += 1
                logger.error(f"[ORDERS_SYNC] {store_name}: unexpected error - {e}")

            # Delay between stores to spread load
            await asyncio.sleep(STORE_DELAY)

    await asyncio.gather(*[_sync_one(s) for s in stores])

    elapsed = (datetime.utcnow() - cycle_start).total_seconds()
    logger.info(
        f"[ORDERS_SYNC] Cycle complete in {elapsed:.0f}s: "
        f"{len(stores)} stores, {total_synced} orders synced, {total_errors} errors"
    )
