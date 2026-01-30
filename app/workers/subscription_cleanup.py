"""
Subscription Cleanup Worker

Periodically checks for expired subscriptions and:
1. Deactivates expired subscriptions
2. Deallocates proxies from users with expired subscriptions
3. Clears proxy rotator cache

Runs every hour.
"""

import asyncio
import logging
from datetime import datetime, timezone

from ..core.database import get_db_pool, close_pool
from ..core.proxy_rotator import clear_rotator_cache
from ..services.proxy_allocator import proxy_allocator

logger = logging.getLogger(__name__)


async def cleanup_expired_subscriptions():
    """
    Find and cleanup expired subscriptions

    Returns:
        Number of subscriptions cleaned up
    """
    pool = await get_db_pool()
    cleaned_count = 0

    async with pool.acquire() as conn:
        # Find active subscriptions that have expired
        expired_subs = await conn.fetch(
            """
            SELECT id, user_id, plan
            FROM subscriptions
            WHERE status = 'active'
              AND current_period_end < NOW()
            """
        )

        if not expired_subs:
            logger.info("No expired subscriptions found")
            return 0

        logger.info(f"Found {len(expired_subs)} expired subscriptions")

        for sub in expired_subs:
            user_id = sub['user_id']
            sub_id = sub['id']
            plan = sub['plan']

            try:
                # Deactivate subscription
                await conn.execute(
                    """
                    UPDATE subscriptions
                    SET status = 'expired'
                    WHERE id = $1
                    """,
                    sub_id
                )

                # Deallocate proxies from user
                freed_count = await proxy_allocator.deallocate_proxies_from_user(user_id)

                # Clear rotator cache for this user
                await clear_rotator_cache(user_id=user_id)

                logger.info(
                    f"✅ Cleaned up expired subscription for user {user_id} "
                    f"(plan: {plan}): freed {freed_count} proxies"
                )

                cleaned_count += 1

            except Exception as e:
                logger.error(
                    f"Error cleaning up subscription {sub_id} for user {user_id}: {e}"
                )

    return cleaned_count


async def run_cleanup_worker():
    """
    Main worker loop - runs cleanup every hour
    """
    logger.info("Starting Subscription Cleanup Worker (interval: 1 hour)")

    while True:
        try:
            logger.info("Running subscription cleanup check...")
            cleaned = await cleanup_expired_subscriptions()

            if cleaned > 0:
                logger.info(f"Subscription cleanup completed: {cleaned} subscriptions processed")
            else:
                logger.debug("No expired subscriptions to cleanup")

        except Exception as e:
            logger.error(f"Error in subscription cleanup worker: {e}")

        # Wait 1 hour before next check
        await asyncio.sleep(3600)


async def main():
    """Entry point for running worker standalone"""
    try:
        await run_cleanup_worker()
    except KeyboardInterrupt:
        logger.info("Subscription cleanup worker stopped")
    finally:
        await close_pool()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())
