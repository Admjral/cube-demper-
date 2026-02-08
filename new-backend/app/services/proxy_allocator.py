"""
Proxy Allocator - Manages proxy allocation to users

Automatically allocates 100 proxies per paying user with module distribution:
- 70 proxies → demper (price demping worker)
- 25 proxies → orders (sales/orders worker)
- 5 proxies → catalog (catalog synchronization)
- 0 proxies → reserve (uses catalog proxies when needed)
"""

import logging
from typing import Dict, List
from uuid import UUID

from ..core.database import get_db_pool
from ..models.proxy import Proxy

logger = logging.getLogger(__name__)


class InsufficientProxiesError(Exception):
    """Raised when not enough proxies available in pool"""
    pass


class ProxyAllocator:
    """Handles proxy allocation to users"""

    DEFAULT_DISTRIBUTION = {
        'demper': 70,    # Main workload: 1000 req/3min (500 products × 2)
        'orders': 25,    # Orders: up to 1000 req/10min (200-500 orders × 2)
        'catalog': 5,    # Catalog sync: ~20 req rarely
        'reserve': 0     # Uses catalog proxies when needed
    }

    async def allocate_proxies_to_user(
        self,
        user_id: UUID,
        count: int = 100,
        distribution: Dict[str, int] = None
    ) -> Dict[str, List[Proxy]]:
        """
        Allocate N proxies to user with per-module distribution

        Args:
            user_id: User UUID
            count: Total proxies to allocate (default 100)
            distribution: Module distribution dict. Defaults to 70/25/5/0

        Returns:
            Dict mapping module name → list of allocated proxies

        Raises:
            InsufficientProxiesError: Not enough proxies in pool
        """
        if distribution is None:
            distribution = self.DEFAULT_DISTRIBUTION.copy()

        # Validate distribution
        total_needed = sum(distribution.values())
        if total_needed != count:
            raise ValueError(
                f"Distribution sum {total_needed} != requested count {count}"
            )

        logger.info(
            f"Allocating {count} proxies to user {user_id} with distribution: {distribution}"
        )

        pool = await get_db_pool()
        result = {}

        async with pool.acquire() as conn:
            # Allocate proxies for each module
            for module, module_count in distribution.items():
                if module_count == 0:
                    result[module] = []
                    continue

                # Select and allocate available proxies
                query = """
                    UPDATE proxies
                    SET user_id = $1,
                        status = 'allocated',
                        module = $2,
                        allocated_at = NOW()
                    WHERE id IN (
                        SELECT id FROM proxies
                        WHERE status = 'available'
                          AND user_id IS NULL
                        ORDER BY created_at ASC
                        LIMIT $3
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING *
                """
                rows = await conn.fetch(query, user_id, module, module_count)

                if len(rows) < module_count:
                    # Insufficient proxies - rollback by deallocating what we just allocated
                    await self.deallocate_proxies_from_user(user_id)

                    shortage = module_count - len(rows)
                    raise InsufficientProxiesError(
                        f"Only {len(rows)}/{module_count} proxies available for module '{module}'. "
                        f"Need to purchase {shortage} more proxies."
                    )

                proxies = [Proxy(**dict(row)) for row in rows]
                result[module] = proxies

                logger.info(
                    f"Allocated {len(proxies)} proxies to user {user_id} for module '{module}'"
                )

        total_allocated = sum(len(proxies) for proxies in result.values())
        logger.info(
            f"Successfully allocated {total_allocated} proxies to user {user_id}"
        )

        return result

    async def deallocate_proxies_from_user(self, user_id: UUID) -> int:
        """
        Free all proxies from user (when subscription expires)

        Args:
            user_id: User UUID

        Returns:
            Number of proxies freed
        """
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE proxies
                SET user_id = NULL,
                    status = 'available',
                    module = NULL,
                    allocated_at = NULL,
                    requests_count = 0,
                    available_at = NULL,
                    success_count = 0,
                    failure_count = 0,
                    last_used_at = NULL,
                    last_error = NULL
                WHERE user_id = $1
                """,
                user_id
            )

            # Parse "UPDATE N" result
            count = int(result.split()[-1]) if result else 0

        logger.info(f"Deallocated {count} proxies from user {user_id}")
        return count

    async def get_user_proxy_stats(self, user_id: UUID) -> Dict[str, any]:
        """
        Get proxy allocation statistics for user

        Args:
            user_id: User UUID

        Returns:
            Dict with stats per module
        """
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            query = """
                SELECT
                    module,
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'allocated') as allocated,
                    COUNT(*) FILTER (WHERE status = 'resting') as resting,
                    COUNT(*) FILTER (WHERE status = 'dead') as dead,
                    AVG(success_count) as avg_success,
                    AVG(failure_count) as avg_failure
                FROM proxies
                WHERE user_id = $1
                GROUP BY module
            """
            rows = await conn.fetch(query, user_id)

            stats = {}
            for row in rows:
                module = row['module'] or 'unassigned'
                total_requests = (row['avg_success'] or 0) + (row['avg_failure'] or 0)
                success_rate = (
                    (row['avg_success'] or 0) / total_requests
                    if total_requests > 0 else 0
                )

                stats[module] = {
                    'total': row['total'],
                    'allocated': row['allocated'],
                    'resting': row['resting'],
                    'dead': row['dead'],
                    'success_rate': success_rate
                }

        return stats

    async def check_pool_availability(self, required_count: int = 100) -> Dict[str, any]:
        """
        Check if enough proxies available in pool

        Args:
            required_count: Number of proxies needed

        Returns:
            Dict with availability status
        """
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            result = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'available' AND user_id IS NULL) as available,
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'allocated') as allocated,
                    COUNT(*) FILTER (WHERE status = 'dead') as dead
                FROM proxies
                """
            )

            available = result['available']
            sufficient = available >= required_count

            return {
                'available': available,
                'total': result['total'],
                'allocated': result['allocated'],
                'dead': result['dead'],
                'required': required_count,
                'sufficient': sufficient,
                'shortage': max(0, required_count - available)
            }


# Singleton instance
proxy_allocator = ProxyAllocator()
