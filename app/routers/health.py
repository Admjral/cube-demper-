"""Health and monitoring endpoints"""

from fastapi import APIRouter, Depends
from typing import Annotated
import asyncpg

from ..core.database import get_db_pool
from ..models.proxy import ProxyPoolStatus
from ..dependencies import get_current_user

router = APIRouter()


@router.get("/proxies", response_model=ProxyPoolStatus)
async def get_proxy_pool_status(
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    current_user: Annotated[dict, Depends(get_current_user)] = None  # Optional auth
):
    """
    Get proxy pool status

    Returns overall proxy pool health and statistics.
    """
    async with pool.acquire() as conn:
        # Get overall proxy counts
        overall = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE status = 'available' AND user_id IS NULL) as available,
                COUNT(*) FILTER (WHERE status = 'allocated') as allocated,
                COUNT(*) FILTER (WHERE status = 'resting') as resting,
                COUNT(*) FILTER (WHERE status = 'dead') as dead,
                COUNT(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL) as users_with_proxies
            FROM proxies
            """
        )

        # Get per-module allocation breakdown
        module_stats = await conn.fetch(
            """
            SELECT
                module,
                COUNT(*) as count
            FROM proxies
            WHERE status IN ('allocated', 'resting')
              AND user_id IS NOT NULL
            GROUP BY module
            """
        )

        # Calculate per-module allocation
        per_module_allocation = {}
        for row in module_stats:
            module = row['module'] or 'unassigned'
            per_module_allocation[module] = row['count']

        # Calculate average proxies per user
        total = overall['total']
        users_count = overall['users_with_proxies'] or 1  # Avoid division by zero
        avg_per_user = (overall['allocated'] + overall['resting']) / users_count if users_count > 0 else 0

    return ProxyPoolStatus(
        total=total,
        available=overall['available'],
        allocated=overall['allocated'],
        resting=overall['resting'],
        dead=overall['dead'],
        users_with_proxies=overall['users_with_proxies'],
        average_proxies_per_user=round(avg_per_user, 2),
        per_module_allocation=per_module_allocation
    )


@router.get("/proxies/user/{user_id}")
async def get_user_proxy_status(
    user_id: str,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    current_user: Annotated[dict, Depends(get_current_user)]
):
    """
    Get proxy status for a specific user

    Returns detailed proxy allocation and health for a user.
    """
    # Verify user can only access their own proxy stats (unless admin)
    if current_user['id'] != user_id:
        # TODO: Add admin check
        from fastapi import HTTPException, status
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Can only view your own proxy status"
        )

    async with pool.acquire() as conn:
        proxies = await conn.fetch(
            """
            SELECT
                id,
                module,
                status,
                requests_count,
                success_count,
                failure_count,
                last_used_at,
                available_at,
                created_at
            FROM proxies
            WHERE user_id = $1
            ORDER BY module, status, created_at
            """,
            user_id
        )

        # Group by module
        by_module = {}
        for proxy in proxies:
            module = proxy['module'] or 'unassigned'
            if module not in by_module:
                by_module[module] = {
                    'total': 0,
                    'allocated': 0,
                    'resting': 0,
                    'dead': 0,
                    'total_requests': 0,
                    'total_success': 0,
                    'total_failures': 0
                }

            by_module[module]['total'] += 1
            by_module[module][proxy['status']] += 1
            by_module[module]['total_requests'] += proxy['requests_count'] or 0
            by_module[module]['total_success'] += proxy['success_count'] or 0
            by_module[module]['total_failures'] += proxy['failure_count'] or 0

        # Calculate success rates
        for module_stats in by_module.values():
            total_req = module_stats['total_requests']
            if total_req > 0:
                module_stats['success_rate'] = round(
                    module_stats['total_success'] / total_req * 100, 2
                )
            else:
                module_stats['success_rate'] = 0.0

    return {
        'user_id': user_id,
        'total_proxies': len(proxies),
        'by_module': by_module
    }
