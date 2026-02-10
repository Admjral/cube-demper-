import asyncio
import asyncpg
from typing import Optional
import logging

from ..config import settings

logger = logging.getLogger(__name__)

# Global connection pool
_pool: Optional[asyncpg.Pool] = None


async def create_pool() -> asyncpg.Pool:
    """Create and return asyncpg connection pool"""
    global _pool

    if _pool is not None:
        return _pool

    try:
        _pool = await asyncio.wait_for(
            asyncpg.create_pool(
                host=settings.postgres_host,
                port=settings.postgres_port,
                database=settings.postgres_db,
                user=settings.postgres_user,
                password=settings.postgres_password,
                min_size=settings.db_pool_min_size,
                max_size=settings.db_pool_max_size,
                command_timeout=30,
            ),
            timeout=30.0  # 30 second timeout for pool creation
        )
        logger.info(
            f"Database pool created: {settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db} "
            f"(min={settings.db_pool_min_size}, max={settings.db_pool_max_size})"
        )
        return _pool
    except asyncio.TimeoutError:
        logger.error("Database pool creation timed out after 30 seconds")
        raise
    except Exception as e:
        logger.error(f"Failed to create database pool: {e}")
        raise


async def close_pool():
    """Close the database connection pool"""
    global _pool

    if _pool is not None:
        await _pool.close()
        logger.info("Database pool closed")
        _pool = None


async def get_db_pool() -> asyncpg.Pool:
    """Dependency for getting database pool"""
    if _pool is None:
        await create_pool()
    return _pool


async def execute_query(query: str, *args):
    """Execute a query and return results"""
    pool = await get_db_pool()
    async with pool.acquire() as connection:
        return await connection.fetch(query, *args)


async def execute_one(query: str, *args):
    """Execute a query and return single result"""
    pool = await get_db_pool()
    async with pool.acquire() as connection:
        return await connection.fetchrow(query, *args)


async def execute_command(query: str, *args):
    """Execute a command (INSERT, UPDATE, DELETE) without returning results"""
    pool = await get_db_pool()
    async with pool.acquire() as connection:
        return await connection.execute(query, *args)
