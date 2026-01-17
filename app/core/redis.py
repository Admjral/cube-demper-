import redis.asyncio as redis
from typing import Optional
import logging

from ..config import settings

logger = logging.getLogger(__name__)

# Global Redis client
_redis_client: Optional[redis.Redis] = None


async def create_redis_client() -> redis.Redis:
    """Create and return Redis client"""
    global _redis_client

    if _redis_client is not None:
        return _redis_client

    try:
        _redis_client = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=50,
        )
        # Test connection
        await _redis_client.ping()
        logger.info(f"Redis client connected: {settings.redis_host}:{settings.redis_port}")
        return _redis_client
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise


async def close_redis_client():
    """Close Redis connection"""
    global _redis_client

    if _redis_client is not None:
        await _redis_client.close()
        logger.info("Redis client closed")
        _redis_client = None


async def get_redis() -> redis.Redis:
    """Dependency for getting Redis client"""
    if _redis_client is None:
        await create_redis_client()
    return _redis_client


class RedisKeyspace:
    """Redis key namespaces for different data types"""

    # Proxy coordination
    PROXY_PREFIX = "proxy:"
    PROXY_LOCK = "proxy:lock:{key}"
    PROXY_USAGE = "proxy:usage:{key}"

    # Sessions
    SESSION_PREFIX = "session:"
    USER_SESSION = "session:user:{user_id}"

    # Rate limiting
    RATE_LIMIT_PREFIX = "ratelimit:"
    GLOBAL_RATE_LIMIT = "ratelimit:global"
    USER_RATE_LIMIT = "ratelimit:user:{user_id}"

    # Cache
    CACHE_PREFIX = "cache:"
    PRODUCT_CACHE = "cache:product:{sku}"
    STORE_CACHE = "cache:store:{store_id}"

    # WAHA container management
    WAHA_PREFIX = "waha:"
    WAHA_PORT_ALLOCATION = "waha:ports"
    WAHA_CONTAINER_STATUS = "waha:container:{user_id}"

    @staticmethod
    def proxy_lock(proxy_key: str) -> str:
        return f"proxy:lock:{proxy_key}"

    @staticmethod
    def proxy_usage(proxy_key: str) -> str:
        return f"proxy:usage:{proxy_key}"

    @staticmethod
    def user_session(user_id: str) -> str:
        return f"session:user:{user_id}"

    @staticmethod
    def user_rate_limit(user_id: str) -> str:
        return f"ratelimit:user:{user_id}"

    @staticmethod
    def product_cache(sku: str) -> str:
        return f"cache:product:{sku}"

    @staticmethod
    def store_cache(store_id: str) -> str:
        return f"cache:store:{store_id}"

    @staticmethod
    def waha_container_status(user_id: str) -> str:
        return f"waha:container:{user_id}"
