import redis.asyncio as redis
from typing import Optional, List
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


# Default TTL values (seconds) for different key types
DEFAULT_TTL = 3600          # 1 hour (general fallback)
PROXY_TTL = 300             # 5 min
SESSION_TTL = 86400         # 24 hours
RATE_LIMIT_TTL = 120        # 2 min
CACHE_TTL = 600             # 10 min
WAHA_TTL = 3600             # 1 hour


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

    # User activity tracking
    USER_ACTIVITY_PREFIX = "user:activity:"
    USER_ACTIVITY = "user:activity:{user_id}"

    @staticmethod
    def user_activity(user_id: str) -> str:
        return f"user:activity:{user_id}"


async def set_user_activity(user_id: str, timestamp: Optional[float] = None) -> None:
    """Set user activity timestamp in Redis"""
    try:
        redis_client = await get_redis()
        key = RedisKeyspace.user_activity(user_id)
        if timestamp is None:
            import time
            timestamp = time.time()
        # Set with expiration of 10 minutes (600 seconds)
        await redis_client.setex(key, 600, str(timestamp))
    except Exception as e:
        logger.error(f"Failed to set user activity: {e}")


async def get_online_users(threshold_minutes: int = 5) -> List[str]:
    """Get list of online user IDs (active within threshold_minutes)"""
    try:
        redis_client = await get_redis()
        import time
        current_time = time.time()
        threshold_seconds = threshold_minutes * 60
        
        # Get all user activity keys
        pattern = RedisKeyspace.USER_ACTIVITY_PREFIX + "*"
        keys = []
        async for key in redis_client.scan_iter(match=pattern):
            keys.append(key)
        
        online_users = []
        for key in keys:
            try:
                timestamp_str = await redis_client.get(key)
                if timestamp_str:
                    timestamp = float(timestamp_str)
                    if current_time - timestamp <= threshold_seconds:
                        # Extract user_id from key (user:activity:{user_id})
                        user_id = key.replace(RedisKeyspace.USER_ACTIVITY_PREFIX, "")
                        online_users.append(user_id)
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid timestamp in key {key}: {e}")
                continue
        
        return online_users
    except Exception as e:
        logger.error(f"Failed to get online users: {e}")
        return []
