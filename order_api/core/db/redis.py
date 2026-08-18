from redis.asyncio import Redis

from order_api.core.config import get_settings

settings = get_settings()

# A single shared client, not one per request — redis.asyncio.Redis already
# pools connections internally, so this is meant to be reused, not recreated.
redis_client: Redis = Redis.from_url(settings.redis_url, decode_responses=True)


async def get_redis() -> Redis:
    """FastAPI dependency: `redis: Redis = Depends(get_redis)`."""
    return redis_client
