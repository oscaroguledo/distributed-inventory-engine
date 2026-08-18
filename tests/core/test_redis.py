import pytest
from redis.asyncio import Redis

from order_api.core.db import redis as redis_module


def test_redis_url_configured():
    assert redis_module.settings.redis_url.startswith("redis://")


def test_redis_client_is_configured():
    assert isinstance(redis_module.redis_client, Redis)


@pytest.mark.asyncio
async def test_get_redis_returns_the_shared_client():
    client = await redis_module.get_redis()
    assert client is redis_module.redis_client
