import pytest

from order_api.core.rate_limiter import RateLimiter, get_rate_limiter


class _FakeRedis:
    """Stands in for redis.asyncio.Redis so we test RateLimiter's own logic
    (script args, result parsing) without a live Redis."""

    def __init__(self, script_result):
        self._script_result = script_result
        self.calls: list[tuple[list, list]] = []

    def register_script(self, _script_body):
        async def _script(keys, args):
            self.calls.append((keys, args))
            return self._script_result

        return _script


@pytest.mark.asyncio
async def test_is_allowed_true_when_script_allows():
    fake_redis = _FakeRedis(script_result=[1, 5.0])
    limiter = RateLimiter(redis=fake_redis, capacity=10, refill_rate=1.0, ttl_seconds=60)

    allowed, tokens = await limiter.is_allowed("client-a")

    assert allowed is True
    assert tokens == 5.0

    keys, args = fake_redis.calls[0]
    assert keys == ["ratelimit:client-a"]
    assert args[0] == 10  # capacity
    assert args[1] == 1.0  # refill_rate
    assert args[2] == 1  # requested cost
    assert args[4] == 60  # ttl_seconds


@pytest.mark.asyncio
async def test_is_allowed_false_when_script_denies():
    fake_redis = _FakeRedis(script_result=[0, 0.0])
    limiter = RateLimiter(redis=fake_redis, capacity=10, refill_rate=1.0)

    allowed, tokens = await limiter.is_allowed("client-a")

    assert allowed is False
    assert tokens == 0.0


@pytest.mark.asyncio
async def test_is_allowed_uses_custom_cost():
    fake_redis = _FakeRedis(script_result=[1, 3.0])
    limiter = RateLimiter(redis=fake_redis, capacity=10, refill_rate=1.0)

    await limiter.is_allowed("client-a", cost=5)

    _, args = fake_redis.calls[0]
    assert args[2] == 5


def test_get_rate_limiter_builds_from_settings():
    fake_redis = _FakeRedis(script_result=[1, 1.0])

    limiter = get_rate_limiter(redis=fake_redis)

    assert isinstance(limiter, RateLimiter)
    assert limiter.capacity > 0
    assert limiter.refill_rate > 0
    assert limiter.ttl_seconds > 0
