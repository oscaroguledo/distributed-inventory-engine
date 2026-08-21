import logging
import time
from pathlib import Path

from fastapi import Depends, Request
from redis.asyncio import Redis

from order_api.core.config import get_settings
from order_api.core.db.redis import get_redis
from order_api.core.metrics import RATE_LIMIT_REJECTIONS

logger = logging.getLogger("order_api.rate_limiter")

_LUA_SCRIPT_PATH = Path(__file__).parent / "lua" / "rate_limit.lua"


class RateLimitExceeded(Exception):
    """Raised by enforce_rate_limit; a FastAPI exception handler turns this
    into a 429 through the same APIResponse envelope as every other error."""


class RateLimiter:
    """Atomic Redis token-bucket — see core/lua/rate_limit.lua. One Lua
    execution per check, so concurrent requests can't race past the limit."""

    def __init__(self, redis: Redis, capacity: int, refill_rate: float, ttl_seconds: int = 60):
        self.redis = redis
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.ttl_seconds = ttl_seconds
        self._script = redis.register_script(_LUA_SCRIPT_PATH.read_text())

    async def is_allowed(self, key: str, cost: int = 1) -> tuple[bool, float]:
        allowed, tokens = await self._script(
            keys=[f"ratelimit:{key}"],
            args=[self.capacity, self.refill_rate, cost, time.time(), self.ttl_seconds],
        )
        return bool(allowed), float(tokens)


def get_rate_limiter(redis: Redis = Depends(get_redis)) -> RateLimiter:
    settings = get_settings()
    return RateLimiter(
        redis=redis,
        capacity=settings.rate_limit_capacity,
        refill_rate=settings.rate_limit_refill_per_second,
        ttl_seconds=settings.rate_limit_ttl_seconds,
    )


async def enforce_rate_limit(
    request: Request, limiter: RateLimiter = Depends(get_rate_limiter)
) -> None:
    client_key = request.client.host if request.client else "unknown"
    allowed, tokens = await limiter.is_allowed(client_key)
    if not allowed:
        RATE_LIMIT_REJECTIONS.labels(path=request.url.path).inc()
        logger.warning(
            "rate limit exceeded: client=%s path=%s tokens_remaining=%.2f",
            client_key,
            request.url.path,
            tokens,
        )
        raise RateLimitExceeded()
