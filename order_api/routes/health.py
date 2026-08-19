import logging

from fastapi import APIRouter, Depends, Response
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from order_api.core.db.postgresql import get_session
from order_api.core.db.redis import get_redis
from order_api.core.response import APIResponse, EResponse, SResponse

logger = logging.getLogger("order_api.routes.health")

router = APIRouter()


@router.get("/health", response_model=APIResponse[dict])
async def healthz(
    response: Response,
    session: AsyncSession = Depends(get_session),
    redis: Redis = Depends(get_redis),
) -> APIResponse:
    checks = {"order-api": "ok", "postgres": "ok", "redis": "ok"}
    healthy = True

    try:
        await session.execute(text("SELECT 1"))
    except Exception:
        checks["postgres"] = "unreachable"
        healthy = False
        logger.warning("health check failed: postgres unreachable")

    try:
        await redis.ping()
    except Exception:
        checks["redis"] = "unreachable"
        healthy = False
        logger.warning("health check failed: redis unreachable")

    if healthy:
        return SResponse(data=checks, message="Service is healthy")

    response.status_code = 503
    return EResponse(data=checks, message="Service is unhealthy", status=503)
