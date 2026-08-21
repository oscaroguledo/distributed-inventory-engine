import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from order_api.core.config import get_settings
from order_api.core.db.postgresql import engine
from order_api.core.rate_limiter import RateLimitExceeded
from order_api.core.response import EResponse
from order_api.models import (  # noqa: F401
    inventory_balances,
    inventory_reservations,
    stock_audit_ledger,
)
from order_api.models.base import Base
from order_api.routes.health import router as health_router
from order_api.routes.order import router as order_router

# Configured at import time, not inside __main__ — uvicorn imports this
# module directly, so a __main__-guarded config would never run.
logging.basicConfig(level=get_settings().log_level)


@asynccontextmanager
async def lifespan(app: FastAPI):  # pragma: no cover -- real Postgres, verified live via Docker
    # No migration tool yet — create_all is idempotent and unblocks a fresh
    # `docker compose up` (and CI) against an empty Postgres.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title="Order API", lifespan=lifespan)
app.include_router(health_router)
app.include_router(order_router)


@app.exception_handler(RateLimitExceeded)
async def handle_rate_limit_exceeded(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    return JSONResponse(
        status_code=429,
        content=EResponse(message="Too Many Requests", status=429).model_dump(),
    )

if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "order_api.main:app", host="0.0.0.0", port=settings.order_api_port, reload=True
    )
