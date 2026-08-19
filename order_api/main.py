from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from order_api.core.config import get_settings
from order_api.core.rate_limiter import RateLimitExceeded
from order_api.core.response import EResponse
from order_api.routes.health import router as health_router
from order_api.routes.order import router as order_router

app = FastAPI(title="Order API")
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
