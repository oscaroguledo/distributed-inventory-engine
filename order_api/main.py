from fastapi import FastAPI

from order_api.core.config import get_settings
from order_api.routes.health import router as health_router
from order_api.routes.order import router as order_router

app = FastAPI(title="Order API")
app.include_router(health_router)
app.include_router(order_router)

if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "order_api.main:app", host="0.0.0.0", port=settings.order_api_port, reload=True
    )
