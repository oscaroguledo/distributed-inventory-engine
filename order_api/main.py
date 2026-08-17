from fastapi import FastAPI

from order_api.routes.health import router as health_router

app = FastAPI(title="Order API")
app.include_router(health_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("order_api.main:app", host="0.0.0.0", port=8000, reload=True)
