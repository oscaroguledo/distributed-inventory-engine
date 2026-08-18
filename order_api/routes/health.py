from fastapi import APIRouter

from order_api.core.response import APIResponse, SResponse

router = APIRouter()


@router.get("/health", response_model=APIResponse[dict])
async def healthz() -> APIResponse:
    return SResponse(data={"status": "ok"}, message="Service is healthy")
