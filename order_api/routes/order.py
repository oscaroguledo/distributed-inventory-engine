from fastapi import APIRouter, Depends, Response

from order_api.core.rate_limiter import enforce_rate_limit
from order_api.core.response import APIResponse, EResponse, SResponse
from order_api.schemas.order import CommitRequest, ReleaseRequest, ReserveRequest
from order_api.services.order import (
    HoldNotFoundError,
    InsufficientStockError,
    OrderService,
    SkuNotFoundError,
    get_order_service,
)

router = APIRouter()


@router.post(
    "/reserve", response_model=APIResponse[dict], dependencies=[Depends(enforce_rate_limit)]
)
async def reserve(
    payload: ReserveRequest,
    response: Response,
    order_service: OrderService = Depends(get_order_service),
) -> APIResponse:
    try:
        reservation = await order_service.reserve(
            sku=payload.sku, quantity=payload.quantity, reservation_id=payload.reservation_id
        )
    except SkuNotFoundError as exc:
        response.status_code = 404
        return EResponse(message=str(exc), status=404)
    except InsufficientStockError as exc:
        response.status_code = 409
        return EResponse(message=str(exc), status=409)

    response.status_code = 201
    return SResponse(data=reservation.to_dict(), message="Reservation held", status=201)


@router.post(
    "/commit", response_model=APIResponse[dict], dependencies=[Depends(enforce_rate_limit)]
)
async def commit(
    payload: CommitRequest,
    response: Response,
    order_service: OrderService = Depends(get_order_service),
) -> APIResponse:
    try:
        result = await order_service.commit(reservation_id=payload.reservation_id)
    except HoldNotFoundError as exc:
        response.status_code = 404
        return EResponse(message=str(exc), status=404)

    return SResponse(data=result.to_dict(), message="Order confirmed", status=200)


@router.post("/release", response_model=APIResponse[dict])
async def release(
    payload: ReleaseRequest,
    response: Response,
    order_service: OrderService = Depends(get_order_service),
) -> APIResponse:
    try:
        pass
    except Exception as exc:
        response.status_code = 400
        return EResponse(message=str(exc), status=400)
    response.status_code = 201
    return SResponse(data=release.to_dict(), message="Reservation cancelled", status=200)
