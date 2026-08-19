from fastapi import APIRouter, Depends, Response

from order_api.core.response import APIResponse, EResponse, SResponse
from order_api.schemas.order import ReserveRequest
from order_api.services.order import (
    InsufficientStockError,
    OrderService,
    SkuNotFoundError,
    get_order_service,
)

router = APIRouter()


@router.post("/reserve", response_model=APIResponse[dict])
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
