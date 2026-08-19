import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from order_api.main import app
from order_api.services.order import InsufficientStockError, SkuNotFoundError, get_order_service


class _FakeReservation:
    def __init__(self, **kwargs):
        self._data = kwargs

    def to_dict(self):
        return self._data


class _OkOrderService:
    async def reserve(self, sku, quantity, reservation_id):
        return _FakeReservation(id=str(reservation_id), sku=sku, quantity=quantity, status="held")


class _UnknownSkuOrderService:
    async def reserve(self, sku, quantity, reservation_id):
        raise SkuNotFoundError(sku)


class _InsufficientStockOrderService:
    async def reserve(self, sku, quantity, reservation_id):
        raise InsufficientStockError(sku=sku, requested=quantity, available=1)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


async def _post_reserve(payload: dict):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.post("/reserve", json=payload)


@pytest.mark.asyncio
async def test_reserve_succeeds():
    app.dependency_overrides[get_order_service] = lambda: _OkOrderService()
    reservation_id = str(uuid.uuid4())

    response = await _post_reserve(
        {"sku": "WIDGET-1", "quantity": 2, "reservation_id": reservation_id}
    )

    assert response.status_code == 201
    body = response.json()
    assert body["success"] is True
    assert body["data"]["sku"] == "WIDGET-1"
    assert body["data"]["status"] == "held"


@pytest.mark.asyncio
async def test_reserve_unknown_sku_returns_404():
    app.dependency_overrides[get_order_service] = lambda: _UnknownSkuOrderService()

    response = await _post_reserve(
        {"sku": "NOPE", "quantity": 1, "reservation_id": str(uuid.uuid4())}
    )

    assert response.status_code == 404
    assert response.json()["success"] is False


@pytest.mark.asyncio
async def test_reserve_insufficient_stock_returns_409():
    app.dependency_overrides[get_order_service] = lambda: _InsufficientStockOrderService()

    response = await _post_reserve(
        {"sku": "WIDGET-1", "quantity": 999, "reservation_id": str(uuid.uuid4())}
    )

    assert response.status_code == 409
    assert response.json()["success"] is False


@pytest.mark.asyncio
async def test_reserve_rejects_non_positive_quantity():
    response = await _post_reserve(
        {"sku": "WIDGET-1", "quantity": 0, "reservation_id": str(uuid.uuid4())}
    )

    assert response.status_code == 422
