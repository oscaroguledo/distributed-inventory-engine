import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from order_api.core.rate_limiter import get_rate_limiter
from order_api.main import app
from order_api.services.order import (
    HoldNotFoundError,
    InsufficientStockError,
    SkuNotFoundError,
    get_order_service,
)


class _FakeResult:
    def __init__(self, **kwargs):
        self._data = kwargs

    def to_dict(self):
        return self._data


class _OkOrderService:
    async def reserve(self, sku, quantity, reservation_id):
        return _FakeResult(id=str(reservation_id), sku=sku, quantity=quantity, status="held")

    async def commit(self, reservation_id):
        return _FakeResult(reservation_id=str(reservation_id), sku="WIDGET-1", status="committed")

    async def release(self, reservation_id):
        return _FakeResult(reservation_id=str(reservation_id), sku="WIDGET-1", status="released")


class _UnknownSkuOrderService:
    async def reserve(self, sku, quantity, reservation_id):
        raise SkuNotFoundError(sku)


class _InsufficientStockOrderService:
    async def reserve(self, sku, quantity, reservation_id):
        raise InsufficientStockError(sku=sku, requested=quantity, available=1)


class _HoldNotFoundOrderService:
    async def commit(self, reservation_id):
        raise HoldNotFoundError(reservation_id)

    async def release(self, reservation_id):
        raise HoldNotFoundError(reservation_id)


class _AllowAllLimiter:
    async def is_allowed(self, key, cost=1):
        return True, 999.0


class _DenyAllLimiter:
    async def is_allowed(self, key, cost=1):
        return False, 0.0


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def _allow_rate_limit_by_default():
    """Every test hits a real dependency chain unless overridden — without
    this, /reserve would try to reach real Redis via enforce_rate_limit."""
    app.dependency_overrides[get_rate_limiter] = lambda: _AllowAllLimiter()


async def _post_reserve(payload: dict):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.post("/reserve", json=payload)


async def _post_commit(payload: dict):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.post("/commit", json=payload)


async def _post_release(payload: dict):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.post("/release", json=payload)


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


@pytest.mark.asyncio
async def test_reserve_returns_429_when_rate_limited(caplog):
    app.dependency_overrides[get_rate_limiter] = lambda: _DenyAllLimiter()
    app.dependency_overrides[get_order_service] = lambda: _OkOrderService()

    with caplog.at_level("WARNING"):
        response = await _post_reserve(
            {"sku": "WIDGET-1", "quantity": 1, "reservation_id": str(uuid.uuid4())}
        )

    assert response.status_code == 429
    body = response.json()
    assert body["success"] is False
    assert body["status"] == 429
    assert any("rate limit exceeded" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_commit_succeeds():
    app.dependency_overrides[get_order_service] = lambda: _OkOrderService()
    reservation_id = str(uuid.uuid4())

    response = await _post_commit({"reservation_id": reservation_id})

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["status"] == "committed"


@pytest.mark.asyncio
async def test_commit_returns_404_when_hold_not_found():
    app.dependency_overrides[get_order_service] = lambda: _HoldNotFoundOrderService()

    response = await _post_commit({"reservation_id": str(uuid.uuid4())})

    assert response.status_code == 404
    assert response.json()["success"] is False


@pytest.mark.asyncio
async def test_commit_returns_429_when_rate_limited():
    app.dependency_overrides[get_rate_limiter] = lambda: _DenyAllLimiter()
    app.dependency_overrides[get_order_service] = lambda: _OkOrderService()

    response = await _post_commit({"reservation_id": str(uuid.uuid4())})

    assert response.status_code == 429


@pytest.mark.asyncio
async def test_release_succeeds():
    app.dependency_overrides[get_order_service] = lambda: _OkOrderService()
    reservation_id = str(uuid.uuid4())

    response = await _post_release({"reservation_id": reservation_id})

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["status"] == "released"


@pytest.mark.asyncio
async def test_release_returns_404_when_hold_not_found():
    app.dependency_overrides[get_order_service] = lambda: _HoldNotFoundOrderService()

    response = await _post_release({"reservation_id": str(uuid.uuid4())})

    assert response.status_code == 404
    assert response.json()["success"] is False
