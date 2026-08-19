import uuid

import pytest

from order_api.services.order import (
    InsufficientStockError,
    OrderService,
    SkuNotFoundError,
    get_order_service,
)


class _FakeRedis:
    """Simulates just enough of Redis (register_script + set + wait) to
    exercise OrderService's own logic. The Lua script's actual atomicity is
    verified live via Docker, not here — see the reserve.lua comments."""

    def __init__(self):
        self.store: dict[str, int] = {}
        self.holds: set[str] = set()
        self.script_calls: list[tuple[list, list]] = []
        self.wait_calls: list[tuple[int, int]] = []

    def register_script(self, _script_body):
        async def _script(keys, args):
            self.script_calls.append((keys, args))
            stock_key, hold_key, _stream_key = keys
            _sku, quantity, _reservation_id, _hold_ttl = args
            quantity = int(quantity)

            if hold_key in self.holds:
                return ["duplicate", self.store.get(stock_key, 0)]
            if stock_key not in self.store:
                return ["unknown_sku", 0]

            available = self.store[stock_key]
            if available < quantity:
                return ["insufficient_stock", available]

            self.store[stock_key] -= quantity
            self.holds.add(hold_key)
            return ["held", self.store[stock_key]]

        return _script

    async def set(self, key, value):
        self.store[key] = int(value)

    async def wait(self, numreplicas, timeout):
        self.wait_calls.append((numreplicas, timeout))
        return numreplicas


@pytest.mark.asyncio
async def test_seed_stock_sets_the_redis_counter():
    fake_redis = _FakeRedis()
    service = OrderService(redis=fake_redis, hold_ttl_seconds=900, stream_name="stream:x")

    await service.seed_stock("WIDGET-1", 42)

    assert fake_redis.store["stock:WIDGET-1:available"] == 42


@pytest.mark.asyncio
async def test_reserve_succeeds_and_decrements_stock(caplog):
    fake_redis = _FakeRedis()
    service = OrderService(redis=fake_redis, hold_ttl_seconds=900, stream_name="stream:x")
    await service.seed_stock("WIDGET-1", 100)

    reservation_id = uuid.uuid4()
    with caplog.at_level("INFO"):
        result = await service.reserve(sku="WIDGET-1", quantity=10, reservation_id=reservation_id)

    assert result.reservation_id == reservation_id
    assert result.sku == "WIDGET-1"
    assert result.available == 90
    assert any(
        "reserve held" in record.message and str(reservation_id) in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_reserve_is_idempotent_on_retry(caplog):
    fake_redis = _FakeRedis()
    service = OrderService(redis=fake_redis, hold_ttl_seconds=900, stream_name="stream:x")
    await service.seed_stock("WIDGET-1", 100)

    reservation_id = uuid.uuid4()
    first = await service.reserve(sku="WIDGET-1", quantity=10, reservation_id=reservation_id)
    with caplog.at_level("INFO"):
        second = await service.reserve(sku="WIDGET-1", quantity=10, reservation_id=reservation_id)

    assert first.available == 90
    assert second.available == 90  # not decremented twice
    assert any(
        "reserve idempotent replay" in record.message and str(reservation_id) in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_reserve_unknown_sku_raises(caplog):
    fake_redis = _FakeRedis()
    service = OrderService(redis=fake_redis, hold_ttl_seconds=900, stream_name="stream:x")

    with caplog.at_level("WARNING"), pytest.raises(SkuNotFoundError):
        await service.reserve(sku="NOPE", quantity=1, reservation_id=uuid.uuid4())

    assert any("unknown sku=NOPE" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_reserve_insufficient_stock_raises(caplog):
    fake_redis = _FakeRedis()
    service = OrderService(redis=fake_redis, hold_ttl_seconds=900, stream_name="stream:x")
    await service.seed_stock("WIDGET-1", 5)

    with caplog.at_level("INFO"), pytest.raises(InsufficientStockError):
        await service.reserve(sku="WIDGET-1", quantity=10, reservation_id=uuid.uuid4())

    assert any("insufficient stock" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_reserve_calls_wait_when_replicas_configured():
    fake_redis = _FakeRedis()
    service = OrderService(
        redis=fake_redis,
        hold_ttl_seconds=900,
        stream_name="stream:x",
        wait_replicas=1,
        wait_timeout_ms=100,
    )
    await service.seed_stock("WIDGET-1", 100)

    await service.reserve(sku="WIDGET-1", quantity=1, reservation_id=uuid.uuid4())

    assert fake_redis.wait_calls == [(1, 100)]


@pytest.mark.asyncio
async def test_reserve_skips_wait_when_no_replicas_configured():
    fake_redis = _FakeRedis()
    service = OrderService(
        redis=fake_redis, hold_ttl_seconds=900, stream_name="stream:x", wait_replicas=0
    )
    await service.seed_stock("WIDGET-1", 100)

    await service.reserve(sku="WIDGET-1", quantity=1, reservation_id=uuid.uuid4())

    assert fake_redis.wait_calls == []


def test_get_order_service_builds_from_settings():
    fake_redis = _FakeRedis()

    service = get_order_service(redis=fake_redis)

    assert isinstance(service, OrderService)
    assert service.hold_ttl_seconds > 0
    assert service.stream_name
