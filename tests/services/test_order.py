import uuid

import pytest

from order_api.core.metrics import REDIS_WAIT_TIMEOUTS
from order_api.services.order import (
    HoldNotFoundError,
    InsufficientStockError,
    OrderService,
    SkuNotFoundError,
    get_order_service,
)


class _FakeRedis:
    """Simulates just enough of Redis to test OrderService's own logic —
    the Lua scripts' actual atomicity is verified live via Docker."""

    def __init__(self, wait_ack_count: int | None = None):
        self.store: dict[str, int] = {}
        self.holds: dict[str, dict] = {}
        self.script_calls: list[tuple[list, list]] = []
        self.wait_calls: list[tuple[int, int]] = []
        self._wait_ack_count = wait_ack_count

    def register_script(self, script_body: str):
        if "insufficient_stock" in script_body:
            return self._reserve
        if "INCRBY" in script_body:
            return self._release
        if "not_found" in script_body:
            return self._commit
        raise ValueError("unrecognized script body in test fake")

    async def _reserve(self, keys, args):
        self.script_calls.append((keys, args))
        stock_key, hold_key, _stream_key = keys
        sku, quantity, _reservation_id, _hold_ttl = args
        quantity = int(quantity)

        if hold_key in self.holds:
            return ["duplicate", self.store.get(stock_key, 0)]
        if stock_key not in self.store:
            return ["unknown_sku", 0]

        available = self.store[stock_key]
        if available < quantity:
            return ["insufficient_stock", available]

        self.store[stock_key] -= quantity
        self.holds[hold_key] = {"sku": sku, "quantity": quantity}
        return ["held", self.store[stock_key]]

    async def _commit(self, keys, args):
        self.script_calls.append((keys, args))
        hold_key, _stream_key = keys

        if hold_key not in self.holds:
            return ["not_found", ""]

        sku = self.holds.pop(hold_key)["sku"]
        return ["committed", sku]

    async def _release(self, keys, args):
        self.script_calls.append((keys, args))
        hold_key, _stream_key = keys

        if hold_key not in self.holds:
            return ["not_found", "", 0, 0]

        hold = self.holds.pop(hold_key)
        sku, quantity = hold["sku"], hold["quantity"]
        stock_key = f"stock:{sku}:available"
        self.store[stock_key] = self.store.get(stock_key, 0) + quantity
        return ["released", sku, quantity, self.store[stock_key]]

    async def set(self, key, value):
        self.store[key] = int(value)

    async def wait(self, numreplicas, timeout):
        self.wait_calls.append((numreplicas, timeout))
        return numreplicas if self._wait_ack_count is None else self._wait_ack_count


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
    assert result.to_dict()["status"] == "held"
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
async def test_reserve_counts_a_wait_timeout_when_ack_falls_short():
    fake_redis = _FakeRedis(wait_ack_count=0)  # replica never acked in time
    service = OrderService(
        redis=fake_redis,
        hold_ttl_seconds=900,
        stream_name="stream:x",
        wait_replicas=1,
        wait_timeout_ms=100,
    )
    await service.seed_stock("WIDGET-1", 100)
    before = REDIS_WAIT_TIMEOUTS.labels(operation="reserve")._value.get()

    await service.reserve(sku="WIDGET-1", quantity=1, reservation_id=uuid.uuid4())

    assert REDIS_WAIT_TIMEOUTS.labels(operation="reserve")._value.get() == before + 1


@pytest.mark.asyncio
async def test_reserve_skips_wait_when_no_replicas_configured():
    fake_redis = _FakeRedis()
    service = OrderService(
        redis=fake_redis, hold_ttl_seconds=900, stream_name="stream:x", wait_replicas=0
    )
    await service.seed_stock("WIDGET-1", 100)

    await service.reserve(sku="WIDGET-1", quantity=1, reservation_id=uuid.uuid4())

    assert fake_redis.wait_calls == []


@pytest.mark.asyncio
async def test_commit_succeeds_after_reserve(caplog):
    fake_redis = _FakeRedis()
    service = OrderService(redis=fake_redis, hold_ttl_seconds=900, stream_name="stream:x")
    await service.seed_stock("WIDGET-1", 100)

    reservation_id = uuid.uuid4()
    await service.reserve(sku="WIDGET-1", quantity=10, reservation_id=reservation_id)

    with caplog.at_level("INFO"):
        result = await service.commit(reservation_id=reservation_id)

    assert result.reservation_id == reservation_id
    assert result.sku == "WIDGET-1"
    assert result.to_dict()["status"] == "committed"
    assert f"hold:{reservation_id}" not in fake_redis.holds
    assert any(
        "reservation committed" in record.message and str(reservation_id) in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_commit_raises_when_no_hold_exists(caplog):
    fake_redis = _FakeRedis()
    service = OrderService(redis=fake_redis, hold_ttl_seconds=900, stream_name="stream:x")

    with caplog.at_level("WARNING"), pytest.raises(HoldNotFoundError):
        await service.commit(reservation_id=uuid.uuid4())

    assert any("commit rejected" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_commit_calls_wait_when_replicas_configured():
    fake_redis = _FakeRedis()
    service = OrderService(
        redis=fake_redis,
        hold_ttl_seconds=900,
        stream_name="stream:x",
        wait_replicas=1,
        wait_timeout_ms=100,
    )
    await service.seed_stock("WIDGET-1", 100)
    reservation_id = uuid.uuid4()
    await service.reserve(sku="WIDGET-1", quantity=1, reservation_id=reservation_id)
    fake_redis.wait_calls.clear()

    await service.commit(reservation_id=reservation_id)

    assert fake_redis.wait_calls == [(1, 100)]


@pytest.mark.asyncio
async def test_release_succeeds_after_reserve(caplog):
    fake_redis = _FakeRedis()
    service = OrderService(redis=fake_redis, hold_ttl_seconds=900, stream_name="stream:x")
    await service.seed_stock("WIDGET-1", 100)

    reservation_id = uuid.uuid4()
    await service.reserve(sku="WIDGET-1", quantity=10, reservation_id=reservation_id)

    with caplog.at_level("INFO"):
        result = await service.release(reservation_id=reservation_id)

    assert result.reservation_id == reservation_id
    assert result.sku == "WIDGET-1"
    assert result.quantity == 10
    assert result.available == 100  # restored
    assert result.to_dict()["status"] == "released"
    assert f"hold:{reservation_id}" not in fake_redis.holds
    assert any(
        "reservation released" in record.message and str(reservation_id) in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_release_raises_when_no_hold_exists(caplog):
    fake_redis = _FakeRedis()
    service = OrderService(redis=fake_redis, hold_ttl_seconds=900, stream_name="stream:x")

    with caplog.at_level("WARNING"), pytest.raises(HoldNotFoundError):
        await service.release(reservation_id=uuid.uuid4())

    assert any("release rejected" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_release_calls_wait_when_replicas_configured():
    fake_redis = _FakeRedis()
    service = OrderService(
        redis=fake_redis,
        hold_ttl_seconds=900,
        stream_name="stream:x",
        wait_replicas=1,
        wait_timeout_ms=100,
    )
    await service.seed_stock("WIDGET-1", 100)
    reservation_id = uuid.uuid4()
    await service.reserve(sku="WIDGET-1", quantity=1, reservation_id=reservation_id)
    fake_redis.wait_calls.clear()

    await service.release(reservation_id=reservation_id)

    assert fake_redis.wait_calls == [(1, 100)]


def test_get_order_service_builds_from_settings():
    fake_redis = _FakeRedis()

    service = get_order_service(redis=fake_redis)

    assert isinstance(service, OrderService)
    assert service.hold_ttl_seconds > 0
    assert service.stream_name
