import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from order_api.models.base import Base
from order_api.models.inventory_balances import InventoryBalance
from order_api.models.reconciliation_log import ReconciliationLog
from order_api.watchdog import ReconciliationWatchdog


class _FakeRedis:
    """Simulates just enough of Redis (GET/SET) to test the watchdog's own
    diff/rebuild logic — no live Redis needed."""

    def __init__(self, store: dict[str, int] | None = None):
        self.store = dict(store or {})
        self.set_calls: list[tuple[str, int]] = []

    async def get(self, key):
        value = self.store.get(key)
        return str(value) if value is not None else None

    async def set(self, key, value):
        self.store[key] = value
        self.set_calls.append((key, value))


@pytest.fixture
async def session_factory():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory

    await engine.dispose()


async def _seed_balance(session_factory, sku: str, available: int, total_stock: int = 100):
    async with session_factory() as session:
        session.add(
            InventoryBalance(sku=sku, name=sku, total_stock=total_stock, available=available)
        )
        await session.commit()


@pytest.mark.asyncio
async def test_run_once_does_nothing_when_in_sync(session_factory):
    await _seed_balance(session_factory, "WIDGET-1", available=90)
    fake_redis = _FakeRedis({"stock:WIDGET-1:available": 90})
    watchdog = ReconciliationWatchdog(redis=fake_redis, session_factory=session_factory)

    rebuilt = await watchdog.run_once()

    assert rebuilt == {}
    assert fake_redis.set_calls == []


@pytest.mark.asyncio
async def test_run_once_does_not_rebuild_on_first_mismatch(session_factory, caplog):
    await _seed_balance(session_factory, "WIDGET-1", available=90)
    fake_redis = _FakeRedis({"stock:WIDGET-1:available": 85})
    watchdog = ReconciliationWatchdog(
        redis=fake_redis, session_factory=session_factory, confirm_passes=2
    )

    with caplog.at_level("WARNING"):
        rebuilt = await watchdog.run_once()

    assert rebuilt == {}
    assert fake_redis.set_calls == []
    assert any(
        "drift detected" in record.message and "sku=WIDGET-1" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_run_once_rebuilds_after_confirm_passes(session_factory, caplog):
    await _seed_balance(session_factory, "WIDGET-1", available=90)
    fake_redis = _FakeRedis({"stock:WIDGET-1:available": 85})
    watchdog = ReconciliationWatchdog(
        redis=fake_redis, session_factory=session_factory, confirm_passes=2
    )

    await watchdog.run_once()
    with caplog.at_level("WARNING"):
        rebuilt = await watchdog.run_once()

    assert rebuilt == {"WIDGET-1": -5}
    assert fake_redis.set_calls == [("stock:WIDGET-1:available", 90)]
    assert any(
        "rebuilt redis from postgres" in record.message and "sku=WIDGET-1" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_run_once_records_a_durable_reconciliation_log_entry(session_factory):
    await _seed_balance(session_factory, "WIDGET-1", available=90)
    fake_redis = _FakeRedis({"stock:WIDGET-1:available": 85})
    watchdog = ReconciliationWatchdog(
        redis=fake_redis, session_factory=session_factory, confirm_passes=2
    )

    await watchdog.run_once()
    await watchdog.run_once()  # confirm_passes reached — triggers the rebuild

    async with session_factory() as session:
        rows = (await session.execute(select(ReconciliationLog))).scalars().all()

    assert len(rows) == 1
    assert rows[0].sku == "WIDGET-1"
    assert rows[0].redis_available == 85
    assert rows[0].postgres_available == 90
    assert rows[0].drift == -5


@pytest.mark.asyncio
async def test_run_once_resets_streak_when_drift_resolves(session_factory):
    await _seed_balance(session_factory, "WIDGET-1", available=90)
    fake_redis = _FakeRedis({"stock:WIDGET-1:available": 85})
    watchdog = ReconciliationWatchdog(
        redis=fake_redis, session_factory=session_factory, confirm_passes=2
    )

    await watchdog.run_once()  # streak=1
    fake_redis.store["stock:WIDGET-1:available"] = 90  # a worker flush catches Redis up
    assert await watchdog.run_once() == {}  # in sync again, streak resets

    fake_redis.store["stock:WIDGET-1:available"] = 85  # drifts again
    rebuilt = await watchdog.run_once()  # streak=1, not 3 — confirms the reset

    assert rebuilt == {}
    assert fake_redis.set_calls == []


@pytest.mark.asyncio
async def test_run_once_treats_missing_redis_key_as_drift(session_factory):
    await _seed_balance(session_factory, "WIDGET-1", available=90)
    fake_redis = _FakeRedis()  # Redis lost the key entirely
    watchdog = ReconciliationWatchdog(
        redis=fake_redis, session_factory=session_factory, confirm_passes=1
    )

    rebuilt = await watchdog.run_once()

    assert rebuilt == {"WIDGET-1": -90}
    assert fake_redis.set_calls == [("stock:WIDGET-1:available", 90)]

    async with session_factory() as session:
        rows = (await session.execute(select(ReconciliationLog))).scalars().all()
    assert rows[0].redis_available == 0  # None coerced to 0, not left null


@pytest.mark.asyncio
async def test_run_once_handles_multiple_skus_independently(session_factory):
    await _seed_balance(session_factory, "WIDGET-1", available=90)
    await _seed_balance(session_factory, "GADGET-2", available=40)
    fake_redis = _FakeRedis({"stock:WIDGET-1:available": 90, "stock:GADGET-2:available": 35})
    watchdog = ReconciliationWatchdog(
        redis=fake_redis, session_factory=session_factory, confirm_passes=1
    )

    rebuilt = await watchdog.run_once()

    assert rebuilt == {"GADGET-2": -5}
    assert fake_redis.set_calls == [("stock:GADGET-2:available", 40)]
