import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from order_api.models.base import Base
from order_api.models.inventory_balances import InventoryBalance
from order_api.models.inventory_reservations import InventoryReservation
from order_api.models.stock_audit_ledger import StockAuditLedger
from order_api.worker import ensure_consumer_group, process_batch


@pytest.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as s:
        yield s

    await engine.dispose()


@pytest.fixture
async def seeded_balance(session):
    balance = InventoryBalance(sku="WIDGET-1", name="Widget", total_stock=100, available=100)
    session.add(balance)
    await session.commit()
    return balance


class _FakeGroupRedis:
    def __init__(self, raise_error: Exception | None = None):
        self.raise_error = raise_error
        self.calls = []

    async def xgroup_create(self, stream, group, id, mkstream):
        self.calls.append((stream, group, id, mkstream))
        if self.raise_error:
            raise self.raise_error


def _reserved_message(reservation_id: str, sku: str = "WIDGET-1", quantity: str = "10"):
    return (
        "1-0",
        {
            "event_type": "reserved",
            "reservation_id": reservation_id,
            "sku": sku,
            "quantity": quantity,
        },
    )


@pytest.mark.asyncio
async def test_ensure_consumer_group_creates_group():
    fake_redis = _FakeGroupRedis()

    await ensure_consumer_group(fake_redis, "stream:x", "group:x")

    assert fake_redis.calls == [("stream:x", "group:x", "0", True)]


@pytest.mark.asyncio
async def test_ensure_consumer_group_swallows_busygroup():
    fake_redis = _FakeGroupRedis(
        raise_error=Exception("BUSYGROUP Consumer Group name already exists")
    )

    await ensure_consumer_group(fake_redis, "stream:x", "group:x")


@pytest.mark.asyncio
async def test_ensure_consumer_group_reraises_other_errors():
    fake_redis = _FakeGroupRedis(raise_error=Exception("connection refused"))

    with pytest.raises(Exception, match="connection refused"):
        await ensure_consumer_group(fake_redis, "stream:x", "group:x")


@pytest.mark.asyncio
async def test_process_batch_creates_reservation_and_decrements_balance(
    session, seeded_balance, caplog
):
    reservation_id = str(uuid.uuid4())

    with caplog.at_level("INFO"):
        processed = await process_batch(session, [_reserved_message(reservation_id)])

    assert processed == 1

    reservation = await session.get(InventoryReservation, uuid.UUID(reservation_id))
    assert reservation is not None
    assert reservation.status == "held"

    await session.refresh(seeded_balance)
    assert seeded_balance.available == 90

    ledger_rows = (await session.execute(select(StockAuditLedger))).scalars().all()
    assert len(ledger_rows) == 1
    assert ledger_rows[0].event_type == "reserved"

    assert any(
        "reserved event processed" in record.message
        and reservation_id in record.message
        and "WIDGET-1" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_process_batch_skips_redelivered_reservation(session, seeded_balance, caplog):
    reservation_id = str(uuid.uuid4())
    message = _reserved_message(reservation_id)

    first = await process_batch(session, [message])
    with caplog.at_level("INFO"):
        second = await process_batch(session, [message])  # simulated XREADGROUP redelivery

    assert first == 1
    assert second == 0

    await session.refresh(seeded_balance)
    assert seeded_balance.available == 90  # not decremented twice

    assert any(
        "skipped redelivered event" in record.message and reservation_id in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_process_batch_ignores_non_reserved_events(session):
    message = (
        "1-0",
        {
            "event_type": "committed",
            "reservation_id": str(uuid.uuid4()),
            "sku": "x",
            "quantity": "1",
        },
    )

    processed = await process_batch(session, [message])

    assert processed == 0
