import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from order_api import worker
from order_api.core.config import get_settings
from order_api.models.base import Base
from order_api.models.inventory_balances import InventoryBalance
from order_api.models.inventory_reservations import InventoryReservation
from order_api.models.stock_audit_ledger import StockAuditLedger
from order_api.worker import ensure_consumer_group, process_batch, run_once


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


class _FakeStreamRedis:
    """Simulates just enough of Redis for run_once — XREADGROUP/XACK — the
    real streaming semantics are exercised live via Docker, not here."""

    def __init__(self, response):
        self._response = response
        self.xack_calls: list[tuple] = []

    async def xreadgroup(self, group, consumer_name, streams, count, block):
        return self._response

    async def xack(self, stream, group, *message_ids):
        self.xack_calls.append((stream, group, message_ids))


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


def _committed_message(reservation_id: str, sku: str = "WIDGET-1", quantity: str = "10"):
    return (
        "2-0",
        {
            "event_type": "committed",
            "reservation_id": reservation_id,
            "sku": sku,
            "quantity": quantity,
        },
    )


def _released_message(reservation_id: str, sku: str = "WIDGET-1", quantity: str = "10"):
    return (
        "3-0",
        {
            "event_type": "released",
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
async def test_process_batch_ignores_unhandled_event_types(session):
    message = (
        "1-0",
        {
            "event_type": "sku_created",
            "reservation_id": str(uuid.uuid4()),
            "sku": "x",
            "quantity": "1",
        },
    )

    processed = await process_batch(session, [message])

    assert processed == 0


@pytest.mark.asyncio
async def test_process_batch_commits_a_held_reservation(session, seeded_balance, caplog):
    reservation_id = str(uuid.uuid4())
    await process_batch(session, [_reserved_message(reservation_id)])

    with caplog.at_level("INFO"):
        processed = await process_batch(session, [_committed_message(reservation_id)])

    assert processed == 1

    reservation = await session.get(InventoryReservation, uuid.UUID(reservation_id))
    assert reservation.status == "committed"
    assert reservation.resolved_at is not None

    ledger_rows = (await session.execute(select(StockAuditLedger))).scalars().all()
    assert len(ledger_rows) == 2  # one 'reserved' + one 'committed'
    assert {row.event_type for row in ledger_rows} == {"reserved", "committed"}

    assert any(
        "committed event processed" in record.message and reservation_id in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_process_batch_skips_commit_for_unknown_reservation(session, caplog):
    reservation_id = str(uuid.uuid4())

    with caplog.at_level("INFO"):
        processed = await process_batch(session, [_committed_message(reservation_id)])

    assert processed == 0
    assert any(
        "skipped committed event" in record.message and reservation_id in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_process_batch_skips_redelivered_commit(session, seeded_balance):
    reservation_id = str(uuid.uuid4())
    await process_batch(session, [_reserved_message(reservation_id)])
    await process_batch(session, [_committed_message(reservation_id)])

    second_commit = await process_batch(session, [_committed_message(reservation_id)])

    assert second_commit == 0


@pytest.mark.asyncio
async def test_process_batch_releases_a_held_reservation(session, seeded_balance, caplog):
    reservation_id = str(uuid.uuid4())
    await process_batch(session, [_reserved_message(reservation_id)])

    with caplog.at_level("INFO"):
        processed = await process_batch(session, [_released_message(reservation_id)])

    assert processed == 1

    reservation = await session.get(InventoryReservation, uuid.UUID(reservation_id))
    assert reservation.status == "released"
    assert reservation.resolved_at is not None

    await session.refresh(seeded_balance)
    assert seeded_balance.available == 100  # restored

    ledger_rows = (await session.execute(select(StockAuditLedger))).scalars().all()
    assert len(ledger_rows) == 2  # one 'reserved' + one 'released'
    assert {row.event_type for row in ledger_rows} == {"reserved", "released"}

    assert any(
        "released event processed" in record.message and reservation_id in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_process_batch_skips_release_for_unknown_reservation(session, caplog):
    reservation_id = str(uuid.uuid4())

    with caplog.at_level("INFO"):
        processed = await process_batch(session, [_released_message(reservation_id)])

    assert processed == 0
    assert any(
        "skipped released event" in record.message and reservation_id in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_process_batch_skips_redelivered_release(session, seeded_balance):
    reservation_id = str(uuid.uuid4())
    await process_batch(session, [_reserved_message(reservation_id)])
    await process_batch(session, [_released_message(reservation_id)])

    second_release = await process_batch(session, [_released_message(reservation_id)])

    assert second_release == 0
    await session.refresh(seeded_balance)
    assert seeded_balance.available == 100  # not restored twice


@pytest.mark.asyncio
async def test_run_once_returns_zero_when_no_messages(monkeypatch):
    monkeypatch.setattr(worker, "redis_client", _FakeStreamRedis(response=None))

    processed = await run_once("consumer-1")

    assert processed == 0


@pytest.mark.asyncio
async def test_run_once_processes_a_batch_and_acks_it(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as seed_session:
        seed_session.add(
            InventoryBalance(sku="WIDGET-1", name="Widget", total_stock=100, available=100)
        )
        await seed_session.commit()

    settings = get_settings()
    reservation_id = str(uuid.uuid4())
    fake_redis = _FakeStreamRedis(
        response=[(settings.stream_inventory_events, [_reserved_message(reservation_id)])]
    )
    monkeypatch.setattr(worker, "redis_client", fake_redis)
    monkeypatch.setattr(worker, "AsyncSessionLocal", session_factory)

    processed = await run_once("consumer-1")

    assert processed == 1
    assert fake_redis.xack_calls == [
        (settings.stream_inventory_events, settings.consumer_group_inventory_sync, ("1-0",))
    ]

    async with session_factory() as check_session:
        reservation = await check_session.get(InventoryReservation, uuid.UUID(reservation_id))
        assert reservation.status == "held"

    await engine.dispose()
