import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from order_api.models.base import Base
from order_api.models.inventory_balances import InventoryBalance
from order_api.models.stock_audit_ledger import StockAuditLedger
from order_api.services.order import InsufficientStockError, OrderService, SkuNotFoundError


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


@pytest.mark.asyncio
async def test_reserve_decrements_balance_and_creates_records(session, seeded_balance):
    service = OrderService(session)
    reservation_id = uuid.uuid4()

    reservation = await service.reserve(
        sku="WIDGET-1", quantity=10, reservation_id=reservation_id
    )

    assert reservation.status == "held"
    assert reservation.quantity == 10

    await session.refresh(seeded_balance)
    assert seeded_balance.available == 90

    ledger_rows = (await session.execute(select(StockAuditLedger))).scalars().all()
    assert len(ledger_rows) == 1
    assert ledger_rows[0].event_type == "reserved"
    assert ledger_rows[0].reservation_id == reservation_id


@pytest.mark.asyncio
async def test_reserve_is_idempotent_on_retry(session, seeded_balance):
    service = OrderService(session)
    reservation_id = uuid.uuid4()

    first = await service.reserve(sku="WIDGET-1", quantity=10, reservation_id=reservation_id)
    second = await service.reserve(sku="WIDGET-1", quantity=10, reservation_id=reservation_id)

    assert first.id == second.id

    await session.refresh(seeded_balance)
    assert seeded_balance.available == 90  # not decremented twice


@pytest.mark.asyncio
async def test_reserve_unknown_sku_raises(session):
    service = OrderService(session)

    with pytest.raises(SkuNotFoundError):
        await service.reserve(sku="NOPE", quantity=1, reservation_id=uuid.uuid4())


@pytest.mark.asyncio
async def test_reserve_insufficient_stock_raises_without_mutating_balance(
    session, seeded_balance
):
    service = OrderService(session)

    with pytest.raises(InsufficientStockError):
        await service.reserve(sku="WIDGET-1", quantity=999, reservation_id=uuid.uuid4())

    await session.refresh(seeded_balance)
    assert seeded_balance.available == 100
