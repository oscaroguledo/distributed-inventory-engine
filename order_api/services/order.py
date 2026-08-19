import uuid

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from order_api.core.db.postgresql import get_session
from order_api.models.inventory_balances import InventoryBalance
from order_api.models.inventory_reservations import InventoryReservation
from order_api.models.stock_audit_ledger import StockAuditLedger


class SkuNotFoundError(Exception):
    def __init__(self, sku: str):
        self.sku = sku
        super().__init__(f"unknown sku: {sku}")


class InsufficientStockError(Exception):
    def __init__(self, sku: str, requested: int, available: int):
        self.sku = sku
        self.requested = requested
        self.available = available
        super().__init__(
            f"insufficient stock for {sku}: requested {requested}, available {available}"
        )


class OrderService:
    """Postgres-backed reservation logic. Row-locks the balance to serialize
    concurrent reserves for the same SKU — see SYSTEM_DESIGN.md for why a
    future iteration would move this hot path to the Redis hold engine.
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def reserve(
        self, sku: str, quantity: int, reservation_id: uuid.UUID
    ) -> InventoryReservation:
        existing = await self.session.get(InventoryReservation, reservation_id)
        if existing is not None:
            return existing

        result = await self.session.execute(
            select(InventoryBalance).where(InventoryBalance.sku == sku).with_for_update()
        )
        balance = result.scalar_one_or_none()
        if balance is None:
            raise SkuNotFoundError(sku)

        if balance.available < quantity:
            raise InsufficientStockError(
                sku=sku, requested=quantity, available=balance.available
            )

        balance.available -= quantity

        reservation = InventoryReservation(
            id=reservation_id, sku=sku, quantity=quantity, status="held"
        )
        self.session.add(reservation)
        self.session.add(
            StockAuditLedger(
                reservation_id=reservation_id, sku=sku, quantity=quantity, event_type="reserved"
            )
        )

        await self.session.commit()
        await self.session.refresh(reservation)
        return reservation


async def get_order_service(session: AsyncSession = Depends(get_session)) -> OrderService:
    return OrderService(session)
