import logging
import uuid
from pathlib import Path

from fastapi import Depends
from redis.asyncio import Redis

from order_api.core.config import get_settings
from order_api.core.db.redis import get_redis

logger = logging.getLogger("order_api.services.order")

_RESERVE_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "core" / "lua" / "reserve.lua"


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


class ReservationHeld:
    """Result of a successful (or idempotently-repeated) reserve call. Not a
    durable record — Postgres only learns about this asynchronously via the
    stream worker; see ORDER_LIFECYCLE.md "01 — Reserve" steps 5-6."""

    def __init__(self, reservation_id: uuid.UUID, sku: str, quantity: int, available: int):
        self.reservation_id = reservation_id
        self.sku = sku
        self.quantity = quantity
        self.available = available

    def to_dict(self) -> dict:
        return {
            "reservation_id": str(self.reservation_id),
            "sku": self.sku,
            "quantity": self.quantity,
            "status": "held",
            "available": self.available,
        }


class OrderService:
    """Redis-backed reservation hot path — ORDER_LIFECYCLE.md "01 — Reserve".
    One atomic Lua script does idempotency + stock check + decrement + hold
    + stream append; WAIT then bounds how much a replica failover could
    lose before the client is told it worked.
    """

    def __init__(
        self,
        redis: Redis,
        hold_ttl_seconds: int,
        stream_name: str,
        wait_replicas: int = 0,
        wait_timeout_ms: int = 200,
    ):
        self.redis = redis
        self.hold_ttl_seconds = hold_ttl_seconds
        self.stream_name = stream_name
        self.wait_replicas = wait_replicas
        self.wait_timeout_ms = wait_timeout_ms
        self._script = redis.register_script(_RESERVE_SCRIPT_PATH.read_text())

    async def reserve(
        self, sku: str, quantity: int, reservation_id: uuid.UUID
    ) -> ReservationHeld:
        status, available = await self._script(
            keys=[f"stock:{sku}:available", f"hold:{reservation_id}", self.stream_name],
            args=[sku, quantity, str(reservation_id), self.hold_ttl_seconds],
        )
        status = status.decode() if isinstance(status, bytes) else status
        available = int(available)

        if status == "unknown_sku":
            logger.warning(
                "reserve rejected: unknown sku=%s reservation_id=%s", sku, reservation_id
            )
            raise SkuNotFoundError(sku)
        if status == "insufficient_stock":
            logger.info(
                "reserve rejected: insufficient stock sku=%s requested=%d available=%d "
                "reservation_id=%s",
                sku,
                quantity,
                available,
                reservation_id,
            )
            raise InsufficientStockError(sku=sku, requested=quantity, available=available)

        if self.wait_replicas > 0:
            await self.redis.wait(self.wait_replicas, self.wait_timeout_ms)

        if status == "duplicate":
            logger.info("reserve idempotent replay: reservation_id=%s sku=%s", reservation_id, sku)
        else:
            logger.info(
                "reserve held: reservation_id=%s sku=%s quantity=%d available=%d",
                reservation_id,
                sku,
                quantity,
                available,
            )

        return ReservationHeld(
            reservation_id=reservation_id, sku=sku, quantity=quantity, available=available
        )

    async def seed_stock(self, sku: str, available: int) -> None:
        """Set Redis's live counter for a SKU. Bootstraps a fresh Redis from
        Postgres; ongoing drift correction is the reconciliation watchdog's
        job (SYSTEM_DESIGN.md), not this method's."""
        await self.redis.set(f"stock:{sku}:available", available)


def get_order_service(redis: Redis = Depends(get_redis)) -> OrderService:
    settings = get_settings()
    return OrderService(
        redis=redis,
        hold_ttl_seconds=settings.hold_ttl_seconds,
        stream_name=settings.stream_inventory_events,
        wait_replicas=settings.redis_wait_replicas,
        wait_timeout_ms=settings.redis_wait_timeout_ms,
    )
