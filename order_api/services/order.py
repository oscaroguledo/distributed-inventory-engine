import logging
import time
import uuid
from pathlib import Path

from fastapi import Depends
from redis.asyncio import Redis

from order_api.core.config import get_settings
from order_api.core.db.redis import get_redis
from order_api.core.metrics import LUA_SCRIPT_DURATION, REDIS_WAIT_TIMEOUTS, STOCK_AVAILABLE

logger = logging.getLogger("order_api.services.order")

_LUA_DIR = Path(__file__).resolve().parent.parent / "core" / "lua"
_RESERVE_SCRIPT_PATH = _LUA_DIR / "reserve.lua"
_COMMIT_SCRIPT_PATH = _LUA_DIR / "commit.lua"
_RELEASE_SCRIPT_PATH = _LUA_DIR / "release.lua"


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


class HoldNotFoundError(Exception):
    def __init__(self, reservation_id: uuid.UUID):
        self.reservation_id = reservation_id
        super().__init__(f"no active hold for reservation_id: {reservation_id}")


class ReservationHeld:
    """Result of a successful (or idempotent) reserve call — not durable;
    Postgres learns about it later via the stream worker."""

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


class ReservationCommitted:
    """Result of a successful commit call — not durable until the worker
    flips the reservation's status in Postgres."""

    def __init__(self, reservation_id: uuid.UUID, sku: str):
        self.reservation_id = reservation_id
        self.sku = sku

    def to_dict(self) -> dict:
        return {
            "reservation_id": str(self.reservation_id),
            "sku": self.sku,
            "status": "committed",
        }


class ReservationReleased:
    """Result of a successful release call — not durable until the worker
    flips the reservation's status in Postgres."""

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
            "status": "released",
            "available": self.available,
        }


class OrderService:
    """Redis-backed reservation hot path — ORDER_LIFECYCLE.md "01 — Reserve".
    One atomic Lua script does the hold; WAIT bounds replica failover risk."""

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
        self._reserve_script = redis.register_script(_RESERVE_SCRIPT_PATH.read_text())
        self._commit_script = redis.register_script(_COMMIT_SCRIPT_PATH.read_text())
        self._release_script = redis.register_script(_RELEASE_SCRIPT_PATH.read_text())

    async def _run_script(self, name: str, script, *, keys: list, args: list):
        start = time.monotonic()
        try:
            return await script(keys=keys, args=args)
        finally:
            LUA_SCRIPT_DURATION.labels(script=name).observe(time.monotonic() - start)

    async def _wait_for_replicas(self, operation: str) -> None:
        if self.wait_replicas <= 0:
            return
        acked = await self.redis.wait(self.wait_replicas, self.wait_timeout_ms)
        if acked < self.wait_replicas:
            REDIS_WAIT_TIMEOUTS.labels(operation=operation).inc()

    async def reserve(
        self, sku: str, quantity: int, reservation_id: uuid.UUID
    ) -> ReservationHeld:
        status, available = await self._run_script(
            "reserve",
            self._reserve_script,
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
            STOCK_AVAILABLE.labels(sku=sku).set(available)
            logger.info(
                "reserve rejected: insufficient stock sku=%s requested=%d available=%d "
                "reservation_id=%s",
                sku,
                quantity,
                available,
                reservation_id,
            )
            raise InsufficientStockError(sku=sku, requested=quantity, available=available)

        await self._wait_for_replicas("reserve")
        STOCK_AVAILABLE.labels(sku=sku).set(available)

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

    async def commit(self, reservation_id: uuid.UUID) -> ReservationCommitted:
        status, sku = await self._run_script(
            "commit",
            self._commit_script,
            keys=[f"hold:{reservation_id}", self.stream_name],
            args=[str(reservation_id)],
        )
        status = status.decode() if isinstance(status, bytes) else status
        sku = sku.decode() if isinstance(sku, bytes) else sku

        if status == "not_found":
            logger.warning("commit rejected: no hold for reservation_id=%s", reservation_id)
            raise HoldNotFoundError(reservation_id)

        await self._wait_for_replicas("commit")

        logger.info("reservation committed: reservation_id=%s sku=%s", reservation_id, sku)
        return ReservationCommitted(reservation_id=reservation_id, sku=sku)

    async def release(self, reservation_id: uuid.UUID) -> ReservationReleased:
        status, sku, quantity, available = await self._run_script(
            "release",
            self._release_script,
            keys=[f"hold:{reservation_id}", self.stream_name],
            args=[str(reservation_id)],
        )
        status = status.decode() if isinstance(status, bytes) else status
        sku = sku.decode() if isinstance(sku, bytes) else sku

        if status == "not_found":
            logger.warning("release rejected: no hold for reservation_id=%s", reservation_id)
            raise HoldNotFoundError(reservation_id)

        await self._wait_for_replicas("release")
        STOCK_AVAILABLE.labels(sku=sku).set(int(available))

        logger.info(
            "reservation released: reservation_id=%s sku=%s quantity=%d available=%d",
            reservation_id,
            sku,
            quantity,
            available,
        )
        return ReservationReleased(
            reservation_id=reservation_id, sku=sku, quantity=int(quantity), available=int(available)
        )

    async def seed_stock(self, sku: str, available: int) -> None:
        """Sets Redis's live counter for a SKU — bootstraps a fresh Redis;
        ongoing drift correction is the reconciliation watchdog's job."""
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
