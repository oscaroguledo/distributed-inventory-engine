import logging
import uuid
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from order_api.core.config import get_settings
from order_api.core.db.postgresql import AsyncSessionLocal
from order_api.core.db.redis import redis_client
from order_api.models.inventory_balances import InventoryBalance
from order_api.models.inventory_reservations import InventoryReservation
from order_api.models.stock_audit_ledger import StockAuditLedger

logger = logging.getLogger("order_api.worker")

# N replicas can commit same-sku batches out of causal order; a rejected,
# retried CheckViolation on inventory_balances is expected — see git log.
BATCH_SIZE = 50
BLOCK_MS = 5000
RECLAIM_MIN_IDLE_MS = 5000

# SYSTEM_DESIGN.md Monitoring table, Stream workers row.
WORKER_EVENTS_PROCESSED = Counter(
    "worker_events_processed_total", "Stream events applied to Postgres", ["event_type"]
)
WORKER_BATCH_DURATION = Histogram("worker_batch_duration_seconds", "process_batch() latency")
WORKER_CONSUMER_GROUP_PENDING = Gauge(
    "worker_consumer_group_pending", "XPENDING summary count for the consumer group"
)


async def ensure_consumer_group(redis, stream: str, group: str) -> None:
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def _apply_reserved(session: AsyncSession, fields: dict) -> str:
    reservation_id = uuid.UUID(fields["reservation_id"])
    sku = fields["sku"]
    quantity = int(fields["quantity"])

    existing = await session.get(InventoryReservation, reservation_id)
    if existing is not None:
        logger.info("skipped redelivered event: reservation_id=%s sku=%s", reservation_id, sku)
        return "skipped"

    session.add(
        InventoryReservation(id=reservation_id, sku=sku, quantity=quantity, status="held")
    )
    session.add(
        StockAuditLedger(
            reservation_id=reservation_id, sku=sku, quantity=quantity, event_type="reserved"
        )
    )

    logger.info(
        "reserved event processed: reservation_id=%s sku=%s quantity=%d",
        reservation_id,
        sku,
        quantity,
    )
    return "applied"


async def _apply_committed(session: AsyncSession, fields: dict) -> str:
    reservation_id = uuid.UUID(fields["reservation_id"])
    sku = fields["sku"]
    quantity = int(fields.get("quantity", 0))

    reservation = await session.get(InventoryReservation, reservation_id)
    if reservation is None:
        # Another worker's batch holding the "reserved" row hasn't committed
        # yet — not a genuine unknown, so don't ack; retry on the next reclaim.
        logger.info(
            "deferring committed event, reserve not visible: reservation_id=%s", reservation_id
        )
        return "retry"
    if reservation.status != "held":
        logger.info("skipped committed event: reservation_id=%s not held", reservation_id)
        return "skipped"

    reservation.status = "committed"
    reservation.resolved_at = datetime.now(timezone.utc)
    session.add(
        StockAuditLedger(
            reservation_id=reservation_id, sku=sku, quantity=quantity, event_type="committed"
        )
    )

    logger.info("committed event processed: reservation_id=%s sku=%s", reservation_id, sku)
    return "applied"


async def _apply_released(session: AsyncSession, fields: dict) -> str:
    reservation_id = uuid.UUID(fields["reservation_id"])
    sku = fields["sku"]
    quantity = int(fields.get("quantity", 0))

    reservation = await session.get(InventoryReservation, reservation_id)
    if reservation is None:
        logger.info(
            "deferring released event, reserve not visible: reservation_id=%s", reservation_id
        )
        return "retry"
    if reservation.status != "held":
        logger.info("skipped released event: reservation_id=%s not held", reservation_id)
        return "skipped"

    reservation.status = "released"
    reservation.resolved_at = datetime.now(timezone.utc)
    session.add(
        StockAuditLedger(
            reservation_id=reservation_id, sku=sku, quantity=quantity, event_type="released"
        )
    )

    logger.info("released event processed: reservation_id=%s sku=%s", reservation_id, sku)
    return "applied"


async def _lock_balances(session: AsyncSession, messages: list[tuple[str, dict]]) -> dict:
    """Locks every sku in this batch once, sorted — not once per event, which
    deadlocks concurrent worker replicas re-locking the same row repeatedly."""
    skus = sorted(
        {
            fields["sku"]
            for _message_id, fields in messages
            if fields.get("event_type") in ("reserved", "released")
        }
    )
    if not skus:
        return {}

    result = await session.execute(
        select(InventoryBalance)
        .where(InventoryBalance.sku.in_(skus))
        .order_by(InventoryBalance.sku)
        .with_for_update()
    )
    return {balance.sku: balance for balance in result.scalars().all()}


async def _recompute_available(session: AsyncSession, balances: dict) -> None:
    """Sets available = total_stock - SUM(held+committed) fresh each time —
    incremental += / -= is only order-safe with one worker, not N replicas."""
    for sku, balance in balances.items():
        held_quantity = (
            await session.execute(
                select(func.coalesce(func.sum(InventoryReservation.quantity), 0)).where(
                    InventoryReservation.sku == sku,
                    InventoryReservation.status.in_(("held", "committed")),
                )
            )
        ).scalar_one()
        balance.available = balance.total_stock - held_quantity


async def process_batch(
    session: AsyncSession, messages: list[tuple[str, dict]]
) -> tuple[int, list[str]]:
    """Persists 'reserved'/'committed'/'released' events (ORDER_LIFECYCLE.md).
    Returns (applied_count, ack_ids) — "retry" status stays un-acked for reclaim."""
    with WORKER_BATCH_DURATION.time():
        processed = 0
        ack_ids = []
        balances = await _lock_balances(session, messages)

        for message_id, fields in messages:
            event_type = fields.get("event_type")
            if event_type == "reserved":
                status = await _apply_reserved(session, fields)
            elif event_type == "committed":
                status = await _apply_committed(session, fields)
            elif event_type == "released":
                status = await _apply_released(session, fields)
            else:
                status = "skipped"

            if status == "applied":
                processed += 1
                WORKER_EVENTS_PROCESSED.labels(event_type=event_type).inc()
            if status != "retry":
                ack_ids.append(message_id)

        await _recompute_available(session, balances)
        await session.commit()
        return processed, ack_ids


async def run_once(consumer_name: str) -> int:
    """Reclaims overdue deferred events, then XREADGROUP -> process -> XACK.
    Returns events applied."""
    settings = get_settings()
    stream = settings.stream_inventory_events
    group = settings.consumer_group_inventory_sync
    total = 0

    async with AsyncSessionLocal() as session:
        _cursor, claimed, _deleted = await redis_client.xautoclaim(
            stream, group, consumer_name, min_idle_time=RECLAIM_MIN_IDLE_MS, start_id="0-0"
        )
        if claimed:
            processed, ack_ids = await process_batch(session, claimed)
            total += processed
            if ack_ids:
                await redis_client.xack(stream, group, *ack_ids)

        response = await redis_client.xreadgroup(
            group, consumer_name, {stream: ">"}, count=BATCH_SIZE, block=BLOCK_MS
        )
        for _stream_name, messages in response or []:
            processed, ack_ids = await process_batch(session, messages)
            total += processed
            if ack_ids:
                await redis_client.xack(stream, group, *ack_ids)

    pending_summary = await redis_client.xpending(stream, group)
    WORKER_CONSUMER_GROUP_PENDING.set((pending_summary or {}).get("pending", 0))

    return total


async def run() -> None:  # pragma: no cover -- infinite poll loop, verified live via Docker
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    consumer_name = f"worker-{uuid.uuid4().hex[:8]}"

    start_http_server(settings.worker_metrics_port)
    await ensure_consumer_group(
        redis_client, settings.stream_inventory_events, settings.consumer_group_inventory_sync
    )
    logger.info("inventory sync worker started, consumer=%s", consumer_name)

    while True:
        try:
            processed = await run_once(consumer_name)
            if processed:
                logger.info("processed %d event(s)", processed)
        except Exception:
            logger.exception("worker batch failed, retrying next poll")


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(run())
