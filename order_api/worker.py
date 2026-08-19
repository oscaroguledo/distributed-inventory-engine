import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from order_api.core.config import get_settings
from order_api.core.db.postgresql import AsyncSessionLocal
from order_api.core.db.redis import redis_client
from order_api.models.inventory_balances import InventoryBalance
from order_api.models.inventory_reservations import InventoryReservation
from order_api.models.stock_audit_ledger import StockAuditLedger

logger = logging.getLogger("order_api.worker")

BATCH_SIZE = 50
BLOCK_MS = 5000


async def ensure_consumer_group(redis, stream: str, group: str) -> None:
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def _apply_reserved(session: AsyncSession, fields: dict) -> bool:
    reservation_id = uuid.UUID(fields["reservation_id"])
    sku = fields["sku"]
    quantity = int(fields["quantity"])

    existing = await session.get(InventoryReservation, reservation_id)
    if existing is not None:
        logger.info("skipped redelivered event: reservation_id=%s sku=%s", reservation_id, sku)
        return False

    session.add(
        InventoryReservation(id=reservation_id, sku=sku, quantity=quantity, status="held")
    )
    session.add(
        StockAuditLedger(
            reservation_id=reservation_id, sku=sku, quantity=quantity, event_type="reserved"
        )
    )

    balance = (
        await session.execute(
            select(InventoryBalance).where(InventoryBalance.sku == sku).with_for_update()
        )
    ).scalar_one_or_none()
    if balance is not None:
        balance.available -= quantity

    logger.info(
        "reserved event processed: reservation_id=%s sku=%s quantity=%d",
        reservation_id,
        sku,
        quantity,
    )
    return True


async def _apply_committed(session: AsyncSession, fields: dict) -> bool:
    reservation_id = uuid.UUID(fields["reservation_id"])
    sku = fields["sku"]
    quantity = int(fields.get("quantity", 0))

    reservation = await session.get(InventoryReservation, reservation_id)
    if reservation is None or reservation.status != "held":
        logger.info("skipped committed event: reservation_id=%s not held", reservation_id)
        return False

    reservation.status = "committed"
    reservation.resolved_at = datetime.now(timezone.utc)
    session.add(
        StockAuditLedger(
            reservation_id=reservation_id, sku=sku, quantity=quantity, event_type="committed"
        )
    )

    logger.info("committed event processed: reservation_id=%s sku=%s", reservation_id, sku)
    return True


async def process_batch(session: AsyncSession, messages: list[tuple[str, dict]]) -> int:
    """Persists 'reserved'/'committed' events — the async half of the order
    lifecycle (ORDER_LIFECYCLE.md "01 — Reserve", "Commit")."""
    processed = 0

    for _message_id, fields in messages:
        event_type = fields.get("event_type")
        if event_type == "reserved":
            processed += await _apply_reserved(session, fields)
        elif event_type == "committed":
            processed += await _apply_committed(session, fields)

    await session.commit()
    return processed


async def run_once(consumer_name: str) -> int:
    """One XREADGROUP -> process -> XACK cycle. Returns events processed."""
    settings = get_settings()
    stream = settings.stream_inventory_events
    group = settings.consumer_group_inventory_sync

    response = await redis_client.xreadgroup(
        group, consumer_name, {stream: ">"}, count=BATCH_SIZE, block=BLOCK_MS
    )
    if not response:
        return 0

    total = 0
    async with AsyncSessionLocal() as session:
        for _stream_name, messages in response:
            total += await process_batch(session, messages)
            message_ids = [message_id for message_id, _ in messages]
            await redis_client.xack(stream, group, *message_ids)

    return total


async def run() -> None:  # pragma: no cover -- infinite poll loop, verified live via Docker
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    consumer_name = f"worker-{uuid.uuid4().hex[:8]}"

    await ensure_consumer_group(
        redis_client, settings.stream_inventory_events, settings.consumer_group_inventory_sync
    )
    logger.info("inventory sync worker started, consumer=%s", consumer_name)

    while True:
        processed = await run_once(consumer_name)
        if processed:
            logger.info("processed %d event(s)", processed)


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(run())
