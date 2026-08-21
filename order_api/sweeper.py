import logging
import re
import time
import uuid
from pathlib import Path

from prometheus_client import start_http_server
from redis.asyncio import Redis

from order_api.core.config import get_settings
from order_api.core.db.redis import redis_client
from order_api.core.metrics import (
    SWEEPER_EXPIRED_HOLDS,
    SWEEPER_LAST_ACTIVITY,
    SWEEPER_SWEEP_DURATION,
)

logger = logging.getLogger("order_api.sweeper")

_LUA_DIR = Path(__file__).resolve().parent / "core" / "lua"
_SWEEP_SCRIPT_PATH = _LUA_DIR / "sweep_release.lua"

_HOLD_META_PATTERN = re.compile(
    r"^holdmeta:(?P<reservation_id>[0-9a-fA-F-]{36}):(?P<sku>[^:]+):(?P<quantity>\d+)$"
)


def parse_hold_meta_key(key: str) -> tuple[uuid.UUID, str, int] | None:
    """Recovers (reservation_id, sku, quantity) from an expired key's name —
    its value is already gone by the time the notification arrives."""
    match = _HOLD_META_PATTERN.match(key)
    if match is None:
        return None
    return (
        uuid.UUID(match.group("reservation_id")),
        match.group("sku"),
        int(match.group("quantity")),
    )


async def ensure_expiry_notifications(redis: Redis) -> None:
    """Turns on Redis keyspace notifications for expired keys (E+x) if not
    already enabled — required for anything to reach our pub/sub subscription."""
    current = await redis.config_get("notify-keyspace-events")
    flags = current.get("notify-keyspace-events", "")
    missing = [flag for flag in ("E", "x") if flag not in flags]
    if missing:
        await redis.config_set("notify-keyspace-events", flags + "".join(missing))


class Sweeper:
    """Reacts to abandoned (TTL-expired, never committed/released) holds —
    ORDER_LIFECYCLE.md "Release" path B."""

    def __init__(self, redis: Redis, stream_name: str, claim_ttl_seconds: int):
        self.redis = redis
        self.stream_name = stream_name
        self.claim_ttl_seconds = claim_ttl_seconds
        self._sweep_script = redis.register_script(_SWEEP_SCRIPT_PATH.read_text())

    async def handle_expired_key(self, key: str) -> bool:
        parsed = parse_hold_meta_key(key)
        if parsed is None:
            return False
        reservation_id, sku, quantity = parsed

        with SWEEPER_SWEEP_DURATION.time():
            status, available = await self._sweep_script(
                keys=[
                    f"sweep_claim:{reservation_id}",
                    f"hold:{reservation_id}",
                    self.stream_name,
                ],
                args=[str(reservation_id), sku, quantity, self.claim_ttl_seconds],
            )
        status = status.decode() if isinstance(status, bytes) else status

        if status == "already_claimed":
            logger.info("sweep skipped, already claimed: reservation_id=%s", reservation_id)
            return False

        SWEEPER_EXPIRED_HOLDS.inc()
        logger.info(
            "swept abandoned hold: reservation_id=%s sku=%s quantity=%d available=%s",
            reservation_id,
            sku,
            quantity,
            available,
        )
        return True


async def run() -> None:  # pragma: no cover -- infinite pub/sub loop, verified live via Docker
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    start_http_server(settings.sweeper_metrics_port)
    await ensure_expiry_notifications(redis_client)
    sweeper = Sweeper(
        redis=redis_client,
        stream_name=settings.stream_inventory_events,
        claim_ttl_seconds=settings.sweep_claim_ttl_seconds,
    )

    pubsub = redis_client.pubsub()
    await pubsub.psubscribe("__keyevent@*__:expired")
    logger.info("sweeper started, listening for expired holds")

    async for message in pubsub.listen():
        if message["type"] != "pmessage":
            continue
        SWEEPER_LAST_ACTIVITY.set(time.time())
        try:
            await sweeper.handle_expired_key(message["data"])
        except Exception:
            logger.exception("sweep failed for key=%s", message["data"])


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(run())
