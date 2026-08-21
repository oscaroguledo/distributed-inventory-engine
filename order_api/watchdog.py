import asyncio
import logging
import time

from prometheus_client import start_http_server
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from order_api.core.config import get_settings
from order_api.core.db.postgresql import AsyncSessionLocal
from order_api.core.db.redis import redis_client
from order_api.core.metrics import WATCHDOG_DRIFT_MAGNITUDE, WATCHDOG_LAST_RUN, WATCHDOG_REBUILDS
from order_api.models.inventory_balances import InventoryBalance
from order_api.models.reconciliation_log import ReconciliationLog

logger = logging.getLogger("order_api.watchdog")


class ReconciliationWatchdog:
    """Diffs Redis's stock counters against the Postgres ledger and rebuilds
    Redis on persistent drift — SYSTEM_DESIGN.md "Closing the durability gap"."""

    def __init__(
        self,
        redis: Redis,
        session_factory: async_sessionmaker,
        confirm_passes: int = 2,
    ):
        self.redis = redis
        self.session_factory = session_factory
        self.confirm_passes = confirm_passes
        self._drift_streak: dict[str, int] = {}

    async def run_once(self) -> dict[str, int]:
        """One diff pass over every known SKU. Returns {sku: drift} for SKUs rebuilt this pass."""
        rebuilt: dict[str, int] = {}

        async with self.session_factory() as session:
            balances = (await session.execute(select(InventoryBalance))).scalars().all()

            for balance in balances:
                sku = balance.sku
                redis_key = f"stock:{sku}:available"
                raw_value = await self.redis.get(redis_key)
                redis_available = int(raw_value) if raw_value is not None else None

                if redis_available == balance.available:
                    self._drift_streak.pop(sku, None)
                    continue

                streak = self._drift_streak.get(sku, 0) + 1
                self._drift_streak[sku] = streak
                drift = (redis_available or 0) - balance.available
                WATCHDOG_DRIFT_MAGNITUDE.labels(sku=sku).set(abs(drift))

                logger.warning(
                    "drift detected: sku=%s redis=%s postgres=%d drift=%d streak=%d",
                    sku,
                    redis_available,
                    balance.available,
                    drift,
                    streak,
                )

                if streak >= self.confirm_passes:
                    await self.redis.set(redis_key, balance.available)
                    session.add(
                        ReconciliationLog(
                            sku=sku,
                            redis_available=redis_available or 0,
                            postgres_available=balance.available,
                            drift=drift,
                        )
                    )
                    await session.commit()
                    WATCHDOG_REBUILDS.labels(sku=sku).inc()
                    logger.warning(
                        "rebuilt redis from postgres: sku=%s postgres=%d (was %s)",
                        sku,
                        balance.available,
                        redis_available,
                    )
                    rebuilt[sku] = drift
                    self._drift_streak.pop(sku, None)

        WATCHDOG_LAST_RUN.set(time.time())
        return rebuilt


async def run() -> None:  # pragma: no cover -- infinite poll loop, verified live via Docker
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    start_http_server(settings.watchdog_metrics_port)
    watchdog = ReconciliationWatchdog(
        redis=redis_client,
        session_factory=AsyncSessionLocal,
        confirm_passes=settings.watchdog_confirm_passes,
    )
    logger.info(
        "reconciliation watchdog started, poll_interval=%ds confirm_passes=%d",
        settings.watchdog_poll_interval_seconds,
        settings.watchdog_confirm_passes,
    )

    while True:
        try:
            await watchdog.run_once()
        except Exception:
            logger.exception("watchdog poll failed, retrying next interval")
        await asyncio.sleep(settings.watchdog_poll_interval_seconds)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run())
