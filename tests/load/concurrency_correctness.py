"""Concurrency correctness load test — SYSTEM_DESIGN.md "Proving correctness
under concurrency". Needs a live order_api+Redis+Postgres; run via docker-compose."""

import asyncio
import os
import random
import sys
import uuid

import httpx
from redis.asyncio import Redis
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from order_api.core.config import get_settings
from order_api.models.inventory_balances import InventoryBalance
from order_api.models.inventory_reservations import InventoryReservation
from order_api.models.stock_audit_ledger import StockAuditLedger

TOTAL_STOCK = 100
TOTAL_REQUESTS = 5000
CONCURRENCY = 200
RESERVE_WEIGHT = 0.85
# low: commit permanently retires a unit — keeps the pool contended for
# the whole run instead of fully committed away early. rest (0.12) is release.
COMMIT_WEIGHT = 0.03

CONVERGENCE_TIMEOUT_SECONDS = 20
DRAIN_TIMEOUT_SECONDS = 20


class State:
    def __init__(self, sku: str):
        self.sku = sku
        self.held: set[uuid.UUID] = set()
        self.committed: set[uuid.UUID] = set()
        self.failures: list[str] = []


async def do_reserve(client: httpx.AsyncClient, state: State) -> None:
    reservation_id = uuid.uuid4()
    resp = await client.post(
        "/reserve",
        json={"sku": state.sku, "quantity": 1, "reservation_id": str(reservation_id)},
    )
    if resp.status_code == 201:
        state.held.add(reservation_id)


async def do_commit(client: httpx.AsyncClient, state: State) -> None:
    if not state.held:
        return await do_reserve(client, state)
    reservation_id = random.choice(tuple(state.held))
    resp = await client.post("/commit", json={"reservation_id": str(reservation_id)})
    if resp.status_code == 200:
        state.held.discard(reservation_id)
        state.committed.add(reservation_id)
    elif resp.status_code == 404:
        state.held.discard(reservation_id)  # a racing commit/release already won


async def do_release(client: httpx.AsyncClient, state: State) -> None:
    if not state.held:
        return await do_reserve(client, state)
    reservation_id = random.choice(tuple(state.held))
    resp = await client.post("/release", json={"reservation_id": str(reservation_id)})
    if resp.status_code in (200, 404):
        state.held.discard(reservation_id)  # released, or a racing call already won


async def do_action(client: httpx.AsyncClient, state: State) -> None:
    roll = random.random()
    if roll < RESERVE_WEIGHT:
        await do_reserve(client, state)
    elif roll < RESERVE_WEIGHT + COMMIT_WEIGHT:
        await do_commit(client, state)
    else:
        await do_release(client, state)


async def checkpoint(redis: Redis, state: State, batch_num: int) -> None:
    available = int(await redis.get(f"stock:{state.sku}:available"))
    expected = TOTAL_STOCK - len(state.held) - len(state.committed)
    print(
        f"[batch {batch_num:>3}] available={available} held={len(state.held)} "
        f"committed={len(state.committed)} expected={expected}"
    )
    if available < 0:
        state.failures.append(f"batch {batch_num}: available went negative ({available})")
    if available != expected:
        state.failures.append(
            f"batch {batch_num}: total_stock == available + holds + committed violated — "
            f"available={available}, expected={expected}"
        )


async def run_burst(base_url: str, redis: Redis, state: State) -> None:
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        remaining = TOTAL_REQUESTS
        batch_num = 0
        while remaining > 0 and not state.failures:
            batch_size = min(CONCURRENCY, remaining)
            await asyncio.gather(*(do_action(client, state) for _ in range(batch_size)))
            remaining -= batch_size
            batch_num += 1
            await checkpoint(redis, state, batch_num)


async def wait_for_convergence(session_factory, redis: Redis, state: State) -> None:
    for _ in range(CONVERGENCE_TIMEOUT_SECONDS):
        async with session_factory() as session:
            balance = await session.get(InventoryBalance, state.sku)
        redis_available = int(await redis.get(f"stock:{state.sku}:available"))
        if balance.available == redis_available:
            print(f"postgres converged: available={balance.available}")
            return
        await asyncio.sleep(1)

    state.failures.append(
        f"postgres never converged with redis within {CONVERGENCE_TIMEOUT_SECONDS}s — "
        f"postgres={balance.available}, redis={redis_available}"
    )


async def wait_for_drain(redis: Redis, settings) -> None:
    """Waits for the consumer group's pending-entries list to empty out —
    a deferred commit/release (worker.py's xautoclaim retry) can otherwise
    still be reclaimed after cleanup deletes this sku's rows, logging a
    harmless-but-noisy FK violation against already-cleaned-up data."""
    stream = settings.stream_inventory_events
    group = settings.consumer_group_inventory_sync
    for _ in range(DRAIN_TIMEOUT_SECONDS):
        summary = await redis.xpending(stream, group)
        if not summary or not summary.get("pending"):
            return
        await asyncio.sleep(1)
    print("warning: consumer group still has pending entries after drain timeout")


async def cleanup(session_factory, redis: Redis, sku: str) -> None:
    async with session_factory() as session:
        await session.execute(delete(StockAuditLedger).where(StockAuditLedger.sku == sku))
        await session.execute(delete(InventoryReservation).where(InventoryReservation.sku == sku))
        await session.execute(delete(InventoryBalance).where(InventoryBalance.sku == sku))
        await session.commit()
    await redis.delete(f"stock:{sku}:available")


async def main() -> int:
    settings = get_settings()
    base_url = os.environ.get("LOAD_TEST_BASE_URL", "http://localhost:8000")
    sku = f"LOADTEST-{uuid.uuid4().hex[:8]}"

    engine = create_async_engine(settings.postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)

    async with session_factory() as session:
        session.add(
            InventoryBalance(sku=sku, name=sku, total_stock=TOTAL_STOCK, available=TOTAL_STOCK)
        )
        await session.commit()
    await redis.set(f"stock:{sku}:available", TOTAL_STOCK)

    state = State(sku=sku)
    print(f"sku={sku} base_url={base_url} requests={TOTAL_REQUESTS} concurrency={CONCURRENCY}")

    try:
        await run_burst(base_url, redis, state)
        if not state.failures:
            await wait_for_convergence(session_factory, redis, state)
            await wait_for_drain(redis, settings)
    finally:
        await cleanup(session_factory, redis, sku)
        await redis.aclose()
        await engine.dispose()

    if state.failures:
        print("FAILED:")
        for failure in state.failures:
            print(f"  - {failure}")
        return 1

    print(
        f"PASSED: {TOTAL_REQUESTS} requests, held={len(state.held)}, "
        f"committed={len(state.committed)}, no oversell, redis/postgres converged"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
