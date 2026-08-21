import uuid

import pytest

from order_api.core.metrics import SWEEPER_EXPIRED_HOLDS
from order_api.sweeper import Sweeper, ensure_expiry_notifications, parse_hold_meta_key


class _FakeRedis:
    """Simulates just enough of Redis to test the sweeper's own logic —
    the Lua script's actual atomicity is verified live via Docker."""

    def __init__(self, already_claimed: bool = False, config: dict | None = None):
        self.already_claimed = already_claimed
        self.script_calls: list[tuple[list, list]] = []
        self.config = config or {}
        self.config_set_calls: list[tuple[str, str]] = []

    def register_script(self, _script_body):
        return self._sweep

    async def _sweep(self, keys, args):
        self.script_calls.append((keys, args))
        if self.already_claimed:
            return ["already_claimed", 0]
        return ["released", 42]

    async def config_get(self, parameter):
        return self.config

    async def config_set(self, parameter, value):
        self.config_set_calls.append((parameter, value))


def test_parse_hold_meta_key_extracts_fields():
    reservation_id = uuid.uuid4()
    key = f"holdmeta:{reservation_id}:WIDGET-1:7"

    parsed = parse_hold_meta_key(key)

    assert parsed == (reservation_id, "WIDGET-1", 7)


def test_parse_hold_meta_key_ignores_unrelated_keys():
    assert parse_hold_meta_key("hold:some-reservation") is None
    assert parse_hold_meta_key("stock:WIDGET-1:available") is None
    assert parse_hold_meta_key("holdmeta:not-a-uuid:WIDGET-1:7") is None


@pytest.mark.asyncio
async def test_ensure_expiry_notifications_sets_missing_flags():
    fake_redis = _FakeRedis(config={"notify-keyspace-events": ""})

    await ensure_expiry_notifications(fake_redis)

    assert fake_redis.config_set_calls == [("notify-keyspace-events", "Ex")]


@pytest.mark.asyncio
async def test_ensure_expiry_notifications_skips_when_already_enabled():
    fake_redis = _FakeRedis(config={"notify-keyspace-events": "gxE"})

    await ensure_expiry_notifications(fake_redis)

    assert fake_redis.config_set_calls == []


@pytest.mark.asyncio
async def test_ensure_expiry_notifications_preserves_other_flags():
    fake_redis = _FakeRedis(config={"notify-keyspace-events": "g"})

    await ensure_expiry_notifications(fake_redis)

    assert fake_redis.config_set_calls == [("notify-keyspace-events", "gEx")]


@pytest.mark.asyncio
async def test_handle_expired_key_ignores_unrelated_keys():
    fake_redis = _FakeRedis()
    sweeper = Sweeper(redis=fake_redis, stream_name="stream:x", claim_ttl_seconds=30)

    handled = await sweeper.handle_expired_key("some:unrelated:key")

    assert handled is False
    assert fake_redis.script_calls == []


@pytest.mark.asyncio
async def test_handle_expired_key_sweeps_a_hold_meta_key(caplog):
    fake_redis = _FakeRedis()
    sweeper = Sweeper(redis=fake_redis, stream_name="stream:x", claim_ttl_seconds=30)
    reservation_id = uuid.uuid4()
    before = SWEEPER_EXPIRED_HOLDS._value.get()

    with caplog.at_level("INFO"):
        handled = await sweeper.handle_expired_key(f"holdmeta:{reservation_id}:WIDGET-1:7")

    assert handled is True
    assert SWEEPER_EXPIRED_HOLDS._value.get() == before + 1
    keys, args = fake_redis.script_calls[0]
    assert keys == [
        f"sweep_claim:{reservation_id}",
        f"hold:{reservation_id}",
        "stream:x",
    ]
    assert args == [str(reservation_id), "WIDGET-1", 7, 30]
    assert any(
        "swept abandoned hold" in record.message and str(reservation_id) in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_handle_expired_key_skips_when_already_claimed(caplog):
    fake_redis = _FakeRedis(already_claimed=True)
    sweeper = Sweeper(redis=fake_redis, stream_name="stream:x", claim_ttl_seconds=30)
    reservation_id = uuid.uuid4()
    before = SWEEPER_EXPIRED_HOLDS._value.get()

    with caplog.at_level("INFO"):
        handled = await sweeper.handle_expired_key(f"holdmeta:{reservation_id}:WIDGET-1:7")

    assert handled is False
    assert SWEEPER_EXPIRED_HOLDS._value.get() == before  # unchanged — no real sweep happened
    assert any(
        "already claimed" in record.message and str(reservation_id) in record.message
        for record in caplog.records
    )
