import pytest
from httpx import ASGITransport, AsyncClient

from order_api.core.db.postgresql import get_session
from order_api.core.db.redis import get_redis
from order_api.main import app


class _OkSession:
    async def execute(self, *args, **kwargs):
        return None


class _FailingSession:
    async def execute(self, *args, **kwargs):
        raise RuntimeError("db unreachable")


class _OkRedis:
    async def ping(self):
        return True


class _FailingRedis:
    async def ping(self):
        raise RuntimeError("redis unreachable")


async def _session_ok():
    yield _OkSession()


async def _session_fail():
    yield _FailingSession()


async def _redis_ok():
    return _OkRedis()


async def _redis_fail():
    return _FailingRedis()


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


async def _get(path: str):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path)


@pytest.mark.asyncio
async def test_health_ok_when_postgres_and_redis_are_up():
    app.dependency_overrides[get_session] = _session_ok
    app.dependency_overrides[get_redis] = _redis_ok

    response = await _get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"] == {"order-api": "ok", "postgres": "ok", "redis": "ok"}


@pytest.mark.asyncio
async def test_health_reports_503_when_postgres_is_down(caplog):
    app.dependency_overrides[get_session] = _session_fail
    app.dependency_overrides[get_redis] = _redis_ok

    with caplog.at_level("WARNING"):
        response = await _get("/health")

    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["data"]["order-api"] == "ok"
    assert body["data"]["postgres"] == "unreachable"
    assert body["data"]["redis"] == "ok"
    assert any("postgres unreachable" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_health_reports_503_when_redis_is_down(caplog):
    app.dependency_overrides[get_session] = _session_ok
    app.dependency_overrides[get_redis] = _redis_fail

    with caplog.at_level("WARNING"):
        response = await _get("/health")

    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["data"]["order-api"] == "ok"
    assert body["data"]["redis"] == "unreachable"
    assert body["data"]["postgres"] == "ok"
    assert any("redis unreachable" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_health_reports_503_when_both_are_down():
    app.dependency_overrides[get_session] = _session_fail
    app.dependency_overrides[get_redis] = _redis_fail

    response = await _get("/health")

    assert response.status_code == 503
    body = response.json()
    assert body["data"] == {
        "order-api": "ok",
        "postgres": "unreachable",
        "redis": "unreachable",
    }
