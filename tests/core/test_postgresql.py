import importlib

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from order_api.core.config import get_settings
from order_api.core.db import postgresql


def test_default_url_uses_asyncpg_driver():
    assert postgresql.settings.postgres_url.startswith("postgresql+asyncpg://")


def test_url_reads_from_environment(monkeypatch):
    monkeypatch.setenv("POSTGRES_URL", "postgresql+asyncpg://test:test@testhost:5432/testdb")
    get_settings.cache_clear()
    importlib.reload(postgresql)

    assert postgresql.settings.postgres_url == "postgresql+asyncpg://test:test@testhost:5432/testdb"

    monkeypatch.delenv("POSTGRES_URL", raising=False)
    get_settings.cache_clear()
    importlib.reload(postgresql)


def test_engine_is_async_engine():
    assert isinstance(postgresql.engine, AsyncEngine)


def test_session_factory_produces_async_sessions():
    session = postgresql.AsyncSessionLocal()
    assert isinstance(session, AsyncSession)


@pytest.mark.asyncio
async def test_get_session_yields_and_closes_a_session():
    gen = postgresql.get_session()
    session = await anext(gen)
    assert isinstance(session, AsyncSession)

    with pytest.raises(StopAsyncIteration):
        await anext(gen)
