from collections.abc import AsyncGenerator

from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from order_api.core.config import get_settings

settings = get_settings()

# Pool sized small and deliberate per replica (SYSTEM_DESIGN.md "Pooled
# Postgres connections"). Both connect_args disabled for pgbouncer transaction pooling.
engine: AsyncEngine = create_async_engine(
    settings.postgres_url,
    pool_size=settings.postgres_pool_size,
    max_overflow=settings.postgres_max_overflow,
    connect_args={"statement_cache_size": 0, "prepared_statement_cache_size": 0},
)

# Instrumented once here, not in main.py/worker.py — both import this module,
# and SQLAlchemyInstrumentor has no engine-level idempotency guard, so calling
# it again per-service would double-register span-emitting event listeners.
SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)

AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    engine, expire_on_commit=False
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: `session: AsyncSession = Depends(get_session)`."""
    async with AsyncSessionLocal() as session:
        yield session
