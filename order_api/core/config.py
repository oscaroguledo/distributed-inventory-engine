from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Single source of truth for env-driven config. Docker Compose sets
    these directly; local (non-Docker) runs fall back to .env."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "order-api"
    log_level: str = "INFO"
    order_api_port: int = 8000

    postgres_url: str = "postgresql+asyncpg://inventory:inventory@localhost:5432/inventory"
    postgres_pool_size: int = 5
    postgres_max_overflow: int = 5

    redis_url: str = "redis://redis:6379/0"

    hold_ttl_seconds: int = 900

    rate_limit_capacity: int = 10
    rate_limit_refill_per_second: float = 1.0
    rate_limit_ttl_seconds: int = 60


@lru_cache
def get_settings() -> Settings:
    return Settings()
