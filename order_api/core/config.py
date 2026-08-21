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
    # Local Docker Compose runs a single Redis, no replica — 0 means WAIT is
    # skipped entirely. Set >0 once a real Sentinel/replica topology exists.
    redis_wait_replicas: int = 0
    redis_wait_timeout_ms: int = 200

    stream_inventory_events: str = "stream:inventory_events"
    consumer_group_inventory_sync: str = "inventory_sync_workers"

    # Guards a sweep against double-restoring stock when more than one
    # sweeper replica is running (expired-key pub/sub fans out to all of them).
    sweep_claim_ttl_seconds: int = 30

    rate_limit_capacity: int = 10
    rate_limit_refill_per_second: float = 1.0
    rate_limit_ttl_seconds: int = 60

    watchdog_poll_interval_seconds: int = 30
    # Requires drift to persist across this many consecutive polls before
    # rebuilding Redis — filters out normal worker flush lag, not just noise.
    watchdog_confirm_passes: int = 2

    # /metrics ports for the background services — order_api serves its own
    # on order_api_port, these are standalone processes with no HTTP server.
    worker_metrics_port: int = 9101
    sweeper_metrics_port: int = 9102
    watchdog_metrics_port: int = 9103

    otel_exporter_otlp_endpoint: str = "jaeger:4317"


@lru_cache
def get_settings() -> Settings:
    return Settings()
