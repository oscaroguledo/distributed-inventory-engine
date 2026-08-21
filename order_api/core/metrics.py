from prometheus_client import Counter, Gauge, Histogram

# order_api — SYSTEM_DESIGN.md "Monitoring & observability", Reservation API row.
HTTP_REQUESTS = Counter(
    "order_api_requests_total", "HTTP requests handled", ["method", "path", "status"]
)
HTTP_REQUEST_DURATION = Histogram(
    "order_api_request_duration_seconds", "HTTP request latency", ["method", "path"]
)
RATE_LIMIT_REJECTIONS = Counter(
    "order_api_rate_limit_rejections_total", "429s returned by the rate limiter", ["path"]
)
REDIS_WAIT_TIMEOUTS = Counter(
    "order_api_redis_wait_timeouts_total",
    "WAIT calls that didn't reach the replica ack target in time",
    ["operation"],
)
LUA_SCRIPT_DURATION = Histogram(
    "order_api_lua_script_duration_seconds", "Redis Lua script execution latency", ["script"]
)
STOCK_AVAILABLE = Gauge(
    "order_api_stock_available", "Last-observed available count for a sku", ["sku"]
)

# worker — SYSTEM_DESIGN.md Monitoring table, Stream workers row.
WORKER_EVENTS_PROCESSED = Counter(
    "worker_events_processed_total", "Stream events applied to Postgres", ["event_type"]
)
WORKER_BATCH_DURATION = Histogram("worker_batch_duration_seconds", "process_batch() latency")
WORKER_CONSUMER_GROUP_PENDING = Gauge(
    "worker_consumer_group_pending", "XPENDING summary count for the consumer group"
)

# sweeper — Monitoring table, Sweeper row.
SWEEPER_EXPIRED_HOLDS = Counter(
    "sweeper_expired_holds_total", "Abandoned holds swept back into available stock"
)
SWEEPER_SWEEP_DURATION = Histogram("sweeper_sweep_duration_seconds", "One sweep's latency")
SWEEPER_LAST_ACTIVITY = Gauge(
    "sweeper_last_activity_timestamp_seconds", "Unix time of the last pub/sub message seen"
)

# watchdog — Monitoring table, Reconciliation watchdog row.
WATCHDOG_DRIFT_MAGNITUDE = Gauge(
    "watchdog_drift_magnitude", "Most recently observed |redis - postgres| drift", ["sku"]
)
WATCHDOG_REBUILDS = Counter(
    "watchdog_rebuilds_total", "Times Redis was rebuilt from Postgres", ["sku"]
)
WATCHDOG_LAST_RUN = Gauge(
    "watchdog_last_run_timestamp_seconds", "Unix time the watchdog last completed a poll"
)
