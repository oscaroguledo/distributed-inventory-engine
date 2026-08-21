from prometheus_client import Counter, Gauge, Histogram

# order_api only — SYSTEM_DESIGN.md "Monitoring & observability", Reservation
# API row. worker/sweeper/watchdog define their own metrics in their own
# modules, not here — a shared module would make every process register
# (and expose as a phantom zero) every other service's metrics too.
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
