-- Atomic reserve: idempotency + stock check + decrement + hold + stream
-- append, all as one indivisible Redis operation. See ORDER_LIFECYCLE.md
-- "01 — Reserve" for the full trace this implements.
--
-- KEYS[1] = stock:{sku}:available
-- KEYS[2] = hold:{reservation_id}
-- KEYS[3] = stream:inventory_events (name is settings-driven, passed in)
-- ARGV[1] = sku
-- ARGV[2] = quantity
-- ARGV[3] = reservation_id
-- ARGV[4] = hold_ttl_seconds
--
-- Returns {status, available} where status is one of:
--   "held" | "duplicate" | "insufficient_stock" | "unknown_sku"

local stock_key = KEYS[1]
local hold_key = KEYS[2]
local stream_key = KEYS[3]

local sku = ARGV[1]
local quantity = tonumber(ARGV[2])
local reservation_id = ARGV[3]
local hold_ttl = tonumber(ARGV[4])

-- Idempotency: a hold ticket already exists for this reservation_id, so a
-- retried request is a no-op rather than a second decrement.
if redis.call("EXISTS", hold_key) == 1 then
    local existing_available = redis.call("GET", stock_key)
    return {"duplicate", existing_available or 0}
end

-- No stock counter for this SKU at all means Redis was never seeded for it.
if redis.call("EXISTS", stock_key) == 0 then
    return {"unknown_sku", 0}
end

local available = tonumber(redis.call("GET", stock_key))
if available < quantity then
    return {"insufficient_stock", available}
end

redis.call("DECRBY", stock_key, quantity)
redis.call("HSET", hold_key, "sku", sku, "quantity", quantity, "status", "held")
redis.call("EXPIRE", hold_key, hold_ttl)
redis.call(
    "XADD", stream_key, "*",
    "event_type", "reserved",
    "reservation_id", reservation_id,
    "sku", sku,
    "quantity", quantity
)

return {"held", redis.call("GET", stock_key)}
