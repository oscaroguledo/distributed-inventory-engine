-- Atomic reserve (idempotency+stock check+decrement+hold+meta+stream, one op).
-- KEYS=[stock,hold,stream]; ARGV=[sku,qty,id,hold_ttl]. holdmeta:{id}:{sku}:{qty}
-- lets the sweeper recover sku/qty once the hold hash's TTL wipes it.

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
redis.call("SET", "holdmeta:" .. reservation_id .. ":" .. sku .. ":" .. quantity, "1", "EX", hold_ttl)
redis.call(
    "XADD", stream_key, "*",
    "event_type", "reserved",
    "reservation_id", reservation_id,
    "sku", sku,
    "quantity", quantity
)

return {"held", redis.call("GET", stock_key)}
