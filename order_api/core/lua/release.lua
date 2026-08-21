-- Atomic release: lookup hold, restore stock, delete it + its holdmeta twin.
-- KEYS=[hold,stream]; ARGV=[reservation_id]. Returns {status, sku, quantity, available}.

local hold_key = KEYS[1]
local stream_key = KEYS[2]
local reservation_id = ARGV[1]
local traceparent = ARGV[2]

if redis.call("EXISTS", hold_key) == 0 then
    return {"not_found", "", 0, 0}
end

local sku = redis.call("HGET", hold_key, "sku")
local quantity = tonumber(redis.call("HGET", hold_key, "quantity"))

local available = redis.call("INCRBY", "stock:" .. sku .. ":available", quantity)
redis.call("DEL", hold_key)
redis.call("DEL", "holdmeta:" .. reservation_id .. ":" .. sku .. ":" .. quantity)
redis.call(
    "XADD", stream_key, "*",
    "event_type", "released",
    "reservation_id", reservation_id,
    "sku", sku,
    "quantity", quantity,
    "traceparent", traceparent
)

return {"released", sku, quantity, available}
