-- Atomic release: lookup hold, restore stock, delete ticket, XADD released.
-- KEYS=[stock,hold,stream]; ARGV=[reservation_id]. Returns {status, sku, quantity}.

local stock_key = KEYS[1]
local hold_key = KEYS[2]
local stream_key = KEYS[3]
local reservation_id = ARGV[1]

if redis.call("EXISTS", hold_key) == 0 then
    return {"not_found", "", 0}
end

local sku = redis.call("HGET", hold_key, "sku")
local quantity = tonumber(redis.call("HGET", hold_key, "quantity"))

redis.call("INCRBY", stock_key, quantity)
redis.call("DEL", hold_key)
redis.call(
    "XADD", stream_key, "*",
    "event_type", "released",
    "reservation_id", reservation_id,
    "sku", sku,
    "quantity", quantity
)

return {"released", sku, quantity}
