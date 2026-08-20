-- Atomic release: lookup hold, restore stock, delete ticket, XADD released.
-- KEYS=[hold,stream]; ARGV=[reservation_id]. Returns {status, sku, quantity}.
-- stock key isn't known until the hold is read, so it's built here rather
-- than declared upfront in KEYS.

local hold_key = KEYS[1]
local stream_key = KEYS[2]
local reservation_id = ARGV[1]

if redis.call("EXISTS", hold_key) == 0 then
    return {"not_found", "", 0}
end

local sku = redis.call("HGET", hold_key, "sku")
local quantity = tonumber(redis.call("HGET", hold_key, "quantity"))

local available = redis.call("INCRBY", "stock:" .. sku .. ":available", quantity)
redis.call("DEL", hold_key)
redis.call(
    "XADD", stream_key, "*",
    "event_type", "released",
    "reservation_id", reservation_id,
    "sku", sku,
    "quantity", quantity
)

return {"released", sku, quantity, available}
