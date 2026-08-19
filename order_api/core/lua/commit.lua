-- Atomic commit: lookup hold, delete it, XADD committed (no stock change).
-- KEYS=[hold,stream]; ARGV=[reservation_id]. Returns {status, sku}.

local hold_key = KEYS[1]
local stream_key = KEYS[2]
local reservation_id = ARGV[1]

if redis.call("EXISTS", hold_key) == 0 then
    return {"not_found", ""}
end

local sku = redis.call("HGET", hold_key, "sku")
local quantity = redis.call("HGET", hold_key, "quantity")

redis.call("DEL", hold_key)
redis.call(
    "XADD", stream_key, "*",
    "event_type", "committed",
    "reservation_id", reservation_id,
    "sku", sku,
    "quantity", quantity
)

return {"committed", sku}
