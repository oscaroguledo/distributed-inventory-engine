-- Atomic sweep-release for an abandoned (TTL-expired) hold — same outcome
-- as release.lua, triggered by the sweeper instead of a client request.
-- KEYS=[claim,hold,stream]; ARGV=[reservation_id,sku,quantity,claim_ttl].
-- Returns {status, available}.
--
-- Keyspace notifications fan out to every subscriber, so a claim key
-- (SET NX) guards against two sweeper replicas both restoring the same
-- hold's stock.

local claim_key = KEYS[1]
local hold_key = KEYS[2]
local stream_key = KEYS[3]

local reservation_id = ARGV[1]
local sku = ARGV[2]
local quantity = tonumber(ARGV[3])
local claim_ttl = tonumber(ARGV[4])

if redis.call("SET", claim_key, "1", "NX", "EX", claim_ttl) == false then
    return {"already_claimed", 0}
end

local available = redis.call("INCRBY", "stock:" .. sku .. ":available", quantity)
redis.call("DEL", hold_key)
redis.call(
    "XADD", stream_key, "*",
    "event_type", "released",
    "reservation_id", reservation_id,
    "sku", sku,
    "quantity", quantity
)

return {"released", available}
