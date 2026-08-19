-- Atomic token-bucket check. KEYS[1]=bucket key; ARGV=[capacity,
-- refill_rate,requested,now,ttl]. Returns {allowed(1/0), tokens_remaining}.

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local ttl_seconds = tonumber(ARGV[5])

local bucket = redis.call("HMGET", key, "tokens", "last_refill")
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
    last_refill = now
end

local elapsed = math.max(0, now - last_refill)
tokens = math.min(capacity, tokens + elapsed * refill_rate)

local allowed = 0
if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
end

redis.call("HSET", key, "tokens", tokens, "last_refill", now)
redis.call("EXPIRE", key, ttl_seconds)

return {allowed, tokens}
