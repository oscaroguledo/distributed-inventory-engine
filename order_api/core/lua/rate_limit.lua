-- Atomic token-bucket rate limiter.
-- Runs as one indivisible Redis operation: no two concurrent callers can
-- both read the same token count and both decide they're allowed.
--
-- KEYS[1] = bucket key (e.g. "ratelimit:<client>")
-- ARGV[1] = capacity            (max tokens the bucket can hold)
-- ARGV[2] = refill_rate         (tokens added per second)
-- ARGV[3] = requested           (tokens this call wants to spend)
-- ARGV[4] = now                 (unix timestamp, float seconds)
-- ARGV[5] = ttl_seconds         (expire the bucket key when idle)
--
-- Returns {allowed, tokens_remaining} — allowed is 1 or 0.

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
