package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketScript is the Lua script that runs the entire Token Bucket
// algorithm as one atomic operation on the Redis server.
//
// Why Lua?
// The token bucket requires: read state → calculate → update state → return.
// If this were split across multiple Go ↔ Redis round trips, two concurrent
// requests could read the same stale token count and both be allowed when
// only one should be. Lua executes atomically — no other Redis command
// can run between any two lines of this script.
//
// Why Redis TIME?
// In a distributed setup, different application servers may have slightly
// different system clocks (clock skew). Using redis.call('TIME') ensures
// all instances use the same time source for refill calculations,
// preventing a fast-clocked server from refilling tokens prematurely.
//
// KEYS[1]  = Redis key for this bucket (e.g. "ratelimit:token_bucket:user_123")
// ARGV[1]  = capacity     (integer: max tokens in the bucket)
// ARGV[2]  = refill_rate  (float:   tokens added per second)
// ARGV[3]  = ttl          (integer: seconds before an idle key is deleted)
//
// Returns a 4-element array:
//
//	[0] allowed        (1 = allow, 0 = deny)
//	[1] capacity       (echoed back for headers)
//	[2] remaining      (floor of current tokens after this request)
//	[3] retry_after_ms (milliseconds until next token available; 0 if allowed)
const tokenBucketScript = `
local key         = KEYS[1]
local capacity    = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local ttl         = tonumber(ARGV[3])

-- Use Redis server time for a consistent clock across all app instances.
-- TIME returns {unix_seconds, microseconds}.
-- We convert to milliseconds for sub-second precision.
local t          = redis.call('TIME')
local now_ms     = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

-- Load existing bucket state.
-- HMGET returns an array of values; nil if the field does not exist.
local state          = redis.call('HMGET', key, 'tokens', 'last_refill_ms')
local current_tokens = tonumber(state[1])
local last_refill_ms = tonumber(state[2])

-- First request for this key: initialise with a full bucket.
-- We intentionally start full — a new client deserves their full burst allowance.
if current_tokens == nil then
    current_tokens = capacity
    last_refill_ms = now_ms
end

-- Calculate refill.
-- elapsed_seconds = time since we last updated this bucket.
-- tokens_to_add   = elapsed × rate (e.g. 2.5s × 10 tokens/s = 25 tokens).
local elapsed_ms      = now_ms - last_refill_ms
local elapsed_seconds = elapsed_ms / 1000.0
local tokens_to_add   = elapsed_seconds * refill_rate

-- Apply refill, respecting the capacity ceiling.
current_tokens = math.min(capacity, current_tokens + tokens_to_add)

-- Attempt to consume one token.
local allowed = 0
if current_tokens >= 1.0 then
    current_tokens = current_tokens - 1.0
    allowed = 1
end

-- Persist updated state.
-- We always update last_refill_ms to now, even on denial, so the next
-- request calculates elapsed time from this moment forward.
redis.call('HSET', key,
    'tokens',         tostring(current_tokens),
    'last_refill_ms', tostring(now_ms))

-- Set TTL so idle buckets are cleaned up automatically.
-- If no requests arrive for 'ttl' seconds, Redis deletes the key.
redis.call('EXPIRE', key, ttl)

-- Calculate remaining (how many more requests can be made immediately).
local remaining = math.floor(current_tokens)

-- Calculate retry_after_ms: how many ms until 1 token refills.
-- Formula: (1.0 - current_tokens) / refill_rate * 1000
-- This tells the denied client exactly when to retry.
local retry_after_ms = 0
if allowed == 0 then
    retry_after_ms = math.ceil((1.0 - current_tokens) / refill_rate * 1000)
end

return {allowed, capacity, remaining, retry_after_ms}
`

// TokenBucketLimiter implements the Token Bucket rate limiting algorithm.
//
// Behaviour:
//   - New clients start with a full bucket (capacity tokens).
//   - Each request consumes 1 token.
//   - Tokens refill at refillRate per second, up to capacity.
//   - Requests when the bucket is empty are denied.
//
// This allows short bursts (up to capacity) while enforcing a sustained
// average rate (refillRate requests/second) over the long term.
type TokenBucketLimiter struct {
	redis       *redis.Client
	capacity    int64
	refillRate  float64       // tokens per second
	idleTTL     time.Duration // how long before an idle key is deleted
	script      *redis.Script
}

// NewTokenBucketLimiter creates a new TokenBucketLimiter.
//
//   - capacity:   maximum number of tokens (burst limit)
//   - refillRate: tokens added per second (sustained rate)
//
// Example: capacity=100, refillRate=10 means:
//   - A client can burst up to 100 requests immediately.
//   - After exhausting the bucket, they get 10 new requests every second.
func NewTokenBucketLimiter(redisClient *redis.Client, capacity int64, refillRate float64) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		redis:      redisClient,
		capacity:   capacity,
		refillRate: refillRate,
		// TTL = time to fully refill from empty + 10s buffer.
		// If a client is idle this long, their bucket key is deleted.
		// The next request will initialise a fresh full bucket.
		idleTTL: time.Duration(float64(capacity)/refillRate)*time.Second + 10*time.Second,
		script:  redis.NewScript(tokenBucketScript),
	}
}

// Allow checks whether a request for the given key is within rate limit.
// The entire token check-and-consume is atomic via Lua script.
func (l *TokenBucketLimiter) Allow(ctx context.Context, key string) (Result, error) {
	redisKey := fmt.Sprintf("ratelimit:token_bucket:%s", key)
	ttlSeconds := int64(l.idleTTL.Seconds())

	res, err := l.script.Run(ctx, l.redis,
		[]string{redisKey},
		l.capacity, l.refillRate, ttlSeconds,
	).Int64Slice()
	if err != nil {
		return Result{}, fmt.Errorf("token bucket: Lua script failed for key %q: %w", redisKey, err)
	}

	// Parse the 4-element result: {allowed, capacity, remaining, retry_after_ms}
	allowed := res[0] == 1
	capacity := res[1]
	remaining := res[2]
	retryAfterMs := res[3]

	// Compute ResetAt: when the next token will be available.
	// For an allowed request this is time.Now() (tokens are available now).
	// For a denied request this is when 1 token will have refilled.
	resetAt := time.Now()
	if !allowed {
		resetAt = time.Now().Add(time.Duration(retryAfterMs) * time.Millisecond)
	}

	return Result{
		Allowed:   allowed,
		Limit:     capacity,
		Remaining: remaining,
		ResetAt:   resetAt,
	}, nil
}
