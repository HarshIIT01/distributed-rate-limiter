package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// slidingWindowScript implements the Sliding Window Log algorithm atomically.
//
// Data structure: Redis Sorted Set
//   - Score  = request timestamp in milliseconds
//   - Member = unique ID per request (Redis TIME microseconds)
//
// On every request, the script:
//  1. Removes stale timestamps (outside the window)
//  2. Counts remaining timestamps
//  3. If count < limit: adds the current timestamp and allows
//  4. If count >= limit: denies and reports when the oldest entry expires
//
// Why not a counter?
// A counter resets at a fixed boundary (Fixed Window). Sorted Set stores
// individual timestamps so we always look at exactly the last N seconds —
// the window slides forward with every request, not at fixed boundaries.
//
// Member uniqueness:
// We use Redis TIME's seconds + microseconds concatenated as the member.
// Since Lua executes atomically (no two scripts run simultaneously in Redis),
// each script gets a unique TIME value in practice.
// In extreme high-throughput scenarios, passing a client-generated UUID
// (as ARGV[4]) would be more robust — noted as a future improvement.
//
// KEYS[1]  = Redis key for this sorted set
// ARGV[1]  = limit     (max requests in the window)
// ARGV[2]  = window_ms (window duration in milliseconds)
// ARGV[3]  = ttl       (key TTL in seconds for idle cleanup)
//
// Returns: {allowed, limit, remaining, retry_after_ms}
const slidingWindowScript = `
local key       = KEYS[1]
local limit     = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local ttl       = tonumber(ARGV[3])

-- Get current time from Redis server.
-- t[1] = seconds, t[2] = microseconds
local t      = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

-- Step 1: Remove all timestamps older than the window start.
-- ZREMRANGEBYSCORE removes members with score between -inf and window_start.
-- This is what makes the window "slide" — stale entries are evicted on each request.
local window_start = now_ms - window_ms
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- Step 2: Count how many requests remain in the window.
local count = redis.call('ZCARD', key)

-- Step 3: Check and add.
local allowed = 0
if count < limit then
    -- Add this request's timestamp to the log.
    -- Member = concatenation of seconds and microseconds = unique per Lua execution
    -- (since Lua is atomic, no two executions share the same TIME value)
    local member = tostring(t[1]) .. tostring(t[2])
    redis.call('ZADD', key, now_ms, member)
    redis.call('EXPIRE', key, ttl)
    count = count + 1
    allowed = 1
end

local remaining = limit - count
if remaining < 0 then remaining = 0 end

-- Step 4: Calculate retry_after_ms for denied requests.
-- The client can retry when the oldest request in the log exits the window.
-- retry_after = (oldest_timestamp + window_ms) - now_ms
local retry_after_ms = 0
if allowed == 0 then
    -- ZRANGE key 0 0 WITHSCORES returns the member with the lowest score
    -- (the oldest request timestamp) along with its score.
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    if oldest and #oldest >= 2 then
        local oldest_ms = tonumber(oldest[2])
        retry_after_ms = math.max(1, (oldest_ms + window_ms) - now_ms)
    end
end

return {allowed, limit, remaining, retry_after_ms}
`

// SlidingWindowLimiter implements the Sliding Window Log algorithm.
//
// It tracks the exact timestamp of every request in a Redis Sorted Set.
// The window slides continuously — there are no fixed reset boundaries.
//
// Accuracy: Perfect. No boundary burst problem.
//
// Cost: Memory grows with request volume (O(limit) per key), unlike
// Fixed Window and Token Bucket which use O(1) per key.
//
// Best used for: strict API limits where accuracy matters more than
// memory efficiency (e.g., payment APIs, security-sensitive endpoints).
type SlidingWindowLimiter struct {
	redis  *redis.Client
	limit  int64
	window time.Duration
	script *redis.Script
}

// NewSlidingWindowLimiter creates a new SlidingWindowLimiter.
//
//   - limit:  maximum requests allowed within the window
//   - window: duration of the sliding window (e.g. 1 * time.Minute)
func NewSlidingWindowLimiter(redisClient *redis.Client, limit int64, window time.Duration) *SlidingWindowLimiter {
	return &SlidingWindowLimiter{
		redis:  redisClient,
		limit:  limit,
		window: window,
		script: redis.NewScript(slidingWindowScript),
	}
}

// Allow checks whether a request for the given key is within the rate limit.
func (l *SlidingWindowLimiter) Allow(ctx context.Context, key string) (Result, error) {
	redisKey := fmt.Sprintf("ratelimit:sliding:%s", key)
	windowMs := l.window.Milliseconds()
	// TTL = window duration + small buffer.
	// The sorted set can be deleted once no requests remain in the window.
	ttlSeconds := int64(l.window.Seconds()) + 10

	res, err := l.script.Run(ctx, l.redis,
		[]string{redisKey},
		l.limit, windowMs, ttlSeconds,
	).Int64Slice()
	if err != nil {
		return Result{}, fmt.Errorf("sliding window: Lua script failed for key %q: %w", redisKey, err)
	}

	// Parse result: {allowed, limit, remaining, retry_after_ms}
	allowed := res[0] == 1
	limit := res[1]
	remaining := res[2]
	retryAfterMs := res[3]

	resetAt := time.Now()
	if !allowed {
		resetAt = time.Now().Add(time.Duration(retryAfterMs) * time.Millisecond)
	}

	return Result{
		Allowed:   allowed,
		Limit:     limit,
		Remaining: remaining,
		ResetAt:   resetAt,
	}, nil
}
