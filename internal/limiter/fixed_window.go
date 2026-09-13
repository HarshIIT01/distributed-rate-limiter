// Package limiter contains rate limiting algorithm implementations.
package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Result holds the outcome of a rate limit check.
type Result struct {
	Allowed   bool
	Limit     int64
	Remaining int64
	ResetAt   time.Time
}

// fixedWindowScript is a Lua script that atomically performs:
//  1. INCR the counter
//  2. If this is the first request (count == 1), set the TTL
//  3. Return the result
//
// Why Lua?
// INCR and EXPIRE are two separate Redis commands. If the process crashes
// between them, the key has no TTL and lives forever. By wrapping both in
// a Lua script, Redis executes them as a single atomic unit — either both
// happen or neither does (in the crash case, INCR already happened but
// EXPIRE is the very next operation with no interleaving possible).
//
// KEYS[1] = the Redis key
// ARGV[1] = limit (max requests)
// ARGV[2] = window size in seconds
//
// Returns a 4-element array: {allowed, limit, remaining, ttl_seconds}
// allowed: 1 = request is within limit, 0 = request exceeds limit
const fixedWindowScript = `
local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

-- INCR atomically increments the counter.
-- If the key does not exist, Redis initializes it to 0 then increments.
local count = redis.call('INCR', key)

-- Set TTL only on the first request of a new window.
-- This is now atomic with INCR — no crash can happen between them.
if count == 1 then
    redis.call('EXPIRE', key, window)
end

-- Fetch remaining TTL so the caller knows when the window resets.
local ttl = redis.call('TTL', key)

local remaining = limit - count
if remaining < 0 then remaining = 0 end

local allowed = 1
if count > limit then allowed = 0 end

return {allowed, limit, remaining, ttl}
`

// FixedWindowLimiter implements the Fixed Window Counter algorithm.
// It uses a Lua script to atomically increment and conditionally set TTL,
// eliminating the race condition between INCR and EXPIRE.
type FixedWindowLimiter struct {
	redis  *redis.Client
	limit  int64
	window time.Duration
	script *redis.Script
}

// NewFixedWindowLimiter creates a new FixedWindowLimiter.
//
//   - limit:  maximum number of requests allowed per window
//   - window: duration of each window (e.g. 1 * time.Minute)
func NewFixedWindowLimiter(redisClient *redis.Client, limit int64, window time.Duration) *FixedWindowLimiter {
	return &FixedWindowLimiter{
		redis:  redisClient,
		limit:  limit,
		window: window,
		// redis.NewScript precomputes the SHA1 hash of the script.
		// On first execution, Redis caches the script by SHA1.
		// Subsequent calls use EVALSHA (faster) instead of EVAL (sends full script).
		script: redis.NewScript(fixedWindowScript),
	}
}

// Allow checks whether a request identified by key is within the rate limit.
// The check-and-increment is performed atomically via a Lua script.
func (l *FixedWindowLimiter) Allow(ctx context.Context, key string) (Result, error) {
	redisKey := fmt.Sprintf("ratelimit:fixed:%s", key)
	windowSeconds := int64(l.window.Seconds())

	// Run executes the Lua script on Redis.
	// Redis guarantees the entire script runs atomically.
	// No other command can execute between our INCR and EXPIRE.
	res, err := l.script.Run(ctx, l.redis,
		[]string{redisKey},       // KEYS array (KEYS[1])
		l.limit, windowSeconds,   // ARGV[1], ARGV[2]
	).Int64Slice()
	if err != nil {
		return Result{}, fmt.Errorf("fixed window: Lua script failed for key %q: %w", redisKey, err)
	}

	// Parse the 4-element result from Lua: {allowed, limit, remaining, ttl}
	allowed := res[0] == 1
	limit := res[1]
	remaining := res[2]
	ttlSeconds := res[3]

	resetAt := time.Now().Add(time.Duration(ttlSeconds) * time.Second)

	return Result{
		Allowed:   allowed,
		Limit:     limit,
		Remaining: remaining,
		ResetAt:   resetAt,
	}, nil
}
