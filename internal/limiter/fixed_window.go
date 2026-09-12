// Package limiter contains rate limiting algorithm implementations.
// Each algorithm is a separate type that implements the same pattern:
// given a key and a policy, return whether the request is allowed.
package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Result holds the outcome of a rate limit check.
// It is returned by every limiter and used by handlers to
// set response headers and decide the HTTP status code.
type Result struct {
	Allowed   bool      // true = request should be served
	Limit     int64     // the configured maximum requests per window
	Remaining int64     // requests left in the current window
	ResetAt   time.Time // when the current window expires and the counter resets
}

// FixedWindowLimiter implements the Fixed Window Counter algorithm.
//
// How it works:
//  1. Each unique key maps to a Redis counter with a TTL equal to the window.
//  2. Every request increments the counter (INCR).
//  3. If the counter was just created (count == 1), set the TTL (EXPIRE).
//  4. If count > limit, deny the request.
//  5. When the TTL expires, Redis deletes the key. The next request starts fresh.
//
// Known limitation — the boundary burst problem:
// A client can make up to 2× the limit by sending requests at the very end
// of one window and the very start of the next. This is inherent to the
// algorithm, not a bug. The Sliding Window algorithm (Phase 7) solves this.
//
// Known limitation — INCR + EXPIRE are not atomic:
// If the process crashes between INCR and EXPIRE, the key has no TTL and
// lives forever. Phase 5 addresses this with a Lua script.
type FixedWindowLimiter struct {
	redis  *redis.Client
	limit  int64
	window time.Duration
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
	}
}

// Allow checks whether a request identified by key is within the rate limit.
// It atomically increments the counter and returns the result.
//
// The key should uniquely identify what is being limited.
// Example: "user_123:/api/orders" limits user_123 on the /api/orders endpoint.
func (l *FixedWindowLimiter) Allow(ctx context.Context, key string) (Result, error) {
	redisKey := fmt.Sprintf("ratelimit:fixed:%s", key)

	// INCR is atomic — Redis processes it as a single operation.
	// No two concurrent callers can get the same value; each gets
	// a unique monotonically increasing count.
	// If the key does not exist, Redis sets it to 0 then increments → returns 1.
	count, err := l.redis.Incr(ctx, redisKey).Result()
	if err != nil {
		return Result{}, fmt.Errorf("fixed window: INCR failed for key %q: %w", redisKey, err)
	}

	// Set the TTL only when the key is brand new (count == 1).
	// This marks the start of a fresh window.
	//
	// Why only when count == 1?
	// If we called EXPIRE on every request, we would keep pushing the
	// expiry forward and the window would never reset — the limit would
	// never be enforced.
	//
	// Race condition (will be fixed in Phase 5 with Lua):
	// If count == 1 but EXPIRE fails or the process dies here,
	// the key has no TTL and accumulates forever.
	if count == 1 {
		if err := l.redis.Expire(ctx, redisKey, l.window).Err(); err != nil {
			return Result{}, fmt.Errorf("fixed window: EXPIRE failed for key %q: %w", redisKey, err)
		}
	}

	// Ask Redis how much time is left on this key's TTL.
	// We use this to tell the client when their window resets.
	ttl, err := l.redis.TTL(ctx, redisKey).Result()
	if err != nil || ttl < 0 {
		// Non-fatal: TTL lookup failed or key has no expiry set yet.
		// Fall back to the full window duration.
		ttl = l.window
	}

	remaining := l.limit - count
	if remaining < 0 {
		remaining = 0
	}

	return Result{
		Allowed:   count <= l.limit,
		Limit:     l.limit,
		Remaining: remaining,
		ResetAt:   time.Now().Add(ttl),
	}, nil
}
