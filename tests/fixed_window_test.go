// Package tests contains integration tests that require a running Redis instance.
//
// These are NOT unit tests — they test real behaviour against a real Redis.
// Run with: go test ./tests/... -v -count=1
//
// Prerequisites:
//   - Redis running on localhost:6379
//   - Run: docker start redis-dev
package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HarshIIT01/distributed-rate-limiter/internal/limiter"
	"github.com/HarshIIT01/distributed-rate-limiter/internal/redisclient"
	"github.com/redis/go-redis/v9"
)

// newTestRedis creates a Redis client for testing.
// It calls t.Fatal if Redis is unreachable, skipping the test
// rather than failing with a confusing error.
func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	client, err := redisclient.New(redisclient.Config{
		Address:  "localhost:6379",
		Password: "",
		DB:       1, // Use DB 1 for tests to avoid polluting development data in DB 0
	})
	if err != nil {
		t.Fatalf("Redis not available — start it with: docker start redis-dev\nError: %v", err)
	}
	return client
}

// cleanupKey removes the test Redis key after the test completes.
// This prevents stale keys from affecting subsequent test runs.
func cleanupKey(t *testing.T, client *redis.Client, key string) {
	t.Helper()
	t.Cleanup(func() {
		client.Del(context.Background(), key)
	})
}

// TestFixedWindow_BasicAllow verifies that requests within the limit are allowed.
func TestFixedWindow_BasicAllow(t *testing.T) {
	client := newTestRedis(t)

	const limit = 5
	l := limiter.NewFixedWindowLimiter(client, limit, 1*time.Minute)

	key := "test:basic:user_1"
	cleanupKey(t, client, fmt.Sprintf("ratelimit:fixed:%s", key))

	for i := 1; i <= limit; i++ {
		result, err := l.Allow(context.Background(), key)
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i, err)
		}
		if !result.Allowed {
			t.Errorf("request %d: expected ALLOW, got DENY", i)
		}
		expected := int64(limit - i)
		if result.Remaining != expected {
			t.Errorf("request %d: expected remaining=%d, got remaining=%d", i, expected, result.Remaining)
		}
	}
}

// TestFixedWindow_DenyAfterLimit verifies that the (limit+1)th request is denied.
func TestFixedWindow_DenyAfterLimit(t *testing.T) {
	client := newTestRedis(t)

	const limit = 3
	l := limiter.NewFixedWindowLimiter(client, limit, 1*time.Minute)

	key := "test:deny:user_2"
	cleanupKey(t, client, fmt.Sprintf("ratelimit:fixed:%s", key))

	// Exhaust the limit.
	for i := 0; i < limit; i++ {
		if _, err := l.Allow(context.Background(), key); err != nil {
			t.Fatalf("setup request %d failed: %v", i, err)
		}
	}

	// The next request must be denied.
	result, err := l.Allow(context.Background(), key)
	if err != nil {
		t.Fatalf("unexpected error on denied request: %v", err)
	}
	if result.Allowed {
		t.Error("expected DENY for request over limit, got ALLOW")
	}
	if result.Remaining != 0 {
		t.Errorf("expected remaining=0 on denied request, got %d", result.Remaining)
	}
}

// TestFixedWindow_Concurrency is the key test of Phase 5.
//
// It fires 100 concurrent goroutines against a limit of 10.
// Each goroutine calls Allow() simultaneously.
//
// Expected result: exactly 10 requests are allowed, 90 are denied.
//
// This test verifies that the Lua script is atomic — without atomicity,
// the count could exceed the limit due to race conditions between
// read-check-write operations.
//
// With Redis INCR (even without Lua), the count is always correct because
// INCR itself is atomic. The Lua script additionally makes INCR + EXPIRE
// atomic, preventing the "key without TTL" bug.
func TestFixedWindow_Concurrency(t *testing.T) {
	client := newTestRedis(t)

	const (
		limit       = 10
		concurrency = 100
	)

	l := limiter.NewFixedWindowLimiter(client, limit, 1*time.Minute)

	key := "test:concurrency:user_3"
	cleanupKey(t, client, fmt.Sprintf("ratelimit:fixed:%s", key))

	var (
		wg      sync.WaitGroup
		allowed atomic.Int64 // atomic counter — safe to increment from multiple goroutines
		denied  atomic.Int64
		errors  atomic.Int64
	)

	// Launch all goroutines at once.
	// sync.WaitGroup tracks when all goroutines finish.
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			result, err := l.Allow(context.Background(), key)
			if err != nil {
				errors.Add(1)
				return
			}
			if result.Allowed {
				allowed.Add(1)
			} else {
				denied.Add(1)
			}
		}()
	}

	// Wait for all goroutines to complete.
	wg.Wait()

	t.Logf("Concurrency test results:")
	t.Logf("  Total goroutines : %d", concurrency)
	t.Logf("  Limit            : %d", limit)
	t.Logf("  Allowed          : %d", allowed.Load())
	t.Logf("  Denied           : %d", denied.Load())
	t.Logf("  Errors           : %d", errors.Load())

	if errors.Load() > 0 {
		t.Errorf("got %d errors during concurrent test", errors.Load())
	}

	// Exactly `limit` requests must be allowed — not one more.
	if allowed.Load() != limit {
		t.Errorf("expected exactly %d allowed, got %d", limit, allowed.Load())
	}

	expectedDenied := int64(concurrency) - limit
	if denied.Load() != expectedDenied {
		t.Errorf("expected exactly %d denied, got %d", expectedDenied, denied.Load())
	}
}

// TestFixedWindow_IsolatedKeys verifies that two different keys do not
// share counters — each identifier has its own independent rate limit.
func TestFixedWindow_IsolatedKeys(t *testing.T) {
	client := newTestRedis(t)

	const limit = 2
	l := limiter.NewFixedWindowLimiter(client, limit, 1*time.Minute)

	keyA := "test:isolation:user_A"
	keyB := "test:isolation:user_B"
	cleanupKey(t, client, fmt.Sprintf("ratelimit:fixed:%s", keyA))
	cleanupKey(t, client, fmt.Sprintf("ratelimit:fixed:%s", keyB))

	// Exhaust user_A's limit completely.
	for i := 0; i < limit; i++ {
		if _, err := l.Allow(context.Background(), keyA); err != nil {
			t.Fatalf("user_A request %d failed: %v", i, err)
		}
	}

	// user_A should now be denied.
	result, _ := l.Allow(context.Background(), keyA)
	if result.Allowed {
		t.Error("user_A: expected DENY after exhausting limit, got ALLOW")
	}

	// user_B should still be allowed — completely separate counter.
	result, _ = l.Allow(context.Background(), keyB)
	if !result.Allowed {
		t.Error("user_B: expected ALLOW (independent counter), got DENY")
	}
}
