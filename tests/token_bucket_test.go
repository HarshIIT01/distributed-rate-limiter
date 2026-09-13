package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HarshIIT01/distributed-rate-limiter/internal/limiter"
)

// TestTokenBucket_FullBucketOnStart verifies that a new client
// starts with a full bucket and can burst up to capacity.
func TestTokenBucket_FullBucketOnStart(t *testing.T) {
	client := newTestRedis(t)

	const capacity = 5
	l := limiter.NewTokenBucketLimiter(client, capacity, 1.0)

	key := "tb:test:full_start"
	cleanupKey(t, client, fmt.Sprintf("ratelimit:token_bucket:%s", key))

	// All requests up to capacity should be allowed immediately.
	for i := 1; i <= capacity; i++ {
		result, err := l.Allow(context.Background(), key)
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i, err)
		}
		if !result.Allowed {
			t.Errorf("request %d: expected ALLOW (burst), got DENY", i)
		}
		t.Logf("request %d: remaining=%d, allowed=%v", i, result.Remaining, result.Allowed)
	}
}

// TestTokenBucket_DenyWhenEmpty verifies that requests are denied
// when the bucket is empty.
func TestTokenBucket_DenyWhenEmpty(t *testing.T) {
	client := newTestRedis(t)

	const capacity = 3
	l := limiter.NewTokenBucketLimiter(client, capacity, 0.5) // slow refill

	key := "tb:test:deny_empty"
	cleanupKey(t, client, fmt.Sprintf("ratelimit:token_bucket:%s", key))

	// Drain the bucket.
	for i := 0; i < capacity; i++ {
		if _, err := l.Allow(context.Background(), key); err != nil {
			t.Fatalf("drain request %d failed: %v", i, err)
		}
	}

	// Next request must be denied.
	result, err := l.Allow(context.Background(), key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Allowed {
		t.Error("expected DENY when bucket is empty, got ALLOW")
	}
	if result.Remaining != 0 {
		t.Errorf("expected remaining=0 when denied, got %d", result.Remaining)
	}

	// Verify retry_after: ResetAt should be in the future.
	if !result.ResetAt.After(time.Now()) {
		t.Error("expected ResetAt to be in the future when denied")
	}
	t.Logf("retry in: %v", time.Until(result.ResetAt).Round(time.Millisecond))
}

// TestTokenBucket_Refill verifies that tokens refill over time.
// This test waits for real time to pass, so it is intentionally slow.
func TestTokenBucket_Refill(t *testing.T) {
	client := newTestRedis(t)

	const (
		capacity   = 2
		refillRate = 5.0 // 5 tokens per second → 1 token every 200ms
	)
	l := limiter.NewTokenBucketLimiter(client, capacity, refillRate)

	key := "tb:test:refill"
	cleanupKey(t, client, fmt.Sprintf("ratelimit:token_bucket:%s", key))

	// Drain the bucket completely.
	for i := 0; i < capacity; i++ {
		l.Allow(context.Background(), key)
	}

	// Confirm it's empty.
	result, _ := l.Allow(context.Background(), key)
	if result.Allowed {
		t.Fatal("bucket should be empty after draining")
	}

	// Wait long enough for 1 token to refill.
	// At 5 tokens/s, 1 token takes 200ms. We wait 300ms to be safe.
	time.Sleep(300 * time.Millisecond)

	// Now 1 token should have refilled.
	result, err := l.Allow(context.Background(), key)
	if err != nil {
		t.Fatalf("unexpected error after refill wait: %v", err)
	}
	if !result.Allowed {
		t.Error("expected ALLOW after token refill, got DENY")
	}
	t.Logf("allowed after refill: remaining=%d", result.Remaining)
}

// TestTokenBucket_Concurrency fires many concurrent requests and verifies
// the allow count never exceeds capacity.
//
// Unlike Fixed Window (where INCR is always safe), Token Bucket's correctness
// under concurrency fully depends on the Lua script being atomic.
// Without atomicity, two goroutines could both read tokens=1.0, both
// think they can consume, and both be allowed — exceeding the limit.
func TestTokenBucket_Concurrency(t *testing.T) {
	client := newTestRedis(t)

	const (
		capacity    = 10
		refillRate  = 0.1  // very slow refill — essentially no refill during the test
		concurrency = 200
	)

	l := limiter.NewTokenBucketLimiter(client, capacity, refillRate)

	key := "tb:test:concurrency"
	cleanupKey(t, client, fmt.Sprintf("ratelimit:token_bucket:%s", key))

	var (
		wg      sync.WaitGroup
		allowed atomic.Int64
		denied  atomic.Int64
		errors  atomic.Int64
	)

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
	wg.Wait()

	t.Logf("Token Bucket concurrency results:")
	t.Logf("  Capacity     : %d", capacity)
	t.Logf("  Concurrency  : %d", concurrency)
	t.Logf("  Allowed      : %d", allowed.Load())
	t.Logf("  Denied       : %d", denied.Load())
	t.Logf("  Errors       : %d", errors.Load())

	if errors.Load() > 0 {
		t.Errorf("got %d errors during concurrent test", errors.Load())
	}

	// CRITICAL: allowed must never exceed capacity.
	// If it does, the Lua script is not atomic.
	if allowed.Load() > capacity {
		t.Errorf("ATOMICITY VIOLATION: allowed=%d exceeds capacity=%d", allowed.Load(), capacity)
	}

	// Allowed should be exactly capacity (all burst tokens consumed).
	if allowed.Load() != capacity {
		t.Errorf("expected exactly %d allowed, got %d", capacity, allowed.Load())
	}
}
