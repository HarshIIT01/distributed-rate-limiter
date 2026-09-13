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

func TestSlidingWindow_BasicAllow(t *testing.T) {
	client := newTestRedis(t)

	const limit = 5
	l := limiter.NewSlidingWindowLimiter(client, limit, 1*time.Minute)

	key := "sw:test:basic"
	cleanupKey(t, client, fmt.Sprintf("ratelimit:sliding:%s", key))

	for i := 1; i <= limit; i++ {
		result, err := l.Allow(context.Background(), key)
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i, err)
		}
		if !result.Allowed {
			t.Errorf("request %d: expected ALLOW, got DENY", i)
		}
		t.Logf("request %d: remaining=%d", i, result.Remaining)
	}
}

func TestSlidingWindow_DenyAtLimit(t *testing.T) {
	client := newTestRedis(t)

	const limit = 3
	l := limiter.NewSlidingWindowLimiter(client, limit, 1*time.Minute)

	key := "sw:test:deny"
	cleanupKey(t, client, fmt.Sprintf("ratelimit:sliding:%s", key))

	// Fill up to limit.
	for i := 0; i < limit; i++ {
		if _, err := l.Allow(context.Background(), key); err != nil {
			t.Fatalf("setup request %d: %v", i, err)
		}
	}

	// Next should be denied.
	result, err := l.Allow(context.Background(), key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Allowed {
		t.Error("expected DENY at limit, got ALLOW")
	}
	// retry_after should be > 0 (time until oldest request exits window).
	if !result.ResetAt.After(time.Now()) {
		t.Error("expected ResetAt in the future when denied")
	}
	t.Logf("retry in: %v", time.Until(result.ResetAt).Round(time.Millisecond))
}

// TestSlidingWindow_NoBoundaryBurst is the critical test that proves
// Sliding Window fixes the Fixed Window boundary burst problem.
//
// With Fixed Window (limit=3, window=60s):
//   t=59s: send 3 → all ALLOW (window 1 ends, count=3)
//   t=61s: send 3 → all ALLOW (window 2 starts, count resets to 0)
//   Result: 6 requests in 2 seconds ← WRONG
//
// With Sliding Window (limit=3, window=60s):
//   t=59s: send 3 → all ALLOW (log=[59,59,59])
//   t=61s: send 1 → DENY (log still shows 3 entries within last 60s)
//   Result: denied correctly ← RIGHT
//
// We simulate this by using a very short window (200ms) and sending
// requests at the boundary.
func TestSlidingWindow_NoBoundaryBurst(t *testing.T) {
	client := newTestRedis(t)

	const (
		limit      = 3
		windowSize = 500 * time.Millisecond
	)
	l := limiter.NewSlidingWindowLimiter(client, limit, windowSize)

	key := "sw:test:boundary"
	cleanupKey(t, client, fmt.Sprintf("ratelimit:sliding:%s", key))

	// Send 3 requests — fills the window.
	for i := 0; i < limit; i++ {
		result, _ := l.Allow(context.Background(), key)
		if !result.Allowed {
			t.Fatalf("setup request %d should be allowed", i)
		}
	}

	// Wait for 60% of the window to pass (300ms of 500ms).
	// In Fixed Window: if this crossed a boundary, counter would reset.
	// In Sliding Window: the 3 requests are still within the 500ms window.
	time.Sleep(300 * time.Millisecond)

	// This request should still be DENIED.
	// The 3 original requests are still within the sliding 500ms window.
	result, _ := l.Allow(context.Background(), key)
	if result.Allowed {
		t.Error("BOUNDARY BURST: sliding window should deny — old requests still in window")
	}
	t.Log("correctly denied: old requests still within the sliding window")

	// Wait for the full window to expire (200ms more = 500ms total).
	time.Sleep(250 * time.Millisecond)

	// Now all old requests have exited the window. This should be allowed.
	result, _ = l.Allow(context.Background(), key)
	if !result.Allowed {
		t.Error("expected ALLOW after window fully expired")
	}
	t.Log("correctly allowed: window has fully slid past old requests")
}

// TestSlidingWindow_Concurrency verifies atomicity under concurrent load.
func TestSlidingWindow_Concurrency(t *testing.T) {
	client := newTestRedis(t)

	const (
		limit       = 10
		concurrency = 100
	)
	l := limiter.NewSlidingWindowLimiter(client, limit, 1*time.Minute)

	key := "sw:test:concurrency"
	cleanupKey(t, client, fmt.Sprintf("ratelimit:sliding:%s", key))

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

	t.Logf("Sliding Window concurrency results:")
	t.Logf("  Limit       : %d", limit)
	t.Logf("  Concurrency : %d", concurrency)
	t.Logf("  Allowed     : %d", allowed.Load())
	t.Logf("  Denied      : %d", denied.Load())
	t.Logf("  Errors      : %d", errors.Load())

	if errors.Load() > 0 {
		t.Errorf("got %d errors", errors.Load())
	}
	if allowed.Load() != limit {
		t.Errorf("expected exactly %d allowed, got %d", limit, allowed.Load())
	}
}
