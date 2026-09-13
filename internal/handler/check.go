package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/HarshIIT01/distributed-rate-limiter/internal/limiter"
	"github.com/HarshIIT01/distributed-rate-limiter/pkg/response"
)

// RateLimiter is the interface that all rate limiting algorithms must satisfy.
// The handler depends on this interface, not on any concrete implementation.
//
// This is the Strategy Pattern — the handler doesn't care whether you give it
// a FixedWindowLimiter, TokenBucketLimiter, or SlidingWindowLimiter.
// Any type with an Allow method matching this signature will work.
//
// Go interfaces are satisfied implicitly. No "implements" keyword is needed.
// If a type has the right methods, it satisfies the interface automatically.
type RateLimiter interface {
	Allow(ctx context.Context, key string) (limiter.Result, error)
}

// checkRequest is the expected JSON body for POST /v1/check.
type checkRequest struct {
	Identifier string `json:"identifier"` // who is making the request (user ID, IP, API key)
	Resource   string `json:"resource"`   // what they are accessing (/api/orders)
	Method     string `json:"method"`     // HTTP method (GET, POST, etc.)
}

// checkResponse is the JSON body returned when a request is ALLOWED.
type checkResponse struct {
	Allowed   bool      `json:"allowed"`
	Limit     int64     `json:"limit"`
	Remaining int64     `json:"remaining"`
	ResetAt   time.Time `json:"reset_at"`
}

// CheckHandler handles POST /v1/check.
// It accepts any RateLimiter — the algorithm is injected at startup.
type CheckHandler struct {
	limiter RateLimiter
}

// NewCheckHandler creates a CheckHandler with the given RateLimiter.
// Pass any limiter that satisfies the RateLimiter interface.
func NewCheckHandler(l RateLimiter) *CheckHandler {
	return &CheckHandler{limiter: l}
}

func (h *CheckHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		response.Error(w, http.StatusMethodNotAllowed, "method_not_allowed", "only POST is supported")
		return
	}

	var req checkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.Error(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	if req.Identifier == "" {
		response.Error(w, http.StatusBadRequest, "invalid_request", "identifier is required")
		return
	}
	if req.Resource == "" {
		response.Error(w, http.StatusBadRequest, "invalid_request", "resource is required")
		return
	}

	key := fmt.Sprintf("%s:%s", req.Identifier, req.Resource)

	result, err := h.limiter.Allow(r.Context(), key)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "internal_error", "rate limit check failed")
		return
	}

	w.Header().Set("RateLimit-Limit", strconv.FormatInt(result.Limit, 10))
	w.Header().Set("RateLimit-Remaining", strconv.FormatInt(result.Remaining, 10))
	w.Header().Set("RateLimit-Reset", strconv.FormatInt(result.ResetAt.Unix(), 10))

	if !result.Allowed {
		retryAfter := int(time.Until(result.ResetAt).Seconds())
		if retryAfter < 1 {
			retryAfter = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		response.JSON(w, http.StatusTooManyRequests, map[string]any{
			"error":       "rate_limit_exceeded",
			"message":     "too many requests",
			"retry_after": retryAfter,
		})
		return
	}

	response.JSON(w, http.StatusOK, checkResponse{
		Allowed:   result.Allowed,
		Limit:     result.Limit,
		Remaining: result.Remaining,
		ResetAt:   result.ResetAt,
	})
}
