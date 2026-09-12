package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/HarshIIT01/distributed-rate-limiter/internal/limiter"
	"github.com/HarshIIT01/distributed-rate-limiter/pkg/response"
)

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
// It is the primary endpoint of the rate limiting service.
// Callers (API gateways, middleware) send a request description
// and receive ALLOW or DENY.
type CheckHandler struct {
	limiter *limiter.FixedWindowLimiter
}

// NewCheckHandler creates a CheckHandler with the given limiter.
func NewCheckHandler(l *limiter.FixedWindowLimiter) *CheckHandler {
	return &CheckHandler{limiter: l}
}

func (h *CheckHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		response.Error(w, http.StatusMethodNotAllowed, "method_not_allowed", "only POST is supported")
		return
	}

	// Decode the JSON request body.
	// json.NewDecoder is preferred over json.Unmarshal for HTTP bodies
	// because it reads directly from the stream without loading the whole
	// body into memory first.
	var req checkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.Error(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	// Validate required fields.
	if req.Identifier == "" {
		response.Error(w, http.StatusBadRequest, "invalid_request", "identifier is required")
		return
	}
	if req.Resource == "" {
		response.Error(w, http.StatusBadRequest, "invalid_request", "resource is required")
		return
	}

	// Build the rate limit key by combining identifier and resource.
	// This creates separate counters per (user, endpoint) pair.
	//
	// Example:
	//   identifier="user_123", resource="/api/orders"
	//   → key = "user_123:/api/orders"
	//   → Redis key = "ratelimit:fixed:user_123:/api/orders"
	//
	// This means user_123 has their own limit on /api/orders that is
	// separate from their limit on /api/products.
	key := fmt.Sprintf("%s:%s", req.Identifier, req.Resource)

	// r.Context() carries the request's deadline and cancellation.
	// Passing it to the limiter means if the HTTP request is cancelled
	// (client disconnects), the Redis operation is also cancelled.
	result, err := h.limiter.Allow(r.Context(), key)
	if err != nil {
		// Redis or internal error — do not expose internal details to clients.
		response.Error(w, http.StatusInternalServerError, "internal_error", "rate limit check failed")
		return
	}

	// Always set rate limit headers — even on denied requests.
	// This lets clients see their current state and plan retry timing.
	// These follow the IETF draft standard for rate limit headers.
	w.Header().Set("RateLimit-Limit", strconv.FormatInt(result.Limit, 10))
	w.Header().Set("RateLimit-Remaining", strconv.FormatInt(result.Remaining, 10))
	w.Header().Set("RateLimit-Reset", strconv.FormatInt(result.ResetAt.Unix(), 10))

	if !result.Allowed {
		// Calculate retry-after in whole seconds.
		retryAfter := int(time.Until(result.ResetAt).Seconds())
		if retryAfter < 0 {
			retryAfter = 0
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
