// Package handler contains HTTP request handlers.
// Each handler is responsible for one endpoint or a group of closely
// related endpoints. Handlers parse the request, call the appropriate
// service, and write the response using the response helper.
package handler

import (
	"net/http"
	"time"

	"github.com/HarshIIT01/distributed-rate-limiter/pkg/response"
)

// HealthHandler handles GET /health.
// It is intentionally simple — its only job is to confirm
// the HTTP server is up and accepting connections.
// It does NOT check Redis or PostgreSQL here; those will get
// their own dedicated health sub-checks later.
type HealthHandler struct{}

// NewHealthHandler creates a new HealthHandler.
// We use a constructor so that if this handler ever needs dependencies
// (e.g., a DB ping), we can add them without changing the caller.
func NewHealthHandler() *HealthHandler {
	return &HealthHandler{}
}

// ServeHTTP implements the http.Handler interface.
// Using a struct with ServeHTTP (instead of a plain function) lets us
// attach dependencies to the handler via struct fields in the future.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		response.Error(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET is supported")
		return
	}

	response.JSON(w, http.StatusOK, map[string]string{
		"status":    "ok",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}
