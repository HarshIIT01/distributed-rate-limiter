package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// healthResponse defines the JSON structure for the health check response.
// The struct tags (e.g., `json:"status"`) tell Go's JSON encoder
// what key name to use in the output.
type healthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// healthHandler handles GET /health requests.
// It returns a JSON response indicating the service is running.
func healthHandler(w http.ResponseWriter, r *http.Request) {
	// Only allow GET requests on this endpoint.
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := healthResponse{
		Status:    "ok",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	// Tell the client we are sending JSON.
	w.Header().Set("Content-Type", "application/json")

	// Write HTTP 200 status.
	w.WriteHeader(http.StatusOK)

	// Encode the struct as JSON and write it to the response body.
	// If encoding fails, we log the error (nothing else we can do here
	// since we already sent the status code).
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("ERROR: failed to encode health response: %v", err)
	}
}

func main() {
	// Create a new ServeMux (request router).
	// We use NewServeMux() instead of http.DefaultServeMux to avoid
	// accidentally exposing routes registered by third-party packages.
	mux := http.NewServeMux()

	// Register our routes.
	mux.HandleFunc("/health", healthHandler)

	// Define the server with explicit timeouts.
	// NEVER use http.ListenAndServe directly in production code —
	// it creates a server with no timeouts, which is a security risk.
	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Println("INFO: server starting on :8080")

	// ListenAndServe blocks forever (until the process is killed).
	// It returns an error only if it fails to start.
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("FATAL: server failed to start: %v", err)
	}
}
