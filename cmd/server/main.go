package main

import (
	"log"
	"net/http"
	"time"

	"github.com/HarshIIT01/distributed-rate-limiter/internal/handler"
	"github.com/HarshIIT01/distributed-rate-limiter/internal/redisclient"
)

func main() {
	// ── Redis ────────────────────────────────────────────────────────────────
	// Connect to Redis before starting the HTTP server.
	// If Redis is unreachable, we fail fast at startup rather than serving
	// requests that will fail anyway.
	redisClient, err := redisclient.New(redisclient.Config{
		Address:  "localhost:6379",
		Password: "",
		DB:       0,
	})
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	log.Println("INFO: connected to Redis")

	// Silence the "declared but not used" error for now.
	// We will pass redisClient to rate-limiter handlers in the next phase.
	_ = redisClient

	// ── HTTP Router ──────────────────────────────────────────────────────────
	mux := http.NewServeMux()

	healthHandler := handler.NewHealthHandler()
	mux.Handle("/health", healthHandler)

	// ── HTTP Server ──────────────────────────────────────────────────────────
	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Println("INFO: server starting on :8080")

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("FATAL: server failed to start: %v", err)
	}
}
