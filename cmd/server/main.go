package main

import (
	"log"
	"net/http"
	"time"

	"github.com/HarshIIT01/distributed-rate-limiter/internal/handler"
	"github.com/HarshIIT01/distributed-rate-limiter/internal/limiter"
	"github.com/HarshIIT01/distributed-rate-limiter/internal/redisclient"
)

func main() {
	// ── Redis ────────────────────────────────────────────────────────────────
	redisClient, err := redisclient.New(redisclient.Config{
		Address:  "localhost:6379",
		Password: "",
		DB:       0,
	})
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	log.Println("INFO: connected to Redis")

	// ── Rate Limiters ────────────────────────────────────────────────────────
	// Create the fixed window limiter:
	//   limit  = 5 requests   (kept small so you can test the denial quickly)
	//   window = 1 minute
	//
	// In production these values would come from environment variables or a
	// database policy. We hardcode them here for now and replace in Phase 9.
	fixedLimiter := limiter.NewFixedWindowLimiter(redisClient, 5, 1*time.Minute)

	// ── HTTP Router ──────────────────────────────────────────────────────────
	mux := http.NewServeMux()

	mux.Handle("/health", handler.NewHealthHandler())
	mux.Handle("/v1/check", handler.NewCheckHandler(fixedLimiter))

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
