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

	// ── Rate Limiter ─────────────────────────────────────────────────────────
	// Token Bucket configuration:
	//   capacity   = 5 tokens  (burst: a client can make 5 requests instantly)
	//   refillRate = 1 token/s (sustained: 1 new request allowed per second)
	//
	// These small values make manual testing easy — you can exhaust the bucket
	// quickly and watch it refill. Production values would be much larger and
	// loaded from configuration/database.
	rateLimiter := limiter.NewTokenBucketLimiter(redisClient, 5, 1.0)

	// ── HTTP Router ──────────────────────────────────────────────────────────
	mux := http.NewServeMux()

	mux.Handle("/health", handler.NewHealthHandler())
	mux.Handle("/v1/check", handler.NewCheckHandler(rateLimiter))

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
