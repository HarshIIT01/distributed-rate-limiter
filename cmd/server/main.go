package main

import (
	"log"
	"net/http"

	"github.com/HarshIIT01/distributed-rate-limiter/internal/config"
	"github.com/HarshIIT01/distributed-rate-limiter/internal/handler"
	"github.com/HarshIIT01/distributed-rate-limiter/internal/limiter"
	"github.com/HarshIIT01/distributed-rate-limiter/internal/redisclient"
)

func main() {
	// ── Configuration ────────────────────────────────────────────────────────
	// Load all settings from environment variables.
	// Defaults make it work in development with no setup.
	// Override with env vars for Docker / production.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	log.Printf("INFO: config loaded — algorithm=%s addr=%s",
		cfg.Limiter.Algorithm, cfg.Redis.Address)

	// ── Redis ────────────────────────────────────────────────────────────────
	redisClient, err := redisclient.New(redisclient.Config{
		Address:  cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	log.Println("INFO: connected to Redis")

	// ── Rate Limiter ─────────────────────────────────────────────────────────
	// Select algorithm based on configuration.
	// The handler accepts any RateLimiter interface — swapping algorithms
	// requires only changing the env var, not recompiling.
	var rateLimiter handler.RateLimiter
	switch cfg.Limiter.Algorithm {
	case "token_bucket":
		rateLimiter = limiter.NewTokenBucketLimiter(
			redisClient,
			cfg.Limiter.Capacity,
			cfg.Limiter.RefillRate,
		)
		log.Printf("INFO: using Token Bucket — capacity=%d refill=%.1f/s",
			cfg.Limiter.Capacity, cfg.Limiter.RefillRate)

	case "fixed_window":
		rateLimiter = limiter.NewFixedWindowLimiter(
			redisClient,
			cfg.Limiter.DefaultLimit,
			cfg.Limiter.WindowSize,
		)
		log.Printf("INFO: using Fixed Window — limit=%d window=%s",
			cfg.Limiter.DefaultLimit, cfg.Limiter.WindowSize)

	case "sliding_window":
		rateLimiter = limiter.NewSlidingWindowLimiter(
			redisClient,
			cfg.Limiter.DefaultLimit,
			cfg.Limiter.WindowSize,
		)
		log.Printf("INFO: using Sliding Window — limit=%d window=%s",
			cfg.Limiter.DefaultLimit, cfg.Limiter.WindowSize)

	default:
		// config.validate() already checks this, so this is a safety net.
		log.Fatalf("FATAL: unknown algorithm %q", cfg.Limiter.Algorithm)
	}

	// ── HTTP Router ──────────────────────────────────────────────────────────
	mux := http.NewServeMux()
	mux.Handle("/health", handler.NewHealthHandler())
	mux.Handle("/v1/check", handler.NewCheckHandler(rateLimiter))

	// ── HTTP Server ──────────────────────────────────────────────────────────
	server := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      mux,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	log.Printf("INFO: server starting on :%s", cfg.Server.Port)

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("FATAL: server failed to start: %v", err)
	}
}
