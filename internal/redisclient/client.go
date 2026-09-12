// Package redisclient provides a configured Redis client for the application.
// It wraps go-redis to centralise connection configuration, timeout settings,
// and the startup connectivity check in one place.
//
// Every other package that needs Redis imports this package and uses the
// *redis.Client it provides — they never configure their own connections.
package redisclient

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds all Redis connection parameters.
// Values are typically loaded from environment variables.
type Config struct {
	Address  string // e.g. "localhost:6379"
	Password string // empty string means no password
	DB       int    // Redis database index (0–15); use 0 by default
}

// New creates a new Redis client using the given config and verifies
// the connection is reachable by sending a PING command.
//
// Returns an error if the connection cannot be established within 5 seconds.
// Call this once at application startup — the returned *redis.Client is
// safe for concurrent use and should be shared across the application.
func New(cfg Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       cfg.DB,

		// Timeouts: how long to wait for Redis to respond.
		// Without these, a slow or unreachable Redis hangs the whole request.
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,

		// Connection pool: go-redis maintains a pool of connections to Redis.
		// PoolSize controls the maximum number of concurrent connections.
		// 10 is a safe default for development.
		PoolSize: 10,
	})

	// Verify the connection is reachable at startup.
	// context.WithTimeout creates a deadline — if PING takes longer than
	// 5 seconds, the context is cancelled and PING returns an error.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel() // always release context resources

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: failed to connect to %s: %w", cfg.Address, err)
	}

	return client, nil
}
