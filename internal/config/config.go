// Package config loads application configuration from environment variables.
//
// Design decisions:
//   - Environment variables are the standard configuration mechanism for
//     containerised applications (12-factor app principle).
//   - Every value has a safe default so the service works out-of-the-box
//     in development without any setup.
//   - We do NOT use a third-party config library (like Viper) — the standard
//     library's os.Getenv is sufficient and keeps dependencies minimal.
//   - Secrets (Redis password, DB password) come from env vars and are
//     never hardcoded or logged.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration.
// Values are loaded once at startup via Load().
type Config struct {
	Server  ServerConfig
	Redis   RedisConfig
	Limiter LimiterConfig
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Port         string        // e.g. "8080"
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// RedisConfig holds Redis connection settings.
type RedisConfig struct {
	Address     string // e.g. "localhost:6379"
	Password    string // empty = no auth
	DB          int    // 0–15
	FailureMode string // "fail_open" or "fail_closed"
}

// LimiterConfig holds rate limiting defaults.
// These are used when no per-client policy is found.
// In Phase 9 (PostgreSQL), policies will come from the database instead.
type LimiterConfig struct {
	Algorithm    string        // "token_bucket", "fixed_window", "sliding_window"
	Capacity     int64         // token bucket: max tokens
	RefillRate   float64       // token bucket: tokens per second
	WindowSize   time.Duration // fixed/sliding window: window duration
	DefaultLimit int64         // fixed/sliding window: max requests per window
}

// Load reads configuration from environment variables.
// Missing variables fall back to safe development defaults.
// Call this once at application startup.
func Load() (*Config, error) {
	cfg := &Config{
		Server: ServerConfig{
			Port:         getEnv("APP_PORT", "8080"),
			ReadTimeout:  getDurationEnv("SERVER_READ_TIMEOUT_SECONDS", 5) * time.Second,
			WriteTimeout: getDurationEnv("SERVER_WRITE_TIMEOUT_SECONDS", 10) * time.Second,
			IdleTimeout:  getDurationEnv("SERVER_IDLE_TIMEOUT_SECONDS", 120) * time.Second,
		},
		Redis: RedisConfig{
			Address:     getEnv("REDIS_ADDRESS", "localhost:6379"),
			Password:    getEnv("REDIS_PASSWORD", ""),
			DB:          getIntEnv("REDIS_DB", 0),
			FailureMode: getEnv("REDIS_FAILURE_MODE", "fail_open"),
		},
		Limiter: LimiterConfig{
			Algorithm:    getEnv("LIMITER_ALGORITHM", "token_bucket"),
			Capacity:     int64(getIntEnv("LIMITER_CAPACITY", 100)),
			RefillRate:   getFloatEnv("LIMITER_REFILL_RATE", 10.0),
			WindowSize:   getDurationEnv("LIMITER_WINDOW_SECONDS", 60) * time.Second,
			DefaultLimit: int64(getIntEnv("LIMITER_DEFAULT_LIMIT", 100)),
		},
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config: invalid configuration: %w", err)
	}

	return cfg, nil
}

// validate checks that the configuration values are sensible.
func (c *Config) validate() error {
	if c.Limiter.Capacity <= 0 {
		return fmt.Errorf("LIMITER_CAPACITY must be > 0, got %d", c.Limiter.Capacity)
	}
	if c.Limiter.RefillRate <= 0 {
		return fmt.Errorf("LIMITER_REFILL_RATE must be > 0, got %f", c.Limiter.RefillRate)
	}
	if c.Limiter.DefaultLimit <= 0 {
		return fmt.Errorf("LIMITER_DEFAULT_LIMIT must be > 0, got %d", c.Limiter.DefaultLimit)
	}

	mode := c.Redis.FailureMode
	if mode != "fail_open" && mode != "fail_closed" {
		return fmt.Errorf("REDIS_FAILURE_MODE must be 'fail_open' or 'fail_closed', got %q", mode)
	}

	algorithm := c.Limiter.Algorithm
	validAlgorithms := map[string]bool{
		"token_bucket":    true,
		"fixed_window":    true,
		"sliding_window":  true,
	}
	if !validAlgorithms[algorithm] {
		return fmt.Errorf("LIMITER_ALGORITHM must be one of token_bucket/fixed_window/sliding_window, got %q", algorithm)
	}

	return nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

// getEnv returns the value of the env var, or defaultValue if not set.
func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// getIntEnv returns the integer value of the env var, or defaultValue if not set or invalid.
func getIntEnv(key string, defaultValue int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultValue
	}
	return n
}

// getFloatEnv returns the float64 value of the env var, or defaultValue if not set or invalid.
func getFloatEnv(key string, defaultValue float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return defaultValue
	}
	return f
}

// getDurationEnv returns the duration value (in seconds) of the env var,
// or defaultValue (in seconds) if not set or invalid.
// Returns a plain int that the caller multiplies by time.Second.
func getDurationEnv(key string, defaultSeconds int) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return time.Duration(defaultSeconds)
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return time.Duration(defaultSeconds)
	}
	return time.Duration(n)
}
