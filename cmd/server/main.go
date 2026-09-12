package main

import (
	"log"
	"net/http"
	"time"

	"github.com/HarshIIT01/distributed-rate-limiter/internal/handler"
)

func main() {
	// Create a new ServeMux (request router).
	// We use NewServeMux() instead of http.DefaultServeMux to avoid
	// accidentally exposing routes registered by third-party packages.
	mux := http.NewServeMux()

	// Wire up handlers.
	// main.go's only job is to create dependencies and register routes.
	// No business logic lives here.
	healthHandler := handler.NewHealthHandler()
	mux.Handle("/health", healthHandler)

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
