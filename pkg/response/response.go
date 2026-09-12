// Package response provides helpers for writing consistent JSON HTTP responses.
// Every handler in the service uses these helpers to ensure uniform structure,
// correct Content-Type headers, and proper error formatting.
package response

import (
	"encoding/json"
	"log"
	"net/http"
)

// JSON writes a JSON-encoded body with the given HTTP status code.
// It sets the Content-Type header before writing the status code —
// headers MUST be set before WriteHeader is called, otherwise they are ignored.
func JSON(w http.ResponseWriter, statusCode int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(body); err != nil {
		// At this point the status code is already sent, so we can only log.
		log.Printf("ERROR: response.JSON encode failed: %v", err)
	}
}

// Error writes a structured JSON error response.
// All API errors use this format so clients can reliably parse error messages.
//
// Example output:
//
//	{"error": "method_not_allowed", "message": "only GET is supported"}
func Error(w http.ResponseWriter, statusCode int, errCode string, message string) {
	body := map[string]string{
		"error":   errCode,
		"message": message,
	}
	JSON(w, statusCode, body)
}
