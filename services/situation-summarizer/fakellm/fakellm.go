// Package fakellm is a stand-in LLM endpoint for tests and the isolation measurement. It never calls a real model.
package fakellm

import (
	"io"
	"net/http"
	"strings"
	"time"
)

// Modes of the fake.
const (
	ModeOK          = "ok"           // a valid summary at once
	ModeSlow        = "slow"         // sleeps Sleep (60 s by default) before answering
	ModeInvalidJSON = "invalid-json" // 200 with a body that is not JSON
	ModeDead        = "dead"         // closes the connection without an answer
)

// DefaultSleep is how long slow mode holds a request.
const DefaultSleep = 60 * time.Second

// Handler serves the fake endpoint in one mode. A slow request ends early when the client gives up.
func Handler(mode string, sleep time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Reading the body to its end is what lets the server notice a client that hung up.
		_, _ = io.Copy(io.Discard, r.Body)

		switch mode {
		case ModeSlow:
			select {
			case <-time.After(sleep):
			case <-r.Context().Done():
				return
			}

			writeOK(w)
		case ModeInvalidJSON:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"summary": "cut off mid`))
		case ModeDead:
			hj, ok := w.(http.Hijacker)
			if !ok {
				w.WriteHeader(http.StatusBadGateway)

				return
			}

			conn, _, err := hj.Hijack()
			if err == nil {
				_ = conn.Close()
			}
		default:
			writeOK(w)
		}
	})
}

func writeOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"summary":"Fake LLM summary."}`))
}

// Valid reports whether mode is one of the four.
func Valid(mode string) bool {
	switch strings.TrimSpace(mode) {
	case ModeOK, ModeSlow, ModeInvalidJSON, ModeDead:
		return true
	}

	return false
}
