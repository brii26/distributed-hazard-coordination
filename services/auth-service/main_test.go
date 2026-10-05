package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthReflectsTheUserDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ping     error
		want     string
		wantCode int
	}{
		{"database up", nil, "ok", http.StatusOK},
		{"database down", errors.New("connection refused"), "down", http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
			rec := httptest.NewRecorder()

			newMux(func(context.Context) error { return tt.ping }).ServeHTTP(rec, req)

			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("health body is not JSON: %v", err)
			}

			if rec.Code != tt.wantCode || body["status"] != tt.want {
				t.Fatalf("expected %d %q, got %d %q", tt.wantCode, tt.want, rec.Code, body["status"])
			}
		})
	}
}

func TestResolvePortDefault(t *testing.T) {
	t.Parallel()

	port, err := resolvePort("", defaultPort)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if port != defaultPort {
		t.Fatalf("expected default port %s, got %s", defaultPort, port)
	}
}

func TestResolvePortInvalid(t *testing.T) {
	t.Parallel()

	if _, err := resolvePort("not-a-port", defaultPort); err == nil {
		t.Fatal("expected error for invalid port, got nil")
	}
}
