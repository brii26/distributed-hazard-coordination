package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthHandler(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	newMux().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
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
