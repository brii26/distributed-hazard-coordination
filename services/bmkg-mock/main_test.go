package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

const testAPIKey = "secret-key"

func TestHealthHandler(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	newMux(testAPIKey).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestResolvePortDefault(t *testing.T) {
	t.Parallel()

	port, err := resolvePort("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if port != defaultPort {
		t.Fatalf("expected default port %s, got %s", defaultPort, port)
	}
}

func TestResolvePortInvalid(t *testing.T) {
	t.Parallel()

	if _, err := resolvePort("not-a-port"); err == nil {
		t.Fatal("expected error for invalid port, got nil")
	}
}

func TestUnauthorized(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/seismic-events", nil)
	rec := httptest.NewRecorder()

	newMux(testAPIKey).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", rec.Code)
	}
}

func TestForbidden(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", "wrong-key")

	rec := httptest.NewRecorder()

	newMux(testAPIKey).ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d", rec.Code)
	}
}

func TestAuthorized(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", testAPIKey)

	rec := httptest.NewRecorder()

	newMux(testAPIKey).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}
