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

func TestOutageToggle(t *testing.T) {
	t.Parallel()

	do := func(method, path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		newMux().ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), method, path, nil))

		return rec
	}

	if rec := do(http.MethodPost, "/admin/outage"); rec.Code != http.StatusOK {
		t.Fatalf("toggle on: got %d", rec.Code)
	}

	if rec := do(http.MethodGet, "/volcanic-reports"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("during outage: expected 503, got %d", rec.Code)
	}

	if rec := do(http.MethodGet, "/health"); rec.Code != http.StatusOK {
		t.Fatalf("health during outage: got %d", rec.Code)
	}

	do(http.MethodPost, "/admin/outage")

	if rec := do(http.MethodGet, "/volcanic-reports"); rec.Code == http.StatusServiceUnavailable {
		t.Fatal("outage should be cleared after second toggle")
	}
}
