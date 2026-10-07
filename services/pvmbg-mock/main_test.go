package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testAdminKey = "admin-secret"

func TestHealthHandler(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	newMux(newTestStore(), testAdminKey, &reportStore{}, 0, 0).ServeHTTP(rec, req)

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
		req := httptest.NewRequestWithContext(context.Background(), method, path, nil)
		req.Header.Set("X-Admin-Key", testAdminKey)

		newMux(newTestStore(), testAdminKey, &reportStore{}, 0, 0).ServeHTTP(rec, req)

		return rec
	}

	if rec := do(http.MethodPost, "/admin/outage"); rec.Code != http.StatusOK {
		t.Fatalf("toggle on: got %d", rec.Code)
	}

	if rec := do(http.MethodGet, "/volcanic-reports"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("during outage: expected 503, got %d", rec.Code)
	}

	rec := do(http.MethodGet, "/health")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"outage_active":true`) {
		t.Fatalf("health during outage: got %d %s", rec.Code, rec.Body.String())
	}

	do(http.MethodPost, "/admin/outage")

	if rec := do(http.MethodGet, "/volcanic-reports"); rec.Code == http.StatusServiceUnavailable {
		t.Fatal("outage should be cleared after second toggle")
	}

	if rec := do(http.MethodGet, "/health"); !strings.Contains(rec.Body.String(), `"outage_active":false`) {
		t.Fatalf("health after outage: %s", rec.Body.String())
	}
}

func TestSchemaVersionToggle(t *testing.T) {
	t.Parallel()

	for _, want := range []string{"extended", "legacy"} {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/schema-version", nil)
		req.Header.Set("X-Admin-Key", testAdminKey)

		rec := httptest.NewRecorder()

		newMux(newTestStore(), testAdminKey, &reportStore{}, 0, 0).ServeHTTP(rec, req)

		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("expected 200 with %q, got %d %s", want, rec.Code, rec.Body.String())
		}
	}
}

func TestAdminKeyRequired(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"/admin/outage", "/admin/schema-version"} {
		for key, want := range map[string]int{"": http.StatusUnauthorized, "wrong": http.StatusForbidden} {
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, nil)
			if key != "" {
				req.Header.Set("X-Admin-Key", key)
			}

			rec := httptest.NewRecorder()
			newMux(newTestStore(), testAdminKey, &reportStore{}, 0, 0).ServeHTTP(rec, req)

			if rec.Code != want {
				t.Fatalf("%s with key %q: expected %d, got %d", path, key, want, rec.Code)
			}
		}
	}
}
