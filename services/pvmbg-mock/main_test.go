package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	wantLegacy   = "legacy"
	wantExtended = "extended"
	bodyVersion2 = `{"version":2}`
)

// Starts a test from both switches off and puts them back off afterwards. Only for tests that are not parallel.
func resetSwitches(t *testing.T) {
	t.Helper()

	outageActive.Store(false)
	schemaExtended.Store(false)
	t.Cleanup(func() {
		outageActive.Store(false)
		schemaExtended.Store(false)
	})
}

func TestHealthHandler(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	newMux(newTestStore(), &reportStore{}, 0, 0).ServeHTTP(rec, req)

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

//nolint:paralleltest // flips the shared switch state
func TestOutageToggle(t *testing.T) {
	resetSwitches(t)

	do := func(method, path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), method, path, nil)

		newMux(newTestStore(), &reportStore{}, 0, 0).ServeHTTP(rec, req)

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

//nolint:paralleltest // flips the shared switch state
func TestSchemaVersionToggle(t *testing.T) {
	resetSwitches(t)

	for _, want := range []string{wantExtended, wantLegacy} {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/schema-version", nil)
		rec := httptest.NewRecorder()

		newMux(newTestStore(), &reportStore{}, 0, 0).ServeHTTP(rec, req)

		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("expected 200 with %q, got %d %s", want, rec.Code, rec.Body.String())
		}
	}
}

// Both admin routes work without any credential and always answer 200, whatever the body.
//
//nolint:paralleltest // flips the shared switch state
func TestAdminOpenWithoutKey(t *testing.T) {
	resetSwitches(t)

	post := func(path, body string, headers map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, strings.NewReader(body))
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		rec := httptest.NewRecorder()
		newMux(newTestStore(), &reportStore{}, 0, 0).ServeHTTP(rec, req)

		return rec
	}

	cases := []struct {
		name, body, want string
		start            bool // schema state before the call
	}{
		{"empty toggles on", "", wantExtended, false},
		{"empty toggles off", "", wantLegacy, true},
		{"version 2 sets extended", bodyVersion2, wantExtended, false},
		{"version 2 twice stays extended", bodyVersion2, wantExtended, true},
		{"version 5 is extended", `{"version":5}`, wantExtended, false},
		{"version 1 sets legacy", `{"version":1}`, wantLegacy, true},
		{"version 0 toggles", `{"version":0}`, wantExtended, false},
		{"negative toggles", `{"version":-3}`, wantLegacy, true},
		{"not json toggles", `not json`, wantExtended, false},
		{"array toggles", `[1]`, wantLegacy, true},
		{"null toggles", `null`, wantExtended, false},
		{"string version toggles", `{"version":"2"}`, wantLegacy, true},
		{"2 KB body toggles", `{"version":1,"pad":"` + strings.Repeat("x", 2048) + `"}`, wantExtended, false},
	}

	for _, tc := range cases {
		schemaExtended.Store(tc.start)

		rec := post("/admin/schema-version", tc.body, nil)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"`+tc.want+`"`) {
			t.Fatalf("%s: expected 200 %q, got %d %s", tc.name, tc.want, rec.Code, rec.Body.String())
		}
	}

	if rec := post("/admin/schema-version", "", map[string]string{"X-Admin-Key": "whatever"}); rec.Code != http.StatusOK {
		t.Fatalf("a stray key header must be ignored, got %d", rec.Code)
	}

	if rec := post("/admin/outage", "ignored", nil); rec.Code != http.StatusOK || !outageActive.Load() {
		t.Fatalf("outage without key: got %d, active %t", rec.Code, outageActive.Load())
	}

	if rec := post("/admin/outage", "", nil); rec.Code != http.StatusOK || outageActive.Load() {
		t.Fatalf("outage second toggle: got %d, active %t", rec.Code, outageActive.Load())
	}
}
