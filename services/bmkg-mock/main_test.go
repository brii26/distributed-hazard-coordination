package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

const testAPIKey = "secret-key"

func TestHealthHandler(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	newMux(testAPIKey, &eventStore{}).ServeHTTP(rec, req)

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

	newMux(testAPIKey, &eventStore{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", rec.Code)
	}
}

func TestForbidden(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", "wrong-key")

	rec := httptest.NewRecorder()

	newMux(testAPIKey, &eventStore{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d", rec.Code)
	}
}

func TestAuthorized(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", testAPIKey)

	rec := httptest.NewRecorder()

	newMux(testAPIKey, &eventStore{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func get(t *testing.T, store *eventStore, path string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	req.Header.Set("X-BMKG-Key", testAPIKey)

	rec := httptest.NewRecorder()
	newMux(testAPIKey, store).ServeHTTP(rec, req)

	return rec
}

func TestSeismicEventsSince(t *testing.T) {
	t.Parallel()

	store := &eventStore{}
	base := time.Now().UTC()
	store.seed(10, time.Minute, base) // oldest 10 min ago, newest 1 min ago

	rec := get(t, store, "/seismic-events?since="+url.QueryEscape(base.Add(-5*time.Minute-time.Second).Format(time.RFC3339Nano)))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}

	if len(got) != 5 {
		t.Fatalf("expected the 5 newest events, got %d", len(got))
	}

	for _, f := range []string{"event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon", "region_name", "occurred_at", "potential_tsunami"} {
		if _, ok := got[0][f]; !ok {
			t.Fatalf("missing field %q in %v", f, got[0])
		}
	}

	if all := get(t, store, "/seismic-events"); len(all.Body.Bytes()) == 0 || all.Code != http.StatusOK {
		t.Fatalf("no since should return everything, got %d", all.Code)
	}
}

func TestTsunamiWarningsReferenceEvents(t *testing.T) {
	t.Parallel()

	store := &eventStore{}
	store.seed(200, time.Second, time.Now())

	var warnings []TsunamiWarning
	if err := json.Unmarshal(get(t, store, "/tsunami-warnings").Body.Bytes(), &warnings); err != nil {
		t.Fatal(err)
	}

	tsunami := map[string]bool{}

	for _, e := range store.eventsSince(time.Time{}) {
		if e.PotentialTsunami {
			tsunami[e.EventID] = true
		}
	}

	if len(warnings) == 0 || len(warnings) != len(tsunami) {
		t.Fatalf("expected one warning per tsunami event (%d), got %d", len(tsunami), len(warnings))
	}

	for _, w := range warnings {
		if !tsunami[w.RelatedEventID] || w.WarningID == "" || len(w.AffectedZones) == 0 {
			t.Fatalf("bad warning %+v", w)
		}
	}
}

func TestInvalidSince(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"/seismic-events?since=yesterday", "/tsunami-warnings?since=yesterday"} {
		if rec := get(t, &eventStore{}, path); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: expected 400, got %d", path, rec.Code)
		}
	}
}

func TestEmptyResultIsArray(t *testing.T) {
	t.Parallel()

	if body := get(t, &eventStore{}, "/tsunami-warnings").Body.String(); body != "[]\n" {
		t.Fatalf("expected [], got %q", body)
	}
}
