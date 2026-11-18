package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func summaryBody(at time.Time) string {
	return `{"summary":"2 hazard events.","source":"rule","llm":"disabled","events":2,"window_seconds":300,"anomalies":[],"generated_at":"` + at.UTC().Format(time.RFC3339Nano) + `"}`
}

// One request through the handler with claims set, the way the JWT middleware leaves it.
func getSituation(t *testing.T, api *situationAPI, role string) (code int, body map[string]any, took time.Duration) {
	t.Helper()

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims := jwtClaims{Email: "x@x.example", Role: role, Exp: time.Now().Add(time.Minute).Unix()}
		api.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), claimsKey{}, claims)))
	})
	srv := httptest.NewServer(withCorrelation(h))
	t.Cleanup(srv.Close)

	start := time.Now()

	resp, err := http.Get(srv.URL + "/situation") //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = resp.Body.Close() }()

	took = time.Since(start)
	_ = json.NewDecoder(resp.Body).Decode(&body)

	return resp.StatusCode, body, took
}

func TestSituationRelaysTheSummaryAsOK(t *testing.T) {
	t.Parallel()

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/summary" {
			http.NotFound(w, r)

			return
		}

		_, _ = w.Write([]byte(summaryBody(time.Now())))
	}))
	t.Cleanup(up.Close)

	code, body, _ := getSituation(t, newSituationAPI(up.URL, 200*time.Millisecond), roleLapangan)
	if code != http.StatusOK || body["status"] != "ok" || body["summary"] != "2 hazard events." {
		t.Fatalf("got %d %v", code, body)
	}
}

func TestSituationMarksAnOldSummaryStale(t *testing.T) {
	t.Parallel()

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(summaryBody(time.Now().Add(-5 * time.Minute))))
	}))
	t.Cleanup(up.Close)

	if code, body, _ := getSituation(t, newSituationAPI(up.URL, 200*time.Millisecond), roleOperator); code != http.StatusOK || body["status"] != "stale" {
		t.Fatalf("got %d %v", code, body)
	}
}

func TestSituationIsNotForMedia(t *testing.T) {
	t.Parallel()

	if code, _, _ := getSituation(t, newSituationAPI("http://127.0.0.1:1", time.Second), roleMedia); code != http.StatusForbidden {
		t.Fatalf("got %d, want 403", code)
	}
}

// The summarizer misbehaving in every way the isolation test needs: hangs (like a stuck LLM would make a naive client), garbage, dead.
func TestSituationAnswersUnder300msWhenTheSummarizerFails(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))

	t.Cleanup(hang.Close)
	t.Cleanup(func() { close(release) }) // runs first, so Close is not stuck on the hanging handler

	garbage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"summary": "cut`)) }))
	t.Cleanup(garbage.Close)

	wrongShape := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"hello":"world"}`)) }))
	t.Cleanup(wrongShape.Close)

	// A port that was just closed, like a killed pod.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	dead := "http://" + l.Addr().String()

	_ = l.Close()

	for name, url := range map[string]string{"slow": hang.URL, "invalid-json": garbage.URL, "wrong-shape": wrongShape.URL, "dead": dead} {
		t.Run(name, func(t *testing.T) {
			api := newSituationAPI(url, 200*time.Millisecond)

			// No copy yet: unavailable.
			code, body, took := getSituation(t, api, roleLapangan)
			if code != http.StatusServiceUnavailable || body["status"] != "unavailable" || took > 300*time.Millisecond {
				t.Fatalf("no copy: %d %v in %s", code, body, took)
			}

			// With a good copy cached: stale.
			api.last, api.at = map[string]any{"summary": "old", "generated_at": time.Now().Format(time.RFC3339Nano)}, time.Now().Add(-time.Minute)

			code, body, took = getSituation(t, api, roleLapangan)
			if code != http.StatusOK || body["status"] != "stale" || took > 300*time.Millisecond {
				t.Fatalf("with a copy: %d %v in %s", code, body, took)
			}
		})
	}
}

func TestSituationDropsACopyOlderThanFifteenMinutes(t *testing.T) {
	t.Parallel()

	api := newSituationAPI("http://127.0.0.1:1", 100*time.Millisecond)
	api.last, api.at = map[string]any{"summary": "old"}, time.Now().Add(-20*time.Minute)

	if code, body, _ := getSituation(t, api, roleOperator); code != http.StatusServiceUnavailable || body["status"] != "unavailable" {
		t.Fatalf("got %d %v", code, body)
	}
}
