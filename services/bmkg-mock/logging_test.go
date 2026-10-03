package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func discardLogger() *slog.Logger {
	return newLogger(io.Discard)
}

// logLines drives one request through the mux and returns every emitted log
// line, failing the test if any line isn't valid JSON.
func logLines(t *testing.T, path, key, correlation string) ([]map[string]any, *httptest.ResponseRecorder) {
	t.Helper()

	var buf bytes.Buffer

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	if key != "" {
		req.Header.Set("X-BMKG-Key", key)
	}

	if correlation != "" {
		req.Header.Set(correlationHeader, correlation)
	}

	rec := httptest.NewRecorder()
	newMux(testAPIKey, &eventStore{}, newLogger(&buf)).ServeHTTP(rec, req)

	var lines []map[string]any

	sc := bufio.NewScanner(&buf)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("log line is not valid JSON: %q: %v", sc.Text(), err)
		}

		for _, f := range []string{"timestamp", "level", "service", "correlation_id", "message"} {
			if _, ok := m[f]; !ok {
				t.Fatalf("log line %q missing field %q", sc.Text(), f)
			}
		}

		lines = append(lines, m)
	}

	return lines, rec
}

func TestRequestLogCarriesCorrelationID(t *testing.T) {
	t.Parallel()

	lines, rec := logLines(t, "/seismic-events", "wrong", "poll-123")

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}

	if len(lines) != 2 {
		t.Fatalf("expected rejection + request lines, got %d: %v", len(lines), lines)
	}

	for _, l := range lines {
		if l["correlation_id"] != "poll-123" || l["service"] != serviceName {
			t.Fatalf("bad correlation/service on %v", l)
		}
	}

	if lines[0]["reason"] != "invalid X-BMKG-Key" || lines[1]["endpoint"] != "GET /seismic-events" || lines[1]["status"] != float64(http.StatusForbidden) {
		t.Fatalf("unexpected rejection/request lines: %v", lines)
	}
}

func TestRequestLogMissingKeyAndGeneratedID(t *testing.T) {
	t.Parallel()

	lines, rec := logLines(t, "/seismic-events", "", "")

	if rec.Code != http.StatusUnauthorized || lines[0]["reason"] != "missing X-BMKG-Key" {
		t.Fatalf("expected 401 with missing-key reason, got %d %v", rec.Code, lines)
	}

	id, _ := lines[0]["correlation_id"].(string)
	if id == "" || rec.Header().Get(correlationHeader) != id {
		t.Fatalf("expected a generated id echoed in the response header, got %q", id)
	}
}

func TestHealthRequestNotLoggedAtInfo(t *testing.T) {
	t.Parallel()

	if lines, _ := logLines(t, "/health", "", ""); len(lines) != 0 {
		t.Fatalf("health probes should be debug-only, got %v", lines)
	}
}
