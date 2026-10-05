package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		readable, conn     bool
		wantCode           int
		wantStatus, wantMQ string
		wantStore          string
	}{
		{"all good", true, true, http.StatusOK, "ok", "connected", "readable"},
		{"broker lost", true, false, http.StatusOK, "degraded", "disconnected", "readable"},
		{"dedup store unreadable", false, false, http.StatusServiceUnavailable, "down", "disconnected", "unreadable"},
	}

	for _, tt := range tests {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()

		newMux(tt.readable, func() bool { return tt.conn }).ServeHTTP(rec, req)

		var body map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}

		if rec.Code != tt.wantCode || body["status"] != tt.wantStatus || body["rabbitmq"] != tt.wantMQ || body["dedup_store"] != tt.wantStore {
			t.Fatalf("%s: got %d %v", tt.name, rec.Code, body)
		}
	}
}

func TestResolvePortDefault(t *testing.T) {
	t.Parallel()

	port, err := resolvePort("")
	if err != nil || port != defaultPort {
		t.Fatalf("expected default port %s, got %s %v", defaultPort, port, err)
	}
}

func TestResolvePortInvalid(t *testing.T) {
	t.Parallel()

	if _, err := resolvePort("not-a-port"); err == nil {
		t.Fatal("expected error for invalid port, got nil")
	}
}
