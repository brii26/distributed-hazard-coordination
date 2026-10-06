package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthReflectsTheBrokerConnection(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		connected          bool
		wantStatus, wantMQ string
	}{
		{true, "ok", "connected"},
		{false, "degraded", "disconnected"},
	} {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()

		newMux(newState(), func() bool { return tt.connected }).ServeHTTP(rec, req)

		var body map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}

		if rec.Code != http.StatusOK || body["status"] != tt.wantStatus || body["rabbitmq"] != tt.wantMQ {
			t.Fatalf("connected=%v: expected 200 %s/%s, got %d %v", tt.connected, tt.wantStatus, tt.wantMQ, rec.Code, body)
		}
	}
}

func TestStateEndpointListsTheEvents(t *testing.T) {
	t.Parallel()

	s := newState()
	s.handle(msg(t, event("h1", "SEISMIC", "WASPADA")))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/state", nil)
	rec := httptest.NewRecorder()
	newMux(s, func() bool { return true }).ServeHTTP(rec, req)

	var body struct {
		Count  int           `json:"count"`
		Events []HazardEvent `json:"events"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Count != 1 || body.Events[0].HazardID != "h1" {
		t.Fatalf("expected the one event, got %s %v", rec.Body, err)
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
