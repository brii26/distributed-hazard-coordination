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

// listener opens a local TCP port and returns its address, so a dependency can be "up".
func listener(t *testing.T) string {
	t.Helper()

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = lis.Close() })

	return lis.Addr().String()
}

// closedAddr is a local address nothing listens on: a port that was just released.
func closedAddr(t *testing.T) string {
	t.Helper()

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	addr := lis.Addr().String()
	_ = lis.Close()

	return addr
}

func TestHealthReflectsTheDependencies(t *testing.T) {
	t.Parallel()

	up, down := listener(t), closedAddr(t)

	tests := []struct {
		name              string
		agg, auth         string
		wantStatus        string
		wantAgg, wantAuth string
	}{
		{"both up", up, up, "ok", "connected", "connected"},
		{"aggregator down", down, up, "degraded", "disconnected", "connected"},
		{"auth service down", up, down, "degraded", "connected", "disconnected"},
		{"both down is still degraded, never down", down, down, "degraded", "disconnected", "disconnected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
			rec := httptest.NewRecorder()

			healthAPI{aggregatorAddr: tt.agg, authAddr: tt.auth, timeout: time.Second}.ServeHTTP(rec, req)

			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("health body is not JSON: %v", err)
			}

			if rec.Code != http.StatusOK || body["status"] != tt.wantStatus ||
				body["aggregator"] != tt.wantAgg || body["auth_service"] != tt.wantAuth {
				t.Fatalf("expected 200 %s/%s/%s, got %d %v", tt.wantStatus, tt.wantAgg, tt.wantAuth, rec.Code, body)
			}
		})
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
