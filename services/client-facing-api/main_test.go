package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/aggregatorpb"
)

// fakeAggregator answers GetHealth with a fixed status, or an error as when it is unreachable.
type fakeAggregator struct {
	aggregatorpb.AggregatorClient

	status string
	err    error
}

func (f fakeAggregator) GetHealth(context.Context, *emptypb.Empty, ...grpc.CallOption) (*aggregatorpb.HealthStatus, error) {
	return &aggregatorpb.HealthStatus{Status: f.status}, f.err
}

// listener opens a local TCP port and returns its address, so the Auth Service can be "up".
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
	unreachable := errors.New("connection refused")

	tests := []struct {
		name              string
		agg               fakeAggregator
		auth              string
		wantStatus        string
		wantAgg, wantAuth string
	}{
		{"both up", fakeAggregator{status: "ok"}, up, "ok", "connected", "connected"},
		{"aggregator limping on a dead source is still usable", fakeAggregator{status: "degraded"}, up, "ok", "connected", "connected"},
		{"aggregator unreachable", fakeAggregator{err: unreachable}, up, "degraded", "disconnected", "connected"},
		{"aggregator up but its Canonical Store is gone", fakeAggregator{status: "down"}, up, "degraded", "disconnected", "connected"},
		{"auth service down", fakeAggregator{status: "ok"}, down, "degraded", "connected", "disconnected"},
		{"both down is still degraded, never down", fakeAggregator{err: unreachable}, down, "degraded", "disconnected", "disconnected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
			rec := httptest.NewRecorder()

			healthAPI{aggregator: tt.agg, authAddr: tt.auth, timeout: time.Second}.ServeHTTP(rec, req)

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
