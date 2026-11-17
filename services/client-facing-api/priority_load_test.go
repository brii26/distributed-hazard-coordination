package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/aggregatorpb"
)

// TestPriorityLoadHarness is an opt-in HTTP fixture for k6. It exercises the real
// JWT, per-account limiter, admission and hazards handler with a 100 ms upstream.
// It does not claim to measure database, broker or Kubernetes performance.
func TestPriorityLoadHarness(t *testing.T) {
	t.Parallel()

	dir := os.Getenv("PRIORITY_LOAD_DIR")
	if dir == "" {
		t.Skip("run tests/load/priority.sh to start the k6 fixture")
	}

	v := newJWTVerifier([]byte(testSecret))
	v.limiter = newRateLimiter(100_000)
	l := newInflightLimiter(8, 4, v)
	mux := http.NewServeMux()
	mux.Handle("GET /hazards", v.require(hazardsAPI{agg: delayedHazards{fakeHazards{resp: oneEvent(t)}}, timeout: time.Second}))
	mux.HandleFunc("GET /metrics", l.metrics)

	stop := make(chan struct{})

	mux.HandleFunc("POST /stop", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
		close(stop)
	})

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := &http.Server{Handler: withCorrelation(l.wrap(mux)), ReadHeaderTimeout: time.Second}

	t.Cleanup(func() { _ = srv.Close() })

	go func() { _ = srv.Serve(lis) }()

	config := map[string]string{"base_url": "http://" + lis.Addr().String()}
	for key, role := range map[string]string{"media_jwt": roleMedia, "field_jwt": roleLapangan} {
		config[key] = sign(t, testSecret, hs256, jwtClaims{Email: role + "@load.example", Role: role, Exp: time.Now().Add(10 * time.Minute).Unix()})
	}

	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}

	output, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = output.Close() }()

	if err := output.WriteFile("config.json", data, 0o600); err != nil {
		t.Fatal(err)
	}

	select {
	case <-stop:
	case <-time.After(5 * time.Minute):
		t.Fatal("k6 fixture was not stopped")
	}
}

type delayedHazards struct{ fakeHazards }

func (d delayedHazards) GetHazards(ctx context.Context, in *aggregatorpb.GetHazardsRequest, opts ...grpc.CallOption) (*aggregatorpb.GetHazardsResponse, error) {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return d.fakeHazards.GetHazards(ctx, in, opts...)
	}
}
