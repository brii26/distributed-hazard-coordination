package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/aggregatorpb"
)

// healthAPI answers GET /health: liveness plus whether the Aggregator and the Auth
// Service can be reached (doc/TRD_M1.md 4). The service never reports "down" for a
// missing dependency: a valid JWT still verifies locally and the other side may
// still work, so that is "degraded" and stays a 200.
type healthAPI struct {
	aggregator aggregatorpb.AggregatorClient
	authAddr   string
	timeout    time.Duration // UPSTREAM_TIMEOUT_MS, per dependency
}

// aggregatorUp asks the Aggregator for its own health, so an Aggregator that is
// running but cannot reach its Canonical Store ("down") counts as unusable too.
func (h healthAPI) aggregatorUp(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	st, err := h.aggregator.GetHealth(ctx, &emptypb.Empty{})

	return err == nil && st.GetStatus() != "down"
}

// reachable reports whether a TCP connection to addr opens.
// ponytail: only proves the port answers, not that gRPC serves behind it. The Auth
// Service has no cheap health RPC; add one if a half-up Auth Service ever matters.
func (h healthAPI) reachable(ctx context.Context, addr string) bool {
	d := net.Dialer{Timeout: h.timeout}

	start := time.Now()
	conn, err := d.DialContext(ctx, "tcp", addr)
	logCall(ctx, "tcp "+addr, start, err)

	if err != nil {
		return false
	}

	_ = conn.Close()

	return true
}

func state(up bool) string {
	if up {
		return "connected"
	}

	return "disconnected"
}

func (h healthAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var agg, auth bool

	var wg sync.WaitGroup

	wg.Go(func() { agg = h.aggregatorUp(r.Context()) })
	wg.Go(func() { auth = h.reachable(r.Context(), h.authAddr) })
	wg.Wait()

	status := "ok"
	if !agg || !auth {
		status = "degraded"
	}

	w.Header().Set("Content-Type", "application/json")

	body := map[string]string{"status": status, "aggregator": state(agg), "auth_service": state(auth)}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("failed to write health response: %v", err)
	}
}
