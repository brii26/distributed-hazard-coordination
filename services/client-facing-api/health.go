package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// healthAPI answers GET /health: liveness plus whether the Aggregator and the Auth
// Service can be reached (doc/TRD_M1.md 4). The service never reports "down" for a
// missing dependency: a valid JWT still verifies locally and the other side may
// still work, so that is "degraded" and stays a 200.
type healthAPI struct {
	aggregatorAddr, authAddr string
	timeout                  time.Duration // UPSTREAM_TIMEOUT_MS, per dependency
}

// reachable reports whether a TCP connection to addr opens.
// ponytail: only proves the port answers, not that gRPC serves behind it; swap in
// a gRPC health call if a half-up dependency ever matters.
func (h healthAPI) reachable(ctx context.Context, addr string) bool {
	d := net.Dialer{Timeout: h.timeout}

	conn, err := d.DialContext(ctx, "tcp", addr)
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

	wg.Go(func() { agg = h.reachable(r.Context(), h.aggregatorAddr) })
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
