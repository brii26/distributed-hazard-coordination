package main

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/brii26/distributed-hazard-coordination/services/aggregator/aggregatorpb"
)

func TestLoadShutdownConfig(t *testing.T) { //nolint:paralleltest // uses t.Setenv
	c, err := loadShutdownConfig()
	if err != nil || c.drainDelay != defaultDrainDelay || c.timeout != defaultShutdownTimeout {
		t.Fatalf("defaults: %+v %v", c, err)
	}

	t.Setenv("SHUTDOWN_TIMEOUT", "3s")
	t.Setenv("SHUTDOWN_DRAIN_DELAY", "100ms")

	if c, err = loadShutdownConfig(); err != nil || c.timeout != 3*time.Second || c.drainDelay != 100*time.Millisecond {
		t.Fatalf("env: %+v %v", c, err)
	}

	t.Setenv("SHUTDOWN_TIMEOUT", "soon")

	if _, err = loadShutdownConfig(); err == nil {
		t.Fatal("a bad duration must be an error")
	}
}

type shutdownRig struct {
	api      *readAPI
	httpSrv  *http.Server
	grpcSrv  *grpc.Server
	httpAddr string
	grpcAddr string
	client   aggregatorpb.AggregatorClient
	started  chan struct{}
	finish   chan struct{}
}

// Real listeners on loopback. The GetHazards handler blocks until finish is closed, so a call can be in flight during the shutdown.
func newShutdownRig(t *testing.T) *shutdownRig {
	t.Helper()

	r := &shutdownRig{started: make(chan struct{}, 1), finish: make(chan struct{})}
	r.api = &readAPI{
		list: func(ctx context.Context, _ listQuery) ([]HazardEvent, bool, error) {
			r.started <- struct{}{}

			select {
			case <-r.finish:
			case <-ctx.Done():
			}

			return nil, false, nil
		},
		ping:     func(context.Context) error { return nil },
		rabbitUp: func(context.Context) error { return nil },
		breakers: map[string]*breaker{
			sourceBMKG:  newBreaker(sourceBMKG, 1, time.Hour),
			sourcePVMBG: newBreaker(sourcePVMBG, 1, time.Hour),
		},
		sem: make(chan struct{}, 10),
	}

	r.grpcSrv = newGRPCServer(r.api)

	glis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	hlis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	r.grpcAddr, r.httpAddr = glis.Addr().String(), hlis.Addr().String()
	r.httpSrv = &http.Server{Handler: newMux(r.api), ReadHeaderTimeout: time.Second}

	go func() { _ = r.grpcSrv.Serve(glis) }()
	go func() { _ = r.httpSrv.Serve(hlis) }()

	t.Cleanup(func() { r.grpcSrv.Stop(); _ = r.httpSrv.Close() })

	conn, err := grpc.NewClient(r.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	r.client = aggregatorpb.NewAggregatorClient(conn)

	return r
}

func (r *shutdownRig) healthCode(t *testing.T) int {
	t.Helper()

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+r.httpAddr+"/health", http.NoBody)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	_ = resp.Body.Close()

	return resp.StatusCode
}

func (r *shutdownRig) dials() bool {
	c, err := (&net.Dialer{Timeout: 200 * time.Millisecond}).DialContext(context.Background(), "tcp", r.httpAddr)
	if err != nil {
		return false
	}

	_ = c.Close()

	return true
}

// Real Postgres: readiness flips first and the listener is still open for the drain delay, an in-flight call finishes, and only then the lease is released.
func TestGracefulShutdownOrderWithLease(t *testing.T) { //nolint:paralleltest // shares the lease row with other runs
	s := testStore(t, 4)
	r := newShutdownRig(t)
	l := pgLease{db: s.db}
	src := sourceBMKG

	clear := func() {
		_, _ = s.db.ExecContext(context.Background(), `UPDATE source_lease SET holder = NULL, expires_at = NULL WHERE source = $1`, src)
	}

	clear()
	t.Cleanup(clear)

	el := newElector(src, leaseConfig{ttl: time.Minute, renew: 50 * time.Millisecond}, l)
	workCtx, cancelWork := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Go(func() { el.run(workCtx, func(ctx context.Context) { <-ctx.Done() }) })

	untilTrue(t, "lease held", func() bool { return el.isLeader.Load() == 1 })

	if r.healthCode(t) != http.StatusOK {
		t.Fatal("must be ready before the signal")
	}

	var (
		mu     sync.Mutex
		events []string
	)

	note := func(e string) { mu.Lock(); events = append(events, e); mu.Unlock() }

	callErr := make(chan error, 1)

	go func() {
		_, err := r.client.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeSeismic})
		note("inflight done")

		callErr <- err
	}()

	<-r.started

	cfg := shutdownConfig{drainDelay: 600 * time.Millisecond, timeout: 5 * time.Second}
	done := make(chan struct{})

	go func() {
		defer close(done)

		gracefulShutdown(cfg, r.api, r.httpSrv, r.grpcSrv, func() {
			note("release work")
			cancelWork()
			wg.Wait()
			note("work stopped")
		})
	}()

	untilTrue(t, "health 503", func() bool { return r.api.draining.Load() })

	if c := r.healthCode(t); c != http.StatusServiceUnavailable {
		t.Fatalf("/health while draining = %d, want 503", c)
	}

	st, err := r.api.hs.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil || st.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("grpc health = %v %v, want NOT_SERVING", st, err)
	}

	if !r.dials() {
		t.Fatal("the listener must stay open during the drain delay")
	}

	if el.isLeader.Load() != 1 || leaseHolder(t, s, src) == "" {
		t.Fatal("the lease must be held until the in-flight call is done")
	}

	untilTrue(t, "listener closed", func() bool { return !r.dials() })

	select {
	case <-done:
		t.Fatal("shutdown finished while a call was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	close(r.finish)

	if err := <-callErr; err != nil {
		t.Fatalf("the in-flight call must finish OK, got %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish")
	}

	if h := leaseHolder(t, s, src); h != "" {
		t.Fatalf("lease still held by %q after shutdown", h)
	}

	mu.Lock()
	defer mu.Unlock()

	want := []string{"inflight done", "release work", "work stopped"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}

	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}

	// Another instance takes the lease at once, with the next epoch and no wait for the TTL.
	epoch, ok, err := l.acquire(context.Background(), src, newHazardID(), time.Minute)
	if err != nil || !ok || epoch != el.epoch+1 {
		t.Fatalf("takeover: epoch %d ok=%v err=%v, want epoch %d", epoch, ok, err, el.epoch+1)
	}
}

func TestGracefulShutdownTimeoutForcesClose(t *testing.T) {
	t.Parallel()

	r := newShutdownRig(t)
	released := false
	done := make(chan struct{})

	go func() {
		_, _ = r.client.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeSeismic})
	}()

	<-r.started

	go func() {
		defer close(done)

		gracefulShutdown(shutdownConfig{drainDelay: 10 * time.Millisecond, timeout: 300 * time.Millisecond}, r.api, r.httpSrv, r.grpcSrv, func() { released = true })
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a stuck call must not block shutdown past the timeout")
	}

	if !released {
		t.Fatal("work must be released even after a timeout")
	}
}

func untilTrue(t *testing.T, what string, f func() bool) {
	t.Helper()

	for range 400 {
		if f() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", what)
}

func leaseHolder(t *testing.T, s *store, src string) string {
	t.Helper()

	var h *string
	if err := s.db.QueryRowContext(context.Background(), `SELECT holder::text FROM source_lease WHERE source = $1`, src).Scan(&h); err != nil {
		t.Fatal(err)
	}

	if h == nil {
		return ""
	}

	return *h
}
