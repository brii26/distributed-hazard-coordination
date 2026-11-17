package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	modeOK = iota
	modeUnavailable
	modeDelay
)

// A sensor whose behavior the test switches at any time. It counts calls and the most that ran at once.
type flakySensor struct {
	mode     atomic.Int32
	delay    atomic.Int64 // nanoseconds, used by modeDelay
	obsCalls atomic.Int64
	inflight atomic.Int32
	maxSeen  atomic.Int32
	gate     chan struct{} // when set, observations calls wait for it to close
	paged    bool          // every page names a next cursor, so a poll never runs out of pages
	srv      *httptest.Server
}

func newFlakySensor(t *testing.T) *flakySensor {
	t.Helper()

	f := &flakySensor{}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)

	return f
}

func (f *flakySensor) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/stations" {
		_, _ = fmt.Fprint(w, `{"items":[{"station_id":"s-1","name":"Padang","province":"Sumatera Barat","latitude":-0.9,"longitude":100.3}]}`)

		return
	}

	n := f.inflight.Add(1)
	defer f.inflight.Add(-1)

	for {
		m := f.maxSeen.Load()
		if n <= m || f.maxSeen.CompareAndSwap(m, n) {
			break
		}
	}

	calls := f.obsCalls.Add(1)

	if f.gate != nil {
		select {
		case <-f.gate:
		case <-r.Context().Done():
			return
		}
	}

	switch f.mode.Load() {
	case modeUnavailable:
		http.Error(w, `{"error":{"code":"unavailable"}}`, http.StatusServiceUnavailable)

		return
	case modeDelay:
		select {
		case <-time.After(time.Duration(f.delay.Load())):
		case <-r.Context().Done():
			return
		}
	}

	next := "null"
	if f.paged {
		next = `"` + base64.StdEncoding.EncodeToString(fmt.Appendf(nil, `{"o":%d}`, calls)) + `"`
	}

	_, _ = fmt.Fprintf(w, `{"generated_at":"2026-01-01T12:00:00Z","items":[],"next_cursor":%s}`, next)
}

// A sensor client and poller wired to a flakySensor, with a breaker on a fake clock.
type sensorRig struct {
	f     *flakySensor
	s     *sensorClient
	p     *poller
	br    *breaker
	clock *fakeClock
}

func newSensorRig(t *testing.T, cfg sensorConfig) *sensorRig {
	t.Helper()

	cfg = cfg.orDefaults()
	f := newFlakySensor(t)
	st := &memState{}
	s := st.client(f.srv.URL, testToken)
	s.http.timeout = cfg.timeout
	s.bulk = make(chan struct{}, cfg.maxConcurrent)
	s.maxPages = cfg.maxPages
	s.slowAfter = cfg.slowAfter
	s.stats = &sensorStats{}
	s.stats.inFlight = func() int { return len(s.bulk) }

	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	br := newBreaker(sourceSensorPantai, cfg.cbThreshold, cfg.cbOpenFor)
	br.now = clock.now
	s.stats.breaker = br

	p := &poller{
		name: sourceSensorPantai, interval: time.Millisecond, fetch: s.fetch, breaker: br, out: make(chan pollResult, 8),
		backoff: newBackoff(cfg.backoffBase, cfg.backoffMax), onReject: func() { s.stats.breakerRejected() }, onWait: s.stats.setBackoff,
	}

	return &sensorRig{f: f, s: s, p: p, br: br, clock: clock}
}

func TestSensorBreakerOpensAndNoRequestsWhileOpen(t *testing.T) {
	r := newSensorRig(t, sensorConfig{cbThreshold: 3, cbOpenFor: 10 * time.Second, backoffBase: time.Millisecond, backoffMax: 2 * time.Millisecond})
	r.f.mode.Store(modeUnavailable)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() { r.p.loop(ctx); close(done) }()

	defer func() { cancel(); <-done }()

	eventually(t, "breaker open", func() bool { return r.br.currentState() == stateOpen })

	opened := r.f.obsCalls.Load()
	if opened != 3 {
		t.Fatalf("requests before the breaker opened = %d, want the threshold 3", opened)
	}

	// The clock does not move, so the breaker stays open. The poller keeps knocking and every knock is turned away.
	eventually(t, "rejections", func() bool { return r.s.stats.rejectedBreaker.Load() >= 20 })

	if got := r.f.obsCalls.Load(); got != opened {
		t.Fatalf("requests while open = %d, want 0", got-opened)
	}
}

func TestSensorHalfOpenProbeAndRecovery(t *testing.T) {
	r := newSensorRig(t, sensorConfig{cbThreshold: 2, cbOpenFor: 10 * time.Second, backoffBase: time.Millisecond, backoffMax: 2 * time.Millisecond})
	r.f.mode.Store(modeUnavailable)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() { r.p.loop(ctx); close(done) }()

	defer func() { cancel(); <-done }()

	eventually(t, "breaker open", func() bool { return r.br.currentState() == stateOpen })

	// The open time ends but the sensor is still down: one probe, and the breaker opens again.
	before := r.f.obsCalls.Load()

	r.clock.advance(11 * time.Second)

	eventually(t, "failed probe", func() bool { return r.f.obsCalls.Load() == before+1 })
	eventually(t, "open again", func() bool { return r.br.currentState() == stateOpen })

	time.Sleep(20 * time.Millisecond)

	if got := r.f.obsCalls.Load(); got != before+1 {
		t.Fatalf("a failed probe was followed by %d more requests", got-before-1)
	}

	// The sensor recovers: the next probe closes the breaker and polling goes on without a restart.
	r.f.mode.Store(modeOK)
	r.clock.advance(11 * time.Second)

	eventually(t, "breaker closed", func() bool { return r.br.currentState() == stateClosed })

	closed := r.f.obsCalls.Load()

	eventually(t, "polling resumes", func() bool { return r.f.obsCalls.Load() >= closed+5 })

	if snap := r.br.snapshot(); snap.Status != statusUp || snap.StaleSince != nil {
		t.Fatalf("status after recovery = %+v", snap)
	}
}

func TestSensorRequestsBoundedDuringOutage(t *testing.T) {
	// Real time for the breaker: open 100ms, backoff 2ms to 10ms, polls every 1ms, for 1s.
	r := newSensorRig(t, sensorConfig{cbThreshold: 3, cbOpenFor: 100 * time.Millisecond, backoffBase: 2 * time.Millisecond, backoffMax: 10 * time.Millisecond})
	r.br.now = time.Now
	r.f.mode.Store(modeUnavailable)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r.p.loop(ctx)

	// The first three requests open the breaker, then at most one probe per open period.
	const bound = 3 + 1000/100 + 2

	if got := r.f.obsCalls.Load(); got > bound || got < 3 {
		t.Fatalf("requests in 1s of outage = %d, want 3 to %d", got, bound)
	}
}

func TestSensorSlowResponsesCountAsFailures(t *testing.T) {
	r := newSensorRig(t, sensorConfig{cbThreshold: 2, slowAfter: 10 * time.Millisecond, timeout: time.Second})
	r.f.mode.Store(modeDelay)
	r.f.delay.Store(int64(40 * time.Millisecond))

	for range 2 {
		if got := r.p.pollOnce(context.Background()); got != pollFailed {
			t.Fatalf("slow poll outcome = %v, want failed", got)
		}
	}

	if r.br.currentState() != stateOpen {
		t.Fatalf("state = %v, want OPEN after repeated slow answers", r.br.currentState())
	}

	calls := r.f.obsCalls.Load()

	if got := r.p.pollOnce(context.Background()); got != pollSkipped || r.f.obsCalls.Load() != calls {
		t.Fatalf("a poll went through the open breaker")
	}

	if r.s.stats.slow.Load() != 2 {
		t.Fatalf("slow count = %d, want 2", r.s.stats.slow.Load())
	}
}

func TestSensorTimeoutCountsAsFailure(t *testing.T) {
	r := newSensorRig(t, sensorConfig{cbThreshold: 2, timeout: 30 * time.Millisecond, slowAfter: 20 * time.Millisecond})
	r.f.mode.Store(modeDelay)
	r.f.delay.Store(int64(5 * time.Second))

	start := time.Now()

	for range 2 {
		r.p.pollOnce(context.Background())
	}

	if r.br.currentState() != stateOpen {
		t.Fatalf("state = %v, want OPEN after timeouts", r.br.currentState())
	}

	if time.Since(start) > 2*time.Second {
		t.Fatal("a hanging sensor held the poller past its timeout")
	}
}

func TestSensorBulkheadCapsConcurrency(t *testing.T) {
	r := newSensorRig(t, sensorConfig{maxConcurrent: 2})
	r.f.gate = make(chan struct{})

	var wg sync.WaitGroup

	for range 2 {
		wg.Go(func() {
			_ = r.s.get(context.Background(), "/v1/observations", "", func(io.Reader) error { return nil })
		})
	}

	eventually(t, "two calls in flight", func() bool { return r.f.inflight.Load() == 2 })

	var rejected atomic.Int32

	for range 8 {
		wg.Go(func() {
			if err := r.s.get(context.Background(), "/v1/observations", "", func(io.Reader) error { return nil }); errors.Is(err, errBulkheadFull) {
				rejected.Add(1)
			}
		})
	}

	eventually(t, "extra calls turned away", func() bool { return rejected.Load() == 8 })

	if got := r.s.stats.inFlight(); got != 2 {
		t.Fatalf("in flight = %d, want 2", got)
	}

	close(r.f.gate)
	wg.Wait()

	if r.f.maxSeen.Load() != 2 {
		t.Fatalf("sensor saw %d calls at once, want the limit 2", r.f.maxSeen.Load())
	}

	if r.s.stats.rejectedBulkhead.Load() != 8 || r.s.stats.requests.Load() != 2 {
		t.Fatalf("rejected %d, requests %d", r.s.stats.rejectedBulkhead.Load(), r.s.stats.requests.Load())
	}
}

func TestSensorPagesPerPollBounded(t *testing.T) {
	r := newSensorRig(t, sensorConfig{maxPages: 3})
	r.f.paged = true

	if _, err := r.s.fetch(context.Background(), time.Time{}); err != nil {
		t.Fatal(err)
	}

	if got := r.f.obsCalls.Load(); got != 3 {
		t.Fatalf("pages in one poll = %d, want 3", got)
	}
}

func TestSensorDownDoesNotStopOtherSources(t *testing.T) {
	r := newSensorRig(t, sensorConfig{cbThreshold: 2, timeout: 50 * time.Millisecond, slowAfter: 20 * time.Millisecond})
	r.f.mode.Store(modeDelay)
	r.f.delay.Store(int64(5 * time.Second))

	var other atomic.Int64

	bmkg := &poller{
		name: sourceBMKG, interval: 2 * time.Millisecond, breaker: newBreaker(sourceBMKG, 3, time.Second), out: make(chan pollResult, 1),
		fetch: func(context.Context, time.Time) (pollResult, error) { other.Add(1); return pollResult{}, nil },
	}

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Go(func() { r.p.loop(ctx) })
	wg.Go(func() { bmkg.loop(ctx) })

	eventually(t, "sensor breaker open", func() bool { return r.br.currentState() == stateOpen })

	n := other.Load()

	eventually(t, "other source polling during the outage", func() bool { return other.Load() >= n+20 })

	cancel()
	wg.Wait()
}

func TestSensorClientHasItsOwnPool(t *testing.T) {
	c := newSensorClient(pollConfig{pantaiURL: "http://s", pantaiToken: "t", sensor: sensorConfig{maxConcurrent: 3}}, nil)

	tr, ok := c.http.client.Transport.(*http.Transport)
	if !ok || tr == http.DefaultTransport || tr.MaxConnsPerHost != 3 || cap(c.bulk) != 3 {
		t.Fatalf("sensor transport is not its own capped pool: %#v", c.http.client.Transport)
	}

	if c.http.timeout != time.Second {
		t.Fatalf("sensor timeout = %v, want 1s", c.http.timeout)
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	b := newBackoff(time.Second, 8*time.Second)
	b.rnd = func() float64 { return 0 } // no jitter: the full delay

	var got []time.Duration
	for range 6 {
		got = append(got, b.failed())
	}

	want := []time.Duration{1, 2, 4, 8, 8, 8}
	for i := range want {
		if got[i] != want[i]*time.Second {
			t.Fatalf("delays = %v, want 1s 2s 4s 8s 8s 8s", got)
		}
	}

	// Full jitter takes off up to half, never more and never past the cap.
	b.rnd = func() float64 { return 0.999999 }

	if d := b.failed(); d < 4*time.Second || d > 8*time.Second {
		t.Fatalf("jittered delay = %v, want 4s to 8s", d)
	}

	for range 100 {
		b.failed()
	}

	if b.last > 8*time.Second {
		t.Fatalf("delay after many failures = %v, above the cap", b.last)
	}

	b.reset()

	if b.last != 0 || b.failed() < 500*time.Millisecond {
		t.Fatal("reset did not start the delay over")
	}
}

func TestBackoffStretchesTheWaitBetweenFailedPolls(t *testing.T) {
	r := newSensorRig(t, sensorConfig{cbThreshold: 1000, backoffBase: 40 * time.Millisecond, backoffMax: 40 * time.Millisecond})
	r.p.backoff.rnd = func() float64 { return 0 }
	r.f.mode.Store(modeUnavailable)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	r.p.loop(ctx)

	// Without backoff a 1ms interval would make about 300 calls.
	if got := r.f.obsCalls.Load(); got < 4 || got > 9 {
		t.Fatalf("requests in 300ms = %d, want about 8 (one per 40ms)", got)
	}
}

func TestSensorMetricsText(t *testing.T) {
	r := newSensorRig(t, sensorConfig{cbThreshold: 1})
	r.f.mode.Store(modeUnavailable)
	r.p.pollOnce(context.Background())
	r.p.pollOnce(context.Background())

	var sb strings.Builder

	r.s.stats.writeMetrics(&sb)

	for _, line := range []string{
		"aggregator_sensor_breaker_state 1\n",
		"aggregator_sensor_requests_total 2\n",
		"aggregator_sensor_rejected_by_breaker_total 1\n",
		"aggregator_sensor_rejected_by_bulkhead_total 0\n",
		"aggregator_sensor_in_flight 0\n",
		"# TYPE aggregator_sensor_backoff_seconds gauge\n",
	} {
		if !strings.Contains(sb.String(), line) {
			t.Fatalf("metrics lack %q:\n%s", line, sb.String())
		}
	}
}

func TestSensorConfigFromEnv(t *testing.T) {
	c, err := loadSensorConfig()
	if err != nil || c != (sensorConfig{
		timeout: time.Second, slowAfter: 800 * time.Millisecond, cbThreshold: 3, cbOpenFor: 10 * time.Second,
		backoffBase: 3 * time.Second, backoffMax: 30 * time.Second, maxConcurrent: 2, maxPages: 20, staleAfter: time.Minute,
	}) {
		t.Fatalf("defaults = %+v, %v", c, err)
	}

	t.Setenv("SENSOR_CB_FAILURE_THRESHOLD", "5")
	t.Setenv("SENSOR_CB_OPEN_MS", "2500")
	t.Setenv("SENSOR_MAX_CONCURRENT", "4")

	if c, err = loadSensorConfig(); err != nil || c.cbThreshold != 5 || c.cbOpenFor != 2500*time.Millisecond || c.maxConcurrent != 4 {
		t.Fatalf("custom = %+v, %v", c, err)
	}

	for name, val := range map[string]string{
		"SENSOR_TIMEOUT_MS": "0", "SENSOR_SLOW_MS": "-1", "SENSOR_MAX_PAGES": "x", "SENSOR_BACKOFF_MAX_MS": "100", "SENSOR_SLOW_MS ": "2000",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(strings.TrimSpace(name), val)

			if _, err := loadSensorConfig(); err == nil {
				t.Fatalf("%s=%s was accepted", name, val)
			}
		})
	}
}

func TestSensorStaleWhenClockStandsStill(t *testing.T) {
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	br := newBreaker(sourceSensorPantai, 3, time.Second)

	var stale bool

	s := &sensorClient{staleAfter: 60 * time.Second, now: func() time.Time { return now }}
	s.onStale = func(v bool, since time.Time) { stale = v; br.setStale(v, since) }

	sensorTime := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)

	s.checkFresh(pollResult{generatedAt: sensorTime})

	if stale {
		t.Fatal("first poll must not be stale")
	}

	now = now.Add(30 * time.Second)
	s.checkFresh(pollResult{generatedAt: sensorTime.Add(30 * time.Second)})

	now = now.Add(59 * time.Second)
	s.checkFresh(pollResult{generatedAt: sensorTime.Add(30 * time.Second)})

	if stale || br.snapshot().Status != statusUp {
		t.Fatalf("59s without progress is still fresh, got stale=%v %v", stale, br.snapshot().Status)
	}

	now = now.Add(2 * time.Second)
	s.checkFresh(pollResult{generatedAt: sensorTime.Add(30 * time.Second)})

	snap := br.snapshot()
	if !stale || snap.Status != statusStale || snap.StaleSince == nil {
		t.Fatalf("61s without progress must be STALE with stale_since, got %v %+v", stale, snap)
	}

	now = now.Add(time.Second)
	s.checkFresh(pollResult{generatedAt: sensorTime.Add(time.Hour)})

	if stale || br.snapshot().Status != statusUp || br.snapshot().StaleSince != nil {
		t.Fatalf("a moving clock must clear STALE, got %v %+v", stale, br.snapshot())
	}
}

func TestSensorStaleCountsNewObservations(t *testing.T) {
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)

	var stale bool

	s := &sensorClient{staleAfter: 10 * time.Second, now: func() time.Time { return now }, onStale: func(v bool, _ time.Time) { stale = v }}
	g := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)

	s.checkFresh(pollResult{generatedAt: g})

	now = now.Add(20 * time.Second)
	s.checkFresh(pollResult{generatedAt: g, records: []map[string]any{{"observed_at": g.Add(time.Minute).Format(time.RFC3339Nano)}}})

	if stale {
		t.Fatal("a newer observed_at is progress")
	}
}

func TestStaleDoesNotHideDown(t *testing.T) {
	br := newBreaker(sourceSensorPantai, 1, time.Hour)
	br.setStale(true, time.Now())
	br.failure()

	if br.snapshot().Status != statusDown {
		t.Fatalf("an open breaker is DOWN even when stale, got %v", br.snapshot().Status)
	}
}

func TestStaleConfigDefaultAndEnv(t *testing.T) {
	if got := (sensorConfig{}).orDefaults().staleAfter; got != defaultSensorStaleAfter {
		t.Fatalf("default stale limit %v", got)
	}

	t.Setenv("SENSOR_STALE_AFTER_MS", "5000")

	c, err := loadSensorConfig()
	if err != nil || c.staleAfter != 5*time.Second {
		t.Fatalf("env not read: %v %v", c.staleAfter, err)
	}
}
