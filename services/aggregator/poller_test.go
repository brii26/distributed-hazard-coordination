package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// catat kapan tiap call mulai dan berapa yang jalan bareng.
type starts struct {
	mu       sync.Mutex
	at       []time.Time
	inflight atomic.Int32
	maxSeen  atomic.Int32
}

// fetch func yang memakan d per call dan mencatatnya.
func (s *starts) fetch(d time.Duration) func(context.Context, time.Time) (pollResult, error) {
	return func(ctx context.Context, _ time.Time) (pollResult, error) {
		s.mu.Lock()
		s.at = append(s.at, time.Now())
		s.mu.Unlock()

		n := s.inflight.Add(1)
		defer s.inflight.Add(-1)

		for {
			m := s.maxSeen.Load()
			if n <= m || s.maxSeen.CompareAndSwap(m, n) {
				break
			}
		}

		select {
		case <-ctx.Done():
		case <-time.After(d):
		}

		return pollResult{}, nil
	}
}

// jarak antar start call yang tercatat.
func (s *starts) gaps() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []time.Duration
	for i := 1; i < len(s.at); i++ {
		out = append(out, s.at[i].Sub(s.at[i-1]))
	}

	return out
}

// channel yang cukup besar supaya poller test nggak pernah menunggu.
func sink() chan pollResult {
	return make(chan pollResult, 256)
}

// breaker yang nggak pernah trip, untuk test soal hal lain.
func testBreaker() *breaker {
	return newBreaker("test", 1000, time.Hour)
}

func runFor(p *poller, d time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	p.run(ctx)
}

func TestNextWait(t *testing.T) {
	t.Parallel()

	for _, c := range []struct{ interval, elapsed, want time.Duration }{
		{3 * time.Second, 0, 3 * time.Second},
		{3 * time.Second, time.Second, 2 * time.Second},
		{3 * time.Second, 3 * time.Second, 0},
		{3 * time.Second, 4 * time.Second, 0},
	} {
		if got := nextWait(c.interval, c.elapsed); got != c.want {
			t.Fatalf("nextWait(%v, %v) = %v, want %v", c.interval, c.elapsed, got, c.want)
		}
	}
}

func TestSlowCallFiresNextImmediatelyWithoutOverlap(t *testing.T) {
	t.Parallel()

	var s starts

	runFor(&poller{name: "slow", interval: 100 * time.Millisecond, fetch: s.fetch(150 * time.Millisecond), breaker: testBreaker(), out: sink()}, 800*time.Millisecond)

	if s.maxSeen.Load() != 1 {
		t.Fatalf("saw %d calls in flight at once, want 1", s.maxSeen.Load())
	}

	gaps := s.gaps()
	if len(gaps) < 3 {
		t.Fatalf("expected several calls, got %d gaps", len(gaps))
	}

	for _, g := range gaps {
		if g < 145*time.Millisecond || g > 210*time.Millisecond {
			t.Fatalf("gap %v, want about the 150 ms call duration", g)
		}
	}
}

func TestWaitIsIntervalMinusCallDuration(t *testing.T) {
	t.Parallel()

	var s starts

	runFor(&poller{name: "ok", interval: 100 * time.Millisecond, fetch: s.fetch(30 * time.Millisecond), breaker: testBreaker(), out: sink()}, 600*time.Millisecond)

	gaps := s.gaps()
	if len(gaps) < 3 {
		t.Fatalf("expected several calls, got %d gaps", len(gaps))
	}

	for _, g := range gaps {
		if g < 95*time.Millisecond || g > 150*time.Millisecond {
			t.Fatalf("gap %v, want about the 100 ms interval", g)
		}
	}
}

func TestSlowSourceDoesNotDelayTheOther(t *testing.T) {
	t.Parallel()

	var fast, slow starts

	var wg sync.WaitGroup

	for _, p := range []*poller{
		{name: sourceBMKG, interval: 50 * time.Millisecond, fetch: fast.fetch(time.Millisecond), breaker: testBreaker(), out: sink()},
		{name: sourcePVMBG, interval: 50 * time.Millisecond, fetch: slow.fetch(400 * time.Millisecond), breaker: testBreaker(), out: sink()},
	} {
		wg.Go(func() { runFor(p, 700*time.Millisecond) })
	}

	wg.Wait()

	gaps := fast.gaps()
	if len(gaps) < 8 {
		t.Fatalf("fast source only polled %d times while the slow one ran", len(gaps)+1)
	}

	for _, g := range gaps {
		if g > 100*time.Millisecond {
			t.Fatalf("fast source gap %v, want about its 50 ms interval", g)
		}
	}
}

func TestCallPastTimeoutIsCancelled(t *testing.T) {
	t.Parallel()

	cancelled := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(cancelled)
	}))

	t.Cleanup(srv.Close)

	c := &sourceClient{baseURL: srv.URL, timeout: 50 * time.Millisecond, client: &http.Client{}, auth: bmkgAuth("k")}

	guard, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()

	start := time.Now()
	_, err := c.records(guard, "/seismic-events", time.Time{})

	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("want a timeout error within the limit, got %v after %v", err, time.Since(start))
	}

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("the server never saw the request being cancelled")
	}
}

func TestBMKGSendsKeyAndAdvancesSince(t *testing.T) {
	t.Parallel()

	var (
		mu     sync.Mutex
		sinces []string
		keys   []string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()

		keys = append(keys, r.Header.Get("X-BMKG-Key"))
		sinces = append(sinces, r.URL.Path+"?"+r.URL.Query().Get("since"))
		mu.Unlock()

		if r.URL.Path == "/seismic-events" {
			_, _ = w.Write([]byte(`[{"occurred_at":"2026-10-04T10:00:00Z"},{"occurred_at":"2026-10-04T10:05:00Z"}]`))

			return
		}

		_, _ = w.Write([]byte(`[]`))
	}))

	t.Cleanup(srv.Close)

	c := &sourceClient{baseURL: srv.URL, timeout: time.Second, client: &http.Client{}, auth: bmkgAuth("secret")}
	p := &poller{name: sourceBMKG, fetch: bmkgFetch(c), breaker: testBreaker(), out: sink()}

	p.pollOnce(context.Background())
	p.pollOnce(context.Background())

	mu.Lock()
	defer mu.Unlock()

	for _, k := range keys {
		if k != "secret" {
			t.Fatalf("X-BMKG-Key = %q on a request", k)
		}
	}

	want := []string{"/seismic-events?", "/tsunami-warnings?", "/seismic-events?2026-10-04T10:05:00Z", "/tsunami-warnings?2026-10-04T10:05:00Z"}
	if len(sinces) != len(want) {
		t.Fatalf("requests = %v, want %v", sinces, want)
	}

	for i := range want {
		if sinces[i] != want[i] {
			t.Fatalf("request %d = %q, want %q", i, sinces[i], want[i])
		}
	}
}

func TestPVMBGBearerTokenAndFailures(t *testing.T) {
	t.Parallel()

	var (
		calls atomic.Int32
		auth  atomic.Value
		code  atomic.Int32
	)

	code.Store(http.StatusOK)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		auth.Store(r.Header.Get("Authorization"))

		if c := int(code.Load()); c != http.StatusOK {
			w.WriteHeader(c)

			return
		}

		_, _ = w.Write([]byte(`[{"reported_at":"2026-10-04T11:00:00Z"}]`))
	}))

	t.Cleanup(srv.Close)

	token := ""
	c := &sourceClient{baseURL: srv.URL, timeout: time.Second, client: &http.Client{}, auth: pvmbgAuth(func() string { return token })}
	p := &poller{name: sourcePVMBG, fetch: pvmbgFetch(c, func() error { return nil }), breaker: testBreaker(), out: sink()}

	p.pollOnce(context.Background())

	if calls.Load() != 0 || !p.since.IsZero() {
		t.Fatalf("without a token expected no request, got %d calls", calls.Load())
	}

	token = "tok"

	code.Store(http.StatusUnauthorized)
	p.pollOnce(context.Background())

	if auth.Load() != "Bearer tok" || !p.since.IsZero() {
		t.Fatalf("Authorization = %v, since = %v", auth.Load(), p.since)
	}

	code.Store(http.StatusOK)
	p.pollOnce(context.Background())

	if want := time.Date(2026, 10, 4, 11, 0, 0, 0, time.UTC); !p.since.Equal(want) {
		t.Fatalf("since = %v, want %v", p.since, want)
	}
}
