package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Manual clock shared by the fake lease table and the elector.
type leaseClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *leaseClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *leaseClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

// In-memory lease table with the same rules as the SQL, judged by the fake clock.
type fakeLease struct {
	mu      sync.Mutex
	clock   *leaseClock
	holder  string
	epoch   int64
	expires time.Time
	down    bool // every call fails, like an unreachable database
}

func (f *fakeLease) valid() bool { return f.holder != "" && f.expires.After(f.clock.now()) }

func (f *fakeLease) acquire(_ context.Context, _, holder string, ttl time.Duration) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.down {
		return 0, false, errors.New("db down")
	}

	if f.valid() {
		return 0, false, nil
	}

	f.holder, f.epoch, f.expires = holder, f.epoch+1, f.clock.now().Add(ttl)

	return f.epoch, true, nil
}

func (f *fakeLease) renew(_ context.Context, _, holder string, epoch int64, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.down {
		return false, errors.New("db down")
	}

	if !f.valid() || f.holder != holder || f.epoch != epoch {
		return false, nil
	}

	f.expires = f.clock.now().Add(ttl)

	return true, nil
}

func (f *fakeLease) release(_ context.Context, _, holder string, epoch int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.holder == holder && f.epoch == epoch {
		f.holder = ""
	}

	return nil
}

var testLeaseCfg = leaseConfig{ttl: 15 * time.Second, renew: 3 * time.Second}

func newTestElector(f *fakeLease, c *leaseClock) *elector {
	e := newElector(sourceBMKG, testLeaseCfg, f)
	e.now = c.now

	return e
}

func TestLeaseAcquireIsExclusiveAndReacquireIsRefused(t *testing.T) {
	t.Parallel()

	c := &leaseClock{t: time.Unix(1000, 0)}
	f := &fakeLease{clock: c}
	a, b := newTestElector(f, c), newTestElector(f, c)

	if got := a.step(context.Background()); got != leaseAcquired || a.epoch != 1 {
		t.Fatalf("a: %v epoch %d, want acquired at epoch 1", got, a.epoch)
	}

	if got := b.step(context.Background()); got != leaseNoChange || b.held {
		t.Fatalf("b: %v held=%v, want no change while a holds the lease", got, b.held)
	}

	if a.isLeader.Load() != 1 || b.isLeader.Load() != 0 || a.token.Load() != 1 || a.takeovers.Load() != 0 {
		t.Fatalf("metrics a leader=%d token=%d takeovers=%d, b leader=%d", a.isLeader.Load(), a.token.Load(), a.takeovers.Load(), b.isLeader.Load())
	}
}

func TestLeaseRenewKeepsItPastTheTTL(t *testing.T) {
	t.Parallel()

	c := &leaseClock{t: time.Unix(1000, 0)}
	f := &fakeLease{clock: c}
	a, b := newTestElector(f, c), newTestElector(f, c)
	a.step(context.Background())

	for range 10 {
		c.advance(testLeaseCfg.renew)

		if got := a.step(context.Background()); got != leaseNoChange || !a.held {
			t.Fatalf("renew: %v held=%v, want the lease kept", got, a.held)
		}

		if got := b.step(context.Background()); got != leaseNoChange {
			t.Fatalf("follower took a renewed lease: %v", got)
		}
	}
}

func TestLeaseTakeoverAfterExpiryAndOldLeaderLosesIt(t *testing.T) {
	t.Parallel()

	c := &leaseClock{t: time.Unix(1000, 0)}
	f := &fakeLease{clock: c}
	a, b := newTestElector(f, c), newTestElector(f, c)
	a.step(context.Background())

	c.advance(testLeaseCfg.ttl + time.Second) // a is frozen or dead

	if got := b.step(context.Background()); got != leaseAcquired || b.epoch != 2 {
		t.Fatalf("b: %v epoch %d, want takeover at epoch 2", got, b.epoch)
	}

	if b.takeovers.Load() != 1 || b.token.Load() != 2 {
		t.Fatalf("takeovers=%d token=%d, want 1 and 2", b.takeovers.Load(), b.token.Load())
	}

	if got := a.step(context.Background()); got != leaseLost || a.held || a.isLeader.Load() != 0 {
		t.Fatalf("a: %v held=%v, want lost: its epoch 1 is stale", got, a.held)
	}
}

func TestLeaseExpiredCannotBeRenewedEvenWithoutAReplacement(t *testing.T) {
	t.Parallel()

	c := &leaseClock{t: time.Unix(1000, 0)}
	f := &fakeLease{clock: c}
	a := newTestElector(f, c)
	a.step(context.Background())

	c.advance(testLeaseCfg.ttl + time.Second)

	if got := a.step(context.Background()); got != leaseLost {
		t.Fatalf("got %v, want lost: an expired lease needs a new epoch", got)
	}

	if got := a.step(context.Background()); got != leaseAcquired || a.epoch != 2 {
		t.Fatalf("got %v epoch %d, want a fresh acquire at epoch 2", got, a.epoch)
	}
}

func TestLeaseRenewErrorIsToleratedUntilTheDeadline(t *testing.T) {
	t.Parallel()

	c := &leaseClock{t: time.Unix(1000, 0)}
	f := &fakeLease{clock: c}
	a := newTestElector(f, c)
	a.step(context.Background())

	f.down = true

	c.advance(testLeaseCfg.renew)

	if got := a.step(context.Background()); got != leaseNoChange || !a.held {
		t.Fatalf("one failed renew: %v held=%v, want it tolerated", got, a.held)
	}

	c.advance(testLeaseCfg.renew)

	if got := a.step(context.Background()); got != leaseLost || a.held {
		t.Fatalf("after the renew deadline: %v held=%v, want lost", got, a.held)
	}
}

func TestLeaseAcquireErrorLeavesAFollower(t *testing.T) {
	t.Parallel()

	c := &leaseClock{t: time.Unix(1000, 0)}
	f := &fakeLease{clock: c, down: true}
	a := newTestElector(f, c)

	if got := a.step(context.Background()); got != leaseNoChange || a.held {
		t.Fatalf("got %v held=%v, want a follower while the database is down", got, a.held)
	}
}

func TestLeaseRunStartsWorkOnlyWhileHeldAndStopsItOnLoss(t *testing.T) {
	t.Parallel()

	c := &leaseClock{t: time.Unix(1000, 0)}
	f := &fakeLease{clock: c}
	cfg := leaseConfig{ttl: 15 * time.Second, renew: 5 * time.Millisecond}
	e := newElector(sourceBMKG, cfg, f)
	e.now = c.now

	started := make(chan int64, 4)
	stopped := make(chan struct{}, 4)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		e.run(ctx, func(wctx context.Context) {
			ep, _ := leaseEpoch(wctx)
			started <- ep

			<-wctx.Done()

			stopped <- struct{}{}
		})
	}()

	if ep := leaseWait(t, started); ep != 1 {
		t.Fatalf("work epoch %d, want 1", ep)
	}

	f.mu.Lock()
	f.holder = "someone-else" // the lease moved away, as after a takeover
	f.mu.Unlock()

	leaseWait(t, stopped)

	// The other holder dies, the work comes back with a new epoch.
	f.mu.Lock()
	f.holder = ""
	f.mu.Unlock()

	if ep := leaseWait(t, started); ep != 2 {
		t.Fatalf("work epoch %d, want 2 after the takeover", ep)
	}

	cancel()
	<-done

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.holder != "" {
		t.Fatalf("holder %q after shutdown, want the lease released", f.holder)
	}
}

func leaseWait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")

		panic("unreachable")
	}
}

func TestPipelineWithALeaseDoesNotPollWhileAnotherInstanceHoldsIt(t *testing.T) {
	t.Parallel()

	c := &leaseClock{t: time.Unix(1000, 0)}
	f := &fakeLease{clock: c, holder: "other", epoch: 7, expires: c.now().Add(time.Hour)}
	e := newElector(sourceBMKG, leaseConfig{ttl: 15 * time.Second, renew: 5 * time.Millisecond}, f)
	e.now = c.now

	var (
		mu    sync.Mutex
		calls int
	)

	out := make(chan pollResult, 1)
	p := pipeline{
		poller: &poller{name: sourceBMKG, interval: time.Millisecond, breaker: testBreaker(), out: out, fetch: func(context.Context, time.Time) (pollResult, error) {
			mu.Lock()
			defer mu.Unlock()

			calls++

			return pollResult{}, nil
		}},
		ingest: &ingest{name: sourceBMKG, in: out},
		lease:  e,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	p.run(ctx)

	mu.Lock()
	defer mu.Unlock()

	if calls != 0 {
		t.Fatalf("%d fetches while another holder had the lease, want 0", calls)
	}
}

func TestPollerStartsFromTheStoredCursor(t *testing.T) {
	t.Parallel()

	stored := time.Date(2026, 10, 4, 10, 0, 0, 0, time.UTC)

	var got time.Time

	p := &poller{name: sourcePVMBG, breaker: testBreaker(), out: make(chan pollResult, 1),
		loadSince: func(context.Context) (time.Time, error) { return stored, nil },
		fetch: func(_ context.Context, since time.Time) (pollResult, error) {
			got = since

			return pollResult{}, nil
		}}

	p.since = stored.Add(time.Hour) // left over from an earlier term

	ctx, cancel := context.WithCancel(context.Background())
	p.fetch = func(_ context.Context, since time.Time) (pollResult, error) {
		got = since

		cancel()

		return pollResult{}, nil
	}

	p.loop(ctx)

	if !got.Equal(stored) {
		t.Fatalf("since = %v, want the stored cursor %v", got, stored)
	}
}

func TestLoadLeaseConfig(t *testing.T) {
	cases := []struct {
		name, ttl, renew string
		want             leaseConfig
		wantErr          bool
	}{
		{name: "defaults", want: leaseConfig{ttl: 15 * time.Second, renew: 3 * time.Second}},
		{name: "custom", ttl: "30s", renew: "5s", want: leaseConfig{ttl: 30 * time.Second, renew: 5 * time.Second}},
		{name: "not a duration", ttl: "abc", wantErr: true},
		{name: "negative", renew: "-3s", wantErr: true},
		{name: "zero", ttl: "0s", wantErr: true},
		{name: "renew not above db timeout", renew: "2s", wantErr: true},
		{name: "ttl not above renew deadline", ttl: "6s", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("LEASE_TTL", tc.ttl)
			t.Setenv("LEASE_RENEW_INTERVAL", tc.renew)

			got, err := loadLeaseConfig()
			if (err != nil) != tc.wantErr || (err == nil && got != tc.want) {
				t.Fatalf("got %+v, %v; want %+v, error=%v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestLeaseMetricsHandler(t *testing.T) {
	t.Parallel()

	c := &leaseClock{t: time.Unix(1000, 0)}
	e := newTestElector(&fakeLease{clock: c}, c)
	e.step(context.Background())

	rec := httptest.NewRecorder()
	leaseMetricsHandler([]*elector{e})(rec, httptest.NewRequest("GET", "/metrics", nil))

	body, _ := io.ReadAll(rec.Body)

	for _, want := range []string{
		`lease_is_leader{source="BMKG"} 1`,
		fmt.Sprintf(`lease_token{source="BMKG"} %d`, 1),
		`lease_takeovers_total{source="BMKG"} 0`,
	} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

// Real Postgres: two holders race for the same lease row and the epoch only grows.
func TestPgLeaseAcquireRenewReleaseAgainstPostgres(t *testing.T) { //nolint:paralleltest // shares the lease row with other runs
	s := testStore(t, 4)
	ctx := context.Background()
	l := pgLease{db: s.db}
	src := sourcePVMBG
	a, b := newHazardID(), newHazardID()

	t.Cleanup(func() {
		_, _ = s.db.ExecContext(ctx, `UPDATE source_lease SET holder = NULL, expires_at = NULL WHERE source = $1`, src)
	})

	if _, err := s.db.ExecContext(ctx, `UPDATE source_lease SET holder = NULL, expires_at = NULL WHERE source = $1`, src); err != nil {
		t.Fatal(err)
	}

	epochA, ok, err := l.acquire(ctx, src, a, time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire a: ok=%v err=%v", ok, err)
	}

	if _, ok, err := l.acquire(ctx, src, b, time.Minute); err != nil || ok {
		t.Fatalf("acquire b while a holds it: ok=%v err=%v", ok, err)
	}

	if _, ok, _ := l.acquire(ctx, src, a, time.Minute); ok {
		t.Fatal("the same holder must not acquire again while valid")
	}

	if ok, err := l.renew(ctx, src, a, epochA, time.Minute); err != nil || !ok {
		t.Fatalf("renew a: ok=%v err=%v", ok, err)
	}

	if ok, _ := l.renew(ctx, src, b, epochA, time.Minute); ok {
		t.Fatal("b renewed a's lease")
	}

	if err := l.release(ctx, src, a, epochA); err != nil {
		t.Fatal(err)
	}

	epochB, ok, err := l.acquire(ctx, src, b, 100*time.Millisecond)
	if err != nil || !ok || epochB != epochA+1 {
		t.Fatalf("acquire b after release: epoch %d (a had %d) ok=%v err=%v", epochB, epochA, ok, err)
	}

	time.Sleep(200 * time.Millisecond)

	if ok, _ := l.renew(ctx, src, b, epochB, time.Minute); ok {
		t.Fatal("an expired lease must not be renewed")
	}

	if epochC, ok, _ := l.acquire(ctx, src, a, time.Minute); !ok || epochC != epochB+1 {
		t.Fatalf("takeover after expiry: epoch %d ok=%v, want %d", epochC, ok, epochB+1)
	}
}

func TestPgLeaseConcurrentAcquireHasOneWinner(t *testing.T) { //nolint:paralleltest // shares the lease row with other runs
	s := testStore(t, 16)
	ctx := context.Background()
	l := pgLease{db: s.db}
	src := sourceBMKG
	reset := func() {
		_, _ = s.db.ExecContext(ctx, `UPDATE source_lease SET holder = NULL, expires_at = NULL WHERE source = $1`, src)
	}

	reset()
	t.Cleanup(reset)

	const rounds, contenders = 20, 12

	for round := range rounds {
		reset()

		var (
			wg      sync.WaitGroup
			winners atomic.Int32
			start   = make(chan struct{})
		)

		for range contenders {
			wg.Add(1)

			go func() {
				defer wg.Done()

				<-start

				if _, ok, err := l.acquire(ctx, src, newHazardID(), time.Minute); err == nil && ok {
					winners.Add(1)
				}
			}()
		}

		close(start)
		wg.Wait()

		if got := winners.Load(); got != 1 {
			t.Fatalf("round %d: %d winners, want exactly 1", round, got)
		}
	}
}
