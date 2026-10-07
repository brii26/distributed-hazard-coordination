package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

// clock palsu, waktu digeser manual.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

func clockedBreaker(threshold int, openFor time.Duration) (*breaker, *fakeClock) {
	clock := &fakeClock{t: time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)}
	b := newBreaker(sourcePVMBG, threshold, openFor)
	b.now = clock.now

	return b, clock
}

func statusOf(b *breaker) string {
	return b.snapshot().Status
}

func TestBreakerOpensAfterThresholdAndMarksStale(t *testing.T) {
	t.Parallel()

	b, clock := clockedBreaker(3, 10*time.Second)

	if statusOf(b) != statusUp || !b.allow() {
		t.Fatal("a fresh breaker should be UP and allow calls")
	}

	b.success()

	good := clock.now()

	clock.advance(time.Second)
	b.failure()
	b.failure()

	if got := statusOf(b); got != statusDegraded || !b.allow() {
		t.Fatalf("two failures out of three: status %s, want DEGRADED and still polling", got)
	}

	b.failure()

	s := b.snapshot()
	if s.Status != statusDown || s.ConsecutiveFail != 3 || b.allow() {
		t.Fatalf("threshold reached: status %s fails %d, want DOWN and no polling", s.Status, s.ConsecutiveFail)
	}

	if s.StaleSince == nil || !s.StaleSince.Equal(good) || s.LastSuccessAt == nil || !s.LastSuccessAt.Equal(good) {
		t.Fatalf("stale_since should be the last good poll %v, got %+v", good, s)
	}
}

func TestBreakerHalfOpenTrialRecovers(t *testing.T) {
	t.Parallel()

	b, clock := clockedBreaker(2, 10*time.Second)

	b.failure()
	b.failure()

	clock.advance(9 * time.Second)

	if b.allow() {
		t.Fatal("must stay closed to callers until the open duration has passed")
	}

	clock.advance(time.Second)

	if !b.allow() {
		t.Fatal("after the open duration one trial call must be allowed")
	}

	if b.allow() {
		t.Fatal("only one trial may be in flight")
	}

	b.success()

	s := b.snapshot()
	if s.Status != statusUp || s.ConsecutiveFail != 0 || s.StaleSince != nil || !b.allow() {
		t.Fatalf("a good trial should close the breaker and clear stale_since: %+v", s)
	}
}

func TestBreakerFailedTrialReopensForAFullDuration(t *testing.T) {
	t.Parallel()

	b, clock := clockedBreaker(2, 10*time.Second)

	b.failure()
	b.failure()
	clock.advance(10 * time.Second)

	if !b.allow() {
		t.Fatal("trial expected")
	}

	b.failure()

	if b.allow() || statusOf(b) != statusDown {
		t.Fatal("a failed trial must reopen the breaker")
	}

	clock.advance(9 * time.Second)

	if b.allow() {
		t.Fatal("the wait restarts from the failed trial, not from the first opening")
	}

	clock.advance(time.Second)

	if !b.allow() {
		t.Fatal("a new trial should be allowed after another full duration")
	}
}

func TestBreakerSuccessResetsFailureCount(t *testing.T) {
	t.Parallel()

	b, _ := clockedBreaker(3, time.Second)

	b.failure()
	b.failure()
	b.success()
	b.failure()
	b.failure()

	if got := statusOf(b); got != statusDegraded {
		t.Fatalf("failures must be consecutive to open the breaker, status %s", got)
	}
}

func TestBreakerNeverSucceededStillGetsStaleSince(t *testing.T) {
	t.Parallel()

	b, clock := clockedBreaker(1, time.Second)
	b.failure()

	s := b.snapshot()
	if s.LastSuccessAt != nil || s.StaleSince == nil || !s.StaleSince.Equal(clock.now()) {
		t.Fatalf("no good poll ever: last_success_at must be null and stale_since the moment it went down: %+v", s)
	}
}

func TestSourceStatusJSONShape(t *testing.T) {
	t.Parallel()

	b, _ := clockedBreaker(3, time.Second)

	out, err := json.Marshal(b.snapshot())
	if err != nil {
		t.Fatal(err)
	}

	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}

	if m["source"] != sourcePVMBG || m["source_status"] != statusUp || m["stale_since"] != nil || m["last_success_at"] != nil || len(m) != 4 {
		t.Fatalf("unexpected meta shape: %s", out)
	}
}

// poller dan breaker jalan bareng: gagal berulang stop call, trial abis open duration recover, breaker source lain nggak tersentuh.
func TestPollerStopsCallingAnOpenSourceAndRecovers(t *testing.T) {
	t.Parallel()

	b, clock := clockedBreaker(3, 10*time.Second)
	healthy := testBreaker()

	var (
		calls int
		fail  = true
	)

	p := &poller{name: sourcePVMBG, breaker: b, fetch: func(context.Context, time.Time) (pollResult, error) {
		calls++

		if fail {
			return pollResult{}, errors.New("down")
		}

		return pollResult{}, nil
	}}
	other := &poller{name: sourceBMKG, breaker: healthy, fetch: func(context.Context, time.Time) (pollResult, error) { return pollResult{}, nil }}

	for range 10 {
		p.pollOnce(context.Background())
		other.pollOnce(context.Background())
	}

	if calls != 3 {
		t.Fatalf("source called %d times, want 3 (polling stops once the breaker opens)", calls)
	}

	if statusOf(b) != statusDown || statusOf(healthy) != statusUp {
		t.Fatalf("PVMBG %s, BMKG %s: only the failing source may go down", statusOf(b), statusOf(healthy))
	}

	fail = false

	clock.advance(10 * time.Second)
	p.pollOnce(context.Background())

	if calls != 4 || statusOf(b) != statusUp {
		t.Fatalf("recovery: calls %d status %s, want one trial that closes the breaker", calls, statusOf(b))
	}
}

func TestTokenFailureCountsAsFailedPoll(t *testing.T) {
	t.Parallel()

	var requests int

	c := &sourceClient{baseURL: "http://unused.invalid", timeout: time.Second, auth: func(*http.Request) error { requests++; return nil }}
	b, _ := clockedBreaker(2, time.Second)
	p := &poller{name: sourcePVMBG, breaker: b, fetch: pvmbgFetch(c, func() error { return errors.New("refresh token rejected") })}

	p.pollOnce(context.Background())
	p.pollOnce(context.Background())

	if requests != 0 || statusOf(b) != statusDown {
		t.Fatalf("a failing token must fail the poll without calling PVMBG: requests=%d status=%s", requests, statusOf(b))
	}
}

func TestBreakerSafeUnderConcurrentUse(t *testing.T) {
	t.Parallel()

	b := newBreaker(sourcePVMBG, 3, time.Millisecond)

	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range 2000 {
			if b.allow() {
				if i%3 == 0 {
					b.success()
				} else {
					b.failure()
				}
			}
		}
	})

	for range 4 {
		wg.Go(func() {
			for range 2000 {
				_ = b.snapshot()
			}
		})
	}

	wg.Wait()
}
