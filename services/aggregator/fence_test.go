package main

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fencedCtx(epoch int64) context.Context {
	return withLeaseEpoch(context.Background(), epoch)
}

// fenceRows reads the highest token the store holds for a source, 0 if none.
func fenceRows(t *testing.T, s *store, source string) int64 {
	t.Helper()

	var n int64

	err := s.db.QueryRowContext(context.Background(), `SELECT coalesce(max(max_token), 0) FROM write_fence WHERE source = $1`, source).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}

	return n
}

func resetFence(t *testing.T, s *store, source string) {
	t.Helper()

	reset := func() {
		_, _ = s.db.ExecContext(context.Background(), `DELETE FROM write_fence WHERE source = $1`, source)
	}

	reset()
	t.Cleanup(reset)
}

func TestStoreFenceLowerTokenIsRejectedEqualAndHigherAreAccepted(t *testing.T) { //nolint:paralleltest // shares the fence row of a source
	s := testStore(t, 4)
	resetFence(t, s, sourcePVMBG)

	write := func(epoch int64, refID string) error {
		_, err := s.upsert(fencedCtx(epoch), hazard(sourcePVMBG, refID))

		return err
	}

	a, b, c, d := ref(t, s), ref(t, s), ref(t, s), ref(t, s)

	if err := write(5, a); err != nil {
		t.Fatalf("token 5: %v", err)
	}

	if err := write(4, b); !errors.Is(err, errFenced) {
		t.Fatalf("token 4 after 5: err=%v, want fenced", err)
	}

	if count(t, s, b) != 0 {
		t.Fatal("a fenced write left a row")
	}

	if err := write(5, c); err != nil {
		t.Fatalf("token 5 again: %v", err)
	}

	if err := write(6, d); err != nil {
		t.Fatalf("token 6: %v", err)
	}

	if got := fenceRows(t, s, sourcePVMBG); got != 6 {
		t.Fatalf("highest token %d, want 6", got)
	}

	if err := write(5, ref(t, s)); !errors.Is(err, errFenced) {
		t.Fatalf("token 5 after 6: err=%v, want fenced", err)
	}
}

func TestStoreFenceReplayChangesNothingAndNoTokenIsUnfenced(t *testing.T) { //nolint:paralleltest // shares the fence row of a source
	s := testStore(t, 4)
	resetFence(t, s, sourcePVMBG)

	id := ref(t, s)
	h := hazard(sourcePVMBG, id)

	if changed, err := s.upsert(fencedCtx(5), h); err != nil || !changed {
		t.Fatalf("first write changed=%v err=%v", changed, err)
	}

	if changed, err := s.upsert(fencedCtx(5), h); err != nil || changed {
		t.Fatalf("replay changed=%v err=%v, want unchanged", changed, err)
	}

	if count(t, s, id) != 1 || fenceRows(t, s, sourcePVMBG) != 5 {
		t.Fatal("replay must keep one row and token 5")
	}

	// No token: accepted even after a higher one was recorded, and it records nothing.
	other := ref(t, s)
	if _, err := s.upsert(context.Background(), hazard(sourcePVMBG, other)); err != nil || count(t, s, other) != 1 {
		t.Fatalf("tokenless write err=%v", err)
	}

	if fenceRows(t, s, sourcePVMBG) != 5 {
		t.Fatal("a tokenless write must not move the token")
	}
}

func TestStoreFenceTokensAreKeptPerSource(t *testing.T) { //nolint:paralleltest // shares the fence rows of two sources
	s := testStore(t, 4)
	resetFence(t, s, sourcePVMBG)
	resetFence(t, s, sourceBMKG)

	if _, err := s.upsert(fencedCtx(9), hazard(sourcePVMBG, ref(t, s))); err != nil {
		t.Fatal(err)
	}

	if _, err := s.upsert(fencedCtx(2), hazard(sourceBMKG, ref(t, s))); err != nil {
		t.Fatalf("BMKG token 2 must not be fenced by PVMBG token 9: %v", err)
	}
}

// Writers with tokens 1 to 8 race. Once a write at the highest token returned, no lower token may commit.
func TestStoreFenceConcurrentWritersLowerNeverWinsAfterHigher(t *testing.T) { //nolint:paralleltest // shares the fence row of a source
	s := testStore(t, 16)
	resetFence(t, s, sourcePVMBG)

	var (
		wg        sync.WaitGroup
		highDone  atomic.Bool
		lateLower atomic.Int64
	)

	for range 20 {
		for tok := int64(1); tok <= 8; tok++ {
			wg.Go(func() {
				after := highDone.Load()
				_, err := s.upsert(fencedCtx(tok), hazard(sourcePVMBG, ref(t, s)))

				if tok == 8 && err == nil {
					highDone.Store(true)
				}

				if after && tok < 8 && err == nil {
					lateLower.Add(1)
				}
			})
		}

	}

	wg.Wait()

	if got := fenceRows(t, s, sourcePVMBG); got != 8 {
		t.Fatalf("highest token %d, want 8", got)
	}

	if n := lateLower.Load(); n != 0 {
		t.Fatalf("%d lower-token writes committed after the highest token had written", n)
	}

	// Sequential check after the race: every lower token is now refused.
	for tok := int64(1); tok < 8; tok++ {
		if _, err := s.upsert(fencedCtx(tok), hazard(sourcePVMBG, ref(t, s))); !errors.Is(err, errFenced) {
			t.Fatalf("token %d after 8: err=%v, want fenced", tok, err)
		}
	}
}

func TestClusterCarriesTheTokenToBothOwners(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.cluster("a", "b", "c")

	var (
		mu   sync.Mutex
		seen = map[string]int64{}
	)

	inner := c.write
	c.write = func(ctx context.Context, n *node, h HazardEvent) (bool, error) {
		e, _ := leaseEpoch(ctx)

		mu.Lock()
		seen[n.name] = e
		mu.Unlock()

		return inner(ctx, n, h)
	}

	if _, err := c.upsert(fencedCtx(7), hz("E1")); err != nil {
		t.Fatal(err)
	}

	if len(seen) != 2 {
		t.Fatalf("%d owners written, want 2", len(seen))
	}

	for n, e := range seen {
		if e != 7 {
			t.Fatalf("owner %s got token %d, want 7", n, e)
		}
	}
}

func TestClusterFencedByAnOwnerReturnsFencedEvenWhenTheOtherCommitted(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.cluster("a", "b", "c")
	h := hz("E1")
	stale := owners(shardOf(h.HazardID), c.live())[0].node.name

	inner := c.write
	c.write = func(ctx context.Context, n *node, h HazardEvent) (bool, error) {
		if n.name == stale {
			return false, &fencedError{source: h.Source, token: 3}
		}

		return inner(ctx, n, h)
	}

	if _, err := c.upsert(fencedCtx(3), h); !errors.Is(err, errFenced) {
		t.Fatalf("err=%v, want fenced", err)
	}

	if len(c.dirtyShards()) != 1 {
		t.Fatal("the shard with a missed copy must be marked dirty")
	}
}

func TestIngestPassesTheBatchEpochToTheWriteAndStopsOnFenced(t *testing.T) {
	t.Parallel()

	var (
		mu     sync.Mutex
		epochs []int64
		fenced []int64
		pub    int
	)

	in, ch := volcanicIngest(func(ctx context.Context, _ HazardEvent) (bool, error) {
		e, _ := leaseEpoch(ctx)

		mu.Lock()
		epochs = append(epochs, e)
		mu.Unlock()

		return false, &fencedError{source: sourcePVMBG, token: e}
	}, func(context.Context, HazardEvent, string) { pub++ })
	in.onFenced = func(e int64) { fenced = append(fenced, e) }

	b := reports("R1", "R2", "R3")
	b.epoch = 4
	ch <- b

	close(ch)
	in.run(context.Background())

	if len(epochs) != 1 || epochs[0] != 4 {
		t.Fatalf("writes with epochs %v, want one write with 4 (no retry, batch dropped)", epochs)
	}

	if len(fenced) != 1 || fenced[0] != 4 || pub != 0 {
		t.Fatalf("fenced %v published %d", fenced, pub)
	}
}

func TestIngestWithoutAnEpochWritesUnfenced(t *testing.T) {
	t.Parallel()

	var got atomic.Bool

	in, ch := volcanicIngest(func(ctx context.Context, _ HazardEvent) (bool, error) {
		_, ok := leaseEpoch(ctx)
		got.Store(ok)

		return true, nil
	}, func(context.Context, HazardEvent, string) {})

	ch <- reports("R1")

	close(ch)
	in.run(context.Background())

	if got.Load() {
		t.Fatal("a batch without a lease must write without a token")
	}
}

func TestPollOnceStampsTheBatchWithTheLeaseEpoch(t *testing.T) {
	t.Parallel()

	out := make(chan pollResult, 1)
	p := &poller{name: sourceBMKG, interval: time.Millisecond, breaker: testBreaker(), out: out,
		fetch: func(context.Context, time.Time) (pollResult, error) {
			return pollResult{records: []map[string]any{{"x": 1}}}, nil
		}}

	p.pollOnce(fencedCtx(11))

	if r := <-out; r.epoch != 11 {
		t.Fatalf("batch epoch %d, want 11", r.epoch)
	}
}

func TestElectorDropsTheLeaseWhenFencedAndAcquiresANewEpoch(t *testing.T) {
	t.Parallel()

	c := &leaseClock{t: time.Unix(1000, 0)}
	f := &fakeLease{clock: c}
	e := newTestElector(f, c)

	if e.step(context.Background()) != leaseAcquired || e.epoch != 1 {
		t.Fatal("setup: want epoch 1 acquired")
	}

	// A rejection of an older term changes nothing.
	e.fence(0)

	if e.step(context.Background()) != leaseNoChange {
		t.Fatal("a stale rejection must not drop a newer term")
	}

	e.fence(1)

	if e.step(context.Background()) != leaseLost || e.isLeader.Load() != 0 {
		t.Fatal("a rejection of the current epoch must drop leadership")
	}

	if e.step(context.Background()) != leaseAcquired || e.epoch != 2 {
		t.Fatalf("want a fresh acquire with epoch 2, got epoch %d", e.epoch)
	}

	if e.fenced.Load() != 2 {
		t.Fatalf("fenced count %d, want 2", e.fenced.Load())
	}
}

func TestElectorRunWakesOnFencedAndRestartsWorkWithANewEpoch(t *testing.T) {
	t.Parallel()

	c := &leaseClock{t: time.Unix(1000, 0)}
	e := newTestElector(&fakeLease{clock: c}, c)
	e.cfg.renew = 5 * time.Millisecond

	epochs := make(chan int64, 4)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		defer close(done)

		e.run(ctx, func(wctx context.Context) {
			ep, _ := leaseEpoch(wctx)
			epochs <- ep

			<-wctx.Done()
		})
	}()

	if got := leaseWait(t, epochs); got != 1 {
		t.Fatalf("first epoch %d, want 1", got)
	}

	e.fence(1)

	if got := leaseWait(t, epochs); got != 2 {
		t.Fatalf("epoch after fence %d, want 2", got)
	}

	cancel()
	<-done
}

func TestFencedWritesMetric(t *testing.T) {
	t.Parallel()

	c := &leaseClock{t: time.Unix(1000, 0)}
	e := newTestElector(&fakeLease{clock: c}, c)
	e.fence(1)

	rec := httptest.NewRecorder()
	leaseMetricsHandler([]*elector{e})(rec, httptest.NewRequest("GET", "/metrics", nil))

	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), `fenced_writes_total{source="BMKG"} 1`) {
		t.Fatalf("metrics missing fenced_writes_total:\n%s", body)
	}
}
