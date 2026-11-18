package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestMigration010RunsTwice(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)

	ddl, err := os.ReadFile("migrations/010_failed_retry.sql")
	if err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if _, err := s.db.ExecContext(context.Background(), string(ddl)); err != nil {
			t.Fatal(err)
		}
	}
}

func retryRow(i int, status string, next time.Time) failedMessage {
	m := newFailedMessage(sourcePVMBG, fmt.Appendf(nil, `{"report_id":"c%d"}`, i), "timeout: x", status == failedRetrying, time.Now())
	m.Status, m.NextAttemptAt, m.MappedEvent = status, next, []byte(`{"source_ref_id":"x"}`)

	return m
}

func TestRetryRowKeepsRawPayloadAndMappedEventAndInsertsOnce(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()
	m := retryRow(1, failedRetrying, time.Now().Add(time.Minute))
	m.CorrelationID = "corr-1"

	for range 3 {
		if err := s.insertFailed(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	rows, _, err := s.listFailed(ctx, only(failedRetrying), "", 10)
	if err != nil || len(rows) != 1 || rows[0].Attempts != 1 || string(rows[0].Payload) != `{"report_id":"c1"}` {
		t.Fatalf("one row with the raw bytes: %+v %v", rows, err)
	}

	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT mapped_event::text || '|' || correlation_id FROM failed_messages`).Scan(&raw); err != nil || raw != `{"source_ref_id": "x"}|corr-1` {
		t.Fatalf("mapped_event/correlation_id: %q %v", raw, err)
	}
}

func TestClaimTakesEveryRowOnceUnderConcurrencyAndHidesClaimedRows(t *testing.T) { //nolint:paralleltest,gocyclo // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()

	for i := range 200 {
		if err := s.insertFailed(ctx, retryRow(i, failedRetrying, time.Now().Add(-time.Second))); err != nil {
			t.Fatal(err)
		}
	}

	// a parked row and a not yet due row must never be taken
	_ = s.insertFailed(ctx, retryRow(900, failedParked, time.Time{}))
	_ = s.insertFailed(ctx, retryRow(901, failedRetrying, time.Now().Add(time.Hour)))

	var (
		mu   sync.Mutex
		seen = map[string]int{}
		wg   sync.WaitGroup
	)

	for range 20 {
		wg.Go(func() {
			for {
				rows, err := s.claimDue(ctx, 7, 60*time.Second)
				if err != nil {
					t.Error(err)

					return
				}

				if len(rows) == 0 {
					return
				}

				mu.Lock()
				for _, r := range rows {
					seen[r.MessageID]++
				}
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	if len(seen) != 200 {
		t.Fatalf("claimed %d distinct rows, want 200", len(seen))
	}

	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s claimed %d times", id, n)
		}
	}

	if rows, _ := s.claimDue(ctx, 10, time.Second); len(rows) != 0 {
		t.Fatal("claimed rows are invisible for the lease")
	}

	// time is set in the row, no sleeping: the lease of one row ran out
	if _, err := s.db.ExecContext(ctx, `UPDATE failed_messages SET next_attempt_at = now() - interval '1 second' WHERE message_id = (SELECT message_id FROM failed_messages WHERE status = 'retrying' ORDER BY message_id LIMIT 1)`); err != nil {
		t.Fatal(err)
	}

	if rows, _ := s.claimDue(ctx, 10, time.Minute); len(rows) != 1 {
		t.Fatalf("a row whose lease ran out must be due again, got %d", len(rows))
	}
}

func TestRetryFailureAndCountsQuery(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()
	a, b := retryRow(1, failedRetrying, time.Now()), retryRow(2, failedRetrying, time.Now())
	c := newFailedMessage(sourcePVMBG, []byte("x"), "schema_incompatible: bad", false, time.Now())

	for _, m := range []failedMessage{a, b, c} {
		if err := s.insertFailed(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.recordRetryFailure(ctx, a.MessageID, true, "timeout: again", time.Minute); err != nil {
		t.Fatal(err)
	}

	if err := s.recordRetryFailure(ctx, b.MessageID, false, "invalid_value: x", 0); err != nil {
		t.Fatal(err)
	}

	got, err := s.failedCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]int64{"retrying/timeout": 1, "parked/invalid_value": 1, "parked/schema_incompatible": 1}
	for _, g := range got {
		if want[g.status+"/"+g.reason] != g.n {
			t.Fatalf("unexpected count %+v in %+v", g, got)
		}

		delete(want, g.status+"/"+g.reason)
	}

	if len(want) != 0 {
		t.Fatalf("missing %v", want)
	}

	var attempts int
	if err := s.db.QueryRowContext(ctx, `SELECT attempts FROM failed_messages WHERE message_id = $1`, a.MessageID).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("attempts %d %v", attempts, err)
	}
}

func TestGuardedUpsertDoesNotOverwriteANewerWrite(t *testing.T) { //nolint:paralleltest // shares the hazard table
	s := testStore(t, 25)
	ctx := context.Background()
	id := ref(t, s)
	failedAt := time.Now().UTC().Truncate(time.Microsecond)

	newer := hazard(sourcePVMBG, id)
	newer.IngestedAt = failedAt.Add(time.Second)
	newer.AreaName = new("newer")

	if _, err := s.upsert(ctx, newer); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	errs := make([]error, 20)

	for i := range errs {
		wg.Go(func() {
			old := hazard(sourcePVMBG, id)
			old.IngestedAt = failedAt.Add(-time.Second)
			old.AreaName = new(fmt.Sprint("old", i))
			_, errs[i] = s.upsert(withNotNewerThan(ctx, failedAt), old)
		})
	}

	wg.Wait()

	for _, err := range errs {
		if !errors.Is(err, errSuperseded) {
			t.Fatalf("every stale write must be superseded, got %v", err)
		}
	}

	if got := storedArea(t, s, id); got != "newer" {
		t.Fatalf("stored area %q, the newer write must survive", got)
	}

	// the live path is unchanged: without the guard the old record still overwrites (#303 owns that rule)
	live := hazard(sourcePVMBG, id)
	live.AreaName = new("live")

	if changed, err := s.upsert(ctx, live); err != nil || !changed || storedArea(t, s, id) != "live" {
		t.Fatalf("live upsert: %v %v", changed, err)
	}
}

func TestGuardedUpsertWritesWhenNothingNewerExists(t *testing.T) { //nolint:paralleltest // shares the hazard table
	s := testStore(t, 25)
	ctx := context.Background()
	id := ref(t, s)
	failedAt := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond) // the row below is written before the failure
	h := hazard(sourcePVMBG, id)
	h.IngestedAt = failedAt.Add(-time.Second)

	g := withNotNewerThan(ctx, failedAt)
	if changed, err := s.upsert(g, h); err != nil || !changed {
		t.Fatalf("new row: %v %v", changed, err)
	}

	if changed, err := s.upsert(g, h); err != nil || changed {
		t.Fatalf("a repeat is a no-op and not superseded: %v %v", changed, err)
	}

	h.AreaName = new("fixed")
	if changed, err := s.upsert(g, h); err != nil || !changed || storedArea(t, s, id) != "fixed" {
		t.Fatalf("an older row is updated by its retry: %v %v", changed, err)
	}
}

// An UPDATE keeps ingested_at, so only updated_at shows that the row changed after the failure.
func TestGuardedUpsertSeesAnUpdateOfAnExistingRow(t *testing.T) { //nolint:paralleltest // shares the hazard table
	s := testStore(t, 25)
	ctx := context.Background()
	id := ref(t, s)

	first := hazard(sourcePVMBG, id)
	first.AreaName = new("v1")

	if _, err := s.upsert(ctx, first); err != nil { // row exists before the failure
		t.Fatal(err)
	}

	time.Sleep(20 * time.Millisecond)

	failedAt := time.Now().UTC().Truncate(time.Microsecond) // T: the record below failed to write

	time.Sleep(20 * time.Millisecond)

	update := first // T+1: a live update of the same row, same ingested_at
	update.AreaName = new("v2")

	if changed, err := s.upsert(ctx, update); err != nil || !changed {
		t.Fatalf("live update: %v %v", changed, err)
	}

	stale := first
	stale.AreaName = new("stale")

	if _, err := s.upsert(withNotNewerThan(ctx, failedAt), stale); !errors.Is(err, errSuperseded) {
		t.Fatalf("the retry must be superseded, got %v", err)
	}

	if got := storedArea(t, s, id); got != "v2" {
		t.Fatalf("stored area %q, the update must survive", got)
	}
}

func storedArea(t *testing.T, s *store, id string) string {
	t.Helper()

	var area string
	if err := s.db.QueryRowContext(context.Background(), `SELECT area_name FROM hazard_events WHERE source_ref_id = $1`, id).Scan(&area); err != nil {
		t.Fatal(err)
	}

	return area
}

func TestEndToEndStoreOutageThenRecovery(t *testing.T) { //nolint:paralleltest // renames hazard_events for a moment
	s := testFailedStore(t)
	ctx := context.Background()

	const n = 25

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("e2e-%d-%d", time.Now().UnixNano(), i)
	}

	t.Cleanup(func() {
		_, _ = s.db.ExecContext(ctx, `DELETE FROM hazard_events WHERE source_ref_id = ANY($1)`, ids)
	})

	var rec recorder

	in, ch := volcanicIngest(s.upsert, rec.publish)
	mp := newMapper() // real uuid ids, the test mapper's ids are not uuids
	in.mapBatch = func(r pollResult) ([]HazardEvent, error) { return mp.mapVolcanic(r.records) }
	in.fail, in.retry = s.insertFailed, retryConfig{base: time.Millisecond, cap: time.Millisecond, poll: time.Millisecond, maxAttempts: 8}

	ch <- reports(ids...)

	close(ch)

	if _, err := s.db.ExecContext(ctx, `ALTER TABLE hazard_events RENAME TO hazard_events_off`); err != nil {
		t.Fatal(err)
	}

	restored := false
	restore := func() {
		if !restored {
			restored = true
			_, _ = s.db.ExecContext(ctx, `ALTER TABLE hazard_events_off RENAME TO hazard_events`)
		}
	}

	t.Cleanup(restore)

	in.run(ctx)
	restore()

	var kept int

	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM failed_messages WHERE status = 'retrying' AND attempts = 1`).Scan(&kept)

	if kept != n {
		t.Fatalf("%d records kept, want %d", kept, n)
	}

	time.Sleep(5 * time.Millisecond) // next_attempt_at is now + about 1 ms

	w := &retryWorker{q: s, upsert: s.upsert, publish: rec.publish, cfg: in.retry}

	for n := 1; n > 0; {
		n = w.tick(ctx)
	}

	var events, recovered int

	_ = s.db.QueryRowContext(ctx, `SELECT count(DISTINCT source_ref_id) FROM hazard_events WHERE source_ref_id = ANY($1)`, ids).Scan(&events)

	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM failed_messages WHERE status = 'recovered'`).Scan(&recovered)

	if _, pub := rec.snapshot(); events != n || recovered != n || len(pub) != n {
		t.Fatalf("events %d recovered %d published %d, want %d each", events, recovered, len(pub), n)
	}
}
