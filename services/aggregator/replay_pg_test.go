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

func replayHazard(ref string, occurred time.Time) HazardEvent {
	return HazardEvent{HazardID: hazardID(sourcePVMBG, ref), Source: sourcePVMBG, SourceRefID: ref, HazardType: typeVolcanic, OccurredAt: occurred, IngestedAt: occurred}
}

func TestReplayGuardAgainstPostgres(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, `TRUNCATE hazard_events`); err != nil {
		t.Fatal(err)
	}

	t0 := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	if _, err := s.upsert(ctx, replayHazard("R1", t0.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}

	// An older source time never overwrites the stored row.
	if changed, err := s.upsert(withReplayAt(ctx, time.Now()), replayHazard("R1", t0)); !errors.Is(err, errSuperseded) || changed {
		t.Fatalf("older version: changed=%v err=%v", changed, err)
	}

	// A newer source time does.
	if changed, err := s.upsert(withReplayAt(ctx, time.Now()), replayHazard("R1", t0.Add(2*time.Hour))); err != nil || !changed {
		t.Fatalf("newer version: changed=%v err=%v", changed, err)
	}

	// Same source time, replayed message received before the stored write: skipped. Received after it: written.
	tie := replayHazard("R1", t0.Add(2*time.Hour))
	tie.AreaName = new(string)

	if _, err := s.upsert(withReplayAt(ctx, time.Now().Add(-time.Hour)), tie); !errors.Is(err, errSuperseded) {
		t.Fatalf("tie with older receive time: %v", err)
	}

	if changed, err := s.upsert(withReplayAt(ctx, time.Now().Add(time.Minute)), tie); err != nil || !changed {
		t.Fatalf("tie with newer receive time: changed=%v err=%v", changed, err)
	}

	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM hazard_events`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("one row per ref id: %d %v", n, err)
	}
}

func TestMigration014RunsTwice(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)

	ddl, err := os.ReadFile("migrations/014_source_received_at.sql")
	if err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if _, err := s.db.ExecContext(context.Background(), string(ddl)); err != nil {
			t.Fatal(err)
		}
	}
}

// Two parked versions of one id with the same source time and different attributes: the one received later wins, even though the replay writes the earlier one first and moves updated_at to now.
func TestReplayTieOnSourceTimeKeepsLaterReceivedAgainstPostgres(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, `TRUNCATE hazard_events`); err != nil {
		t.Fatal(err)
	}

	const rec = `{"report_id":"R1","volcano_id":"MERAPI","alert_level":%q,"reported_at":"2026-10-01T09:00:00Z","ash_column_height_m":1,"eruption_count_24h":1}`

	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	received := time.Now().Add(-2 * time.Hour)

	// B (Awas) is received one minute after A (Waspada) but parked rows come back by receive time, so A is written first.
	for i, level := range []string{"Waspada", "Awas"} {
		m := newFailedMessage(sourcePVMBG, fmt.Appendf(nil, rec, level), "schema_incompatible: x", false, received.Add(time.Duration(i)*time.Minute))
		if err := s.insertFailed(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	var pubs []HazardEvent

	r := &replayer{
		q: s, m: &mapper{now: func() time.Time { return at.Add(time.Hour) }, newID: hazardID}, upsert: s.upsert, batch: 1,
		publish: func(_ context.Context, h HazardEvent, _ string) { pubs = append(pubs, h) },
	}

	rep, err := r.run(ctx, sourcePVMBG, 100)
	if err != nil || rep.Recovered != 2 || rep.Superseded != 0 {
		t.Fatalf("both must be written, none superseded: %+v %v", rep, err)
	}

	var level string
	if err = s.db.QueryRowContext(ctx, `SELECT severity FROM hazard_events WHERE source_ref_id = 'R1'`).Scan(&level); err != nil || level != severityAwas {
		t.Fatalf("the later received version must be stored: %q %v", level, err)
	}

	if len(pubs) != 2 || (pubs[1].Severity == nil || *pubs[1].Severity != severityAwas) {
		t.Fatalf("the later received version must be published last: %+v", pubs)
	}
}

func TestReplayParkedRowsAndLockAgainstPostgres(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()

	for i, raw := range []string{`{"a":1}`, `{"a":2}`} {
		m := newFailedMessage(sourcePVMBG, []byte(raw), "schema_incompatible: x", false, time.Now().Add(time.Duration(i)*time.Second))
		if err := s.insertFailed(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	other := newFailedMessage(sourceBMKG, []byte(`{"a":3}`), "schema_incompatible: x", false, time.Now())
	if err := s.insertFailed(ctx, other); err != nil {
		t.Fatal(err)
	}

	rows, err := s.parkedPage(ctx, sourcePVMBG, time.Time{}, "", 10)
	if err != nil || len(rows) != 2 || string(rows[0].Payload) != `{"a":1}` {
		t.Fatalf("parked page: %+v %v", rows, err)
	}

	if err = s.recordReplayFailure(ctx, rows[0].MessageID, "again"); err != nil {
		t.Fatal(err)
	}

	if ok, err := s.markReplayed(ctx, rows[1].MessageID, false); err != nil || !ok {
		t.Fatalf("mark: %v %v", ok, err)
	}

	if ok, _ := s.markReplayed(ctx, rows[1].MessageID, false); ok {
		t.Fatal("a recovered row must not be marked twice")
	}

	if rows, _ = s.parkedPage(ctx, sourcePVMBG, time.Time{}, "", 10); len(rows) != 1 || rows[0].Attempts != 2 {
		t.Fatalf("one row left with attempts 2: %+v", rows)
	}
}

func TestReplayLockAgainstPostgres(t *testing.T) { //nolint:paralleltest // shares the database with the other tests
	s := testFailedStore(t)
	ctx := context.Background()

	var (
		mu      sync.Mutex
		holders int
		busy    int
		wg      sync.WaitGroup
	)

	hold, release := make(chan struct{}), make(chan struct{})

	for range 20 {
		wg.Go(func() {
			rel, ok, err := s.lockReplay(ctx, sourcePVMBG)
			if err != nil {
				t.Error(err)

				return
			}

			mu.Lock()
			defer mu.Unlock()

			if !ok {
				busy++

				return
			}

			holders++

			go func() { <-release; rel() }()
		})
	}

	wg.Wait()
	close(hold)

	if holders != 1 || busy != 19 {
		t.Fatalf("one runner, 19 busy: %d %d", holders, busy)
	}

	close(release)
}

func TestReplayAcceptanceAgainstPostgres(t *testing.T) { //nolint:paralleltest,gocyclo // shares one truncated table, one linear scenario
	s := testFailedStore(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, `TRUNCATE hazard_events`); err != nil {
		t.Fatal(err)
	}

	const (
		n    = 5
		rec  = `{"report_id":%q,"volcano_id":"MERAPI","alert_level":%q,"reported_at":%q,"ash_column_height_m":1,"eruption_count_24h":1}`
		base = "2026-10-01T09:00:00Z"
	)

	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	var raws []string
	for i := range n {
		raws = append(raws, fmt.Sprintf(rec, fmt.Sprintf("R%d", i), "Waspada", base))
	}

	// Two versions of R0, the newer one parked first.
	raws = append(raws, fmt.Sprintf(rec, "R0", "Awas", at.Add(time.Hour).Format(time.RFC3339)))

	for i, raw := range raws {
		m := newFailedMessage(sourcePVMBG, []byte(raw), "schema_incompatible: x", false, time.Now().Add(time.Duration(i)*time.Second))
		if err := s.insertFailed(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	var pubs int

	r := &replayer{
		q: s, m: &mapper{now: func() time.Time { return at.Add(2 * time.Hour) }, newID: hazardID}, upsert: s.upsert, batch: 2,
		publish: func(context.Context, HazardEvent, string) { pubs++ },
	}

	rep, err := r.run(ctx, sourcePVMBG, 100)
	if err != nil || rep.Scanned != n+1 || rep.Recovered != n+1 || rep.StillParked != 0 {
		t.Fatalf("first run: %+v %v", rep, err)
	}

	var rows int

	var level string

	if err = s.db.QueryRowContext(ctx, `SELECT count(*), max(severity) FILTER (WHERE source_ref_id = 'R0') FROM hazard_events`).Scan(&rows, &level); err != nil || rows != n || level != severityAwas {
		t.Fatalf("one row per id and the newer version wins: rows=%d level=%q %v", rows, level, err)
	}

	if rep, err = r.run(ctx, sourcePVMBG, 100); err != nil || rep.Scanned != 0 || pubs != n+1 {
		t.Fatalf("second run must be a no-op: %+v pubs=%d %v", rep, pubs, err)
	}
}
