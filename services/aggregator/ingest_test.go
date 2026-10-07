package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// catat apa yang dikerjain ingest, urut.
type recorder struct {
	mu        sync.Mutex
	upserts   []string
	published []string
}

func (r *recorder) upsert(changed func(string) bool) func(context.Context, HazardEvent) (bool, error) {
	return func(_ context.Context, h HazardEvent) (bool, error) {
		r.mu.Lock()
		r.upserts = append(r.upserts, h.SourceRefID)
		r.mu.Unlock()

		return changed(h.SourceRefID), nil
	}
}

func (r *recorder) publish(_ context.Context, h HazardEvent, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.published = append(r.published, h.SourceRefID)
}

func (r *recorder) snapshot() (upserts, published []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.upserts), slices.Clone(r.published)
}

func always(string) bool { return true }

// ingest pake mapping asli dan upsert yang diberikan.
func volcanicIngest(upsert func(context.Context, HazardEvent) (bool, error), publish func(context.Context, HazardEvent, string)) (*ingest, chan pollResult) {
	ch := make(chan pollResult, 8)
	m := testMapper()

	return &ingest{
		name: sourcePVMBG, in: ch, upsert: upsert, publish: publish,
		mapBatch:  func(r pollResult) ([]HazardEvent, error) { return m.mapVolcanic(r.records) },
		retryBase: time.Millisecond, retryMax: 5 * time.Millisecond,
	}, ch
}

func reports(ids ...string) pollResult {
	var recs []map[string]any
	for _, id := range ids {
		recs = append(recs, report(id, "MERAPI", "Siaga"))
	}

	return pollResult{records: recs}
}

func TestIngestUpsertsInOrderAndPublishesOnlyWhatChanged(t *testing.T) {
	t.Parallel()

	var rec recorder

	in, ch := volcanicIngest(rec.upsert(func(ref string) bool { return ref != "R2" }), rec.publish)

	ch <- reports("R1", "R2", "R3")

	ch <- reports("R4")

	close(ch)
	in.run(context.Background())

	up, pub := rec.snapshot()
	if !slices.Equal(up, []string{"R1", "R2", "R3", "R4"}) || !slices.Equal(pub, []string{"R1", "R3", "R4"}) {
		t.Fatalf("upserts %v published %v, want every record in order and R2 (unchanged) not published", up, pub)
	}
}

func TestIngestSkipsARejectedRecordAndKeepsGoing(t *testing.T) {
	t.Parallel()

	var rec recorder

	in, ch := volcanicIngest(rec.upsert(always), rec.publish)
	batch := reports("R1", "R3")
	batch.records = append([]map[string]any{{"volcano_id": "MERAPI"}}, batch.records...)

	ch <- batch

	close(ch)
	in.run(context.Background())

	if up, _ := rec.snapshot(); !slices.Equal(up, []string{"R1", "R3"}) {
		t.Fatalf("upserts %v, want the two valid records", up)
	}
}

func TestSlowSourceDoesNotHoldUpTheOtherIngest(t *testing.T) {
	t.Parallel()

	var fastRec recorder

	slowIn, slowCh := volcanicIngest(func(context.Context, HazardEvent) (bool, error) {
		time.Sleep(300 * time.Millisecond)

		return true, nil
	}, func(context.Context, HazardEvent, string) {})
	fastIn, fastCh := volcanicIngest(fastRec.upsert(always), fastRec.publish)

	slowCh <- reports("S1", "S2", "S3")

	fastCh <- reports("F1", "F2", "F3")

	close(slowCh)
	close(fastCh)

	var wg sync.WaitGroup

	wg.Go(func() { slowIn.run(context.Background()) })

	start := time.Now()

	fastIn.run(context.Background())

	if d := time.Since(start); d > 150*time.Millisecond {
		t.Fatalf("the fast source took %v while the other was slow", d)
	}

	wg.Wait()
}

func TestPollerWaitsOnAFullChannelAndMovesTheCursorOnlyAfterHandoff(t *testing.T) {
	t.Parallel()

	out := make(chan pollResult)
	cursor := time.Date(2026, 10, 4, 10, 0, 0, 0, time.UTC)
	p := &poller{name: sourcePVMBG, breaker: testBreaker(), out: out, fetch: func(context.Context, time.Time) (pollResult, error) {
		return pollResult{records: []map[string]any{{"reported_at": "x"}}, cursor: cursor}, nil
	}}

	done := make(chan struct{})

	go func() {
		p.pollOnce(context.Background())
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("the poller must wait while its channel is full")
	case <-time.After(80 * time.Millisecond):
	}

	<-out

	<-done

	if !p.since.Equal(cursor) {
		t.Fatalf("cursor = %v, want it moved to %v once the batch was handed over", p.since, cursor)
	}
}

func TestCursorStaysPutWhenTheBatchWasNeverHandedOver(t *testing.T) {
	t.Parallel()

	p := &poller{name: sourcePVMBG, breaker: testBreaker(), out: make(chan pollResult), fetch: func(context.Context, time.Time) (pollResult, error) {
		return pollResult{records: []map[string]any{{"reported_at": "x"}}, cursor: time.Now()}, nil
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	p.pollOnce(ctx)

	if !p.since.IsZero() {
		t.Fatalf("cursor = %v, but nothing was handed over, so it must not have moved", p.since)
	}
}

func TestNoBatchIsLostWhenTheIngestIsSlow(t *testing.T) {
	t.Parallel()

	out := make(chan pollResult, 1)

	var calls atomic.Int64

	p := &poller{name: sourcePVMBG, interval: time.Millisecond, breaker: testBreaker(), out: out, fetch: func(context.Context, time.Time) (pollResult, error) {
		n := calls.Add(1)

		return pollResult{records: []map[string]any{{"n": float64(n)}}}, nil
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	go p.run(ctx)

	var got []int

	for res := range out {
		n, _ := res.records[0]["n"].(float64)
		got = append(got, int(n))

		time.Sleep(20 * time.Millisecond)
	}

	if len(got) < 5 {
		t.Fatalf("only %d batches arrived", len(got))
	}

	for i, n := range got {
		if n != i+1 {
			t.Fatalf("batches %v: expected 1, 2, 3 ... with none missing or reordered", got)
		}
	}
}

func TestPollerClosesItsChannelWhenStopped(t *testing.T) {
	t.Parallel()

	out := make(chan pollResult, 1)
	p := &poller{
		name: sourceBMKG, interval: time.Hour, breaker: testBreaker(), out: out,
		fetch: func(context.Context, time.Time) (pollResult, error) { return pollResult{}, nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.run(ctx)

	if _, open := <-out; open {
		t.Fatal("the channel must be closed once the poller stops")
	}
}

func TestAnOutageIsRetriedUntilTheWriteSucceeds(t *testing.T) {
	t.Parallel()

	var (
		calls atomic.Int32
		rec   recorder
	)

	in, ch := volcanicIngest(func(context.Context, HazardEvent) (bool, error) {
		if calls.Add(1) <= 3 {
			return false, errors.New("connection refused")
		}

		return true, nil
	}, rec.publish)

	ch <- reports("R1")

	close(ch)
	in.run(context.Background())

	if _, pub := rec.snapshot(); calls.Load() != 4 || !slices.Equal(pub, []string{"R1"}) {
		t.Fatalf("%d attempts, published %v; want 4 attempts and R1 published once", calls.Load(), pub)
	}
}

func TestADataErrorIsNotRetried(t *testing.T) {
	t.Parallel()

	var attempts []string

	in, ch := volcanicIngest(func(_ context.Context, h HazardEvent) (bool, error) {
		attempts = append(attempts, h.SourceRefID)

		if h.SourceRefID == "R1" {
			return false, &pgconn.PgError{Code: "23514"}
		}

		return true, nil
	}, func(context.Context, HazardEvent, string) {})

	ch <- reports("R1", "R2")

	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	in.run(ctx)

	if !slices.Equal(attempts, []string{"R1", "R2"}) {
		t.Fatalf("attempts %v, want R1 once (skipped) then R2", attempts)
	}
}

func TestShutdownStillWritesWhatTheChannelHolds(t *testing.T) {
	t.Parallel()

	var rec recorder

	in, ch := volcanicIngest(rec.upsert(always), rec.publish)

	ch <- reports("R1")

	ch <- reports("R2")

	ch <- reports("R3")

	close(ch)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in.run(ctx)

	if up, _ := rec.snapshot(); !slices.Equal(up, []string{"R1", "R2", "R3"}) {
		t.Fatalf("upserts %v, want everything handed over written before exit", up)
	}
}

func TestShutdownDuringAnOutageDoesNotHang(t *testing.T) {
	t.Parallel()

	in, ch := volcanicIngest(func(context.Context, HazardEvent) (bool, error) {
		return false, errors.New("connection refused")
	}, func(context.Context, HazardEvent, string) {})
	in.retryBase, in.retryMax = time.Hour, time.Hour

	ch <- reports("R1", "R2")

	close(ch)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})

	go func() {
		in.run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stopping while the database is down must not wait for it")
	}
}

// jalur penuh ke Postgres asli: dua source masuk hazard_events, poll berulang nggak nambah duplikat, tiap record dipublish sekali.
func TestPipelineWritesBothSourcesWithoutDuplicates(t *testing.T) {
	t.Parallel()

	s := testStore(t, 5)
	prefix := ref(t, s)

	t.Cleanup(func() {
		_, _ = s.db.ExecContext(context.Background(), `DELETE FROM hazard_events WHERE source_ref_id LIKE $1`, prefix+"%")
	})

	quakes := fmt.Sprintf(`[{"event_id":"%[1]s-E1","magnitude":4.0,"depth_km":10,"epicenter_lat":-8.5,"epicenter_lon":112.5,"region_name":"Jawa Timur","occurred_at":"2026-10-04T10:00:00Z","potential_tsunami":true},
		{"event_id":"%[1]s-E2","magnitude":6.8,"depth_km":10,"epicenter_lat":-7,"epicenter_lon":105,"region_name":"Banten","occurred_at":"2026-10-04T10:01:00Z","potential_tsunami":false}]`, prefix)
	warnings := fmt.Sprintf(`[{"warning_id":"W1","related_event_id":"%s-E1","threat_level":"Awas","affected_zones":["Jawa Timur"],"estimated_arrival":"2026-10-04T10:30:00Z"}]`, prefix)
	volcanic := fmt.Sprintf(`[{"report_id":"%[1]s-R1","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":4,"ash_column_height_m":900,"reported_at":"2026-10-04T10:02:00Z"}]`, prefix)

	bmkg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tsunami-warnings" {
			_, _ = w.Write([]byte(warnings))

			return
		}

		_, _ = w.Write([]byte(quakes))
	}))
	pvmbg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(volcanic)) }))

	t.Cleanup(bmkg.Close)
	t.Cleanup(pvmbg.Close)

	var (
		mu        sync.Mutex
		published = map[string]int{}
	)

	cfg := pollConfig{
		interval: 30 * time.Millisecond, timeout: time.Second, cbThreshold: 1000, cbOpenFor: time.Hour,
		bmkgURL: bmkg.URL, bmkgKey: "k", pvmbgURL: pvmbg.URL,
		token: func() string { return "tok" }, tokenErr: func() error { return nil },
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup

	for _, p := range newPipelines(cfg, s.upsert, func(_ context.Context, h HazardEvent, _ string) {
		mu.Lock()
		defer mu.Unlock()

		published[h.SourceRefID]++
	}) {
		wg.Go(func() { p.run(ctx) })
	}

	wg.Wait()

	var rows int
	if err := s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM hazard_events WHERE source_ref_id LIKE $1`, prefix+"%").Scan(&rows); err != nil || rows != 3 {
		t.Fatalf("%d rows (err %v), want 3: two seismic events and one volcanic report, each once", rows, err)
	}

	var sev string
	if err := s.db.QueryRowContext(context.Background(), `SELECT severity FROM hazard_events WHERE source_ref_id = $1`, prefix+"-E1").Scan(&sev); err != nil || sev != severityAwas {
		t.Fatalf("E1 severity = %q (err %v), want AWAS from its Awas warning", sev, err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(published) != 3 {
		t.Fatalf("published %v, want 3 distinct records", published)
	}

	for id, n := range published {
		if n != 1 {
			t.Fatalf("%s published %d times, want once however often it was polled", id, n)
		}
	}
}
