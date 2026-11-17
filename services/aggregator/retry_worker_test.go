package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// In-memory failed_messages: a claimed row stays hidden until the claim is released, like the pushed next_attempt_at.
type fakeQueue struct {
	mu       sync.Mutex
	rows     []*failedMessage
	hidden   map[string]bool
	failMark int // markRecovered fails this many times
}

func newFakeQueue() *fakeQueue { return &fakeQueue{hidden: map[string]bool{}} }

func (q *fakeQueue) insert(_ context.Context, m failedMessage) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, r := range q.rows {
		if r.MessageID == m.MessageID {
			return nil
		}
	}

	q.rows = append(q.rows, &m)

	return nil
}

func (q *fakeQueue) claimDue(_ context.Context, limit int, _ time.Duration) ([]failedMessage, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var out []failedMessage

	for _, r := range q.rows {
		if len(out) < limit && r.Status == failedRetrying && !q.hidden[r.MessageID] {
			q.hidden[r.MessageID] = true
			out = append(out, *r)
		}
	}

	return out, nil
}

func (q *fakeQueue) find(id string) *failedMessage {
	for _, r := range q.rows {
		if r.MessageID == id {
			return r
		}
	}

	return nil
}

func (q *fakeQueue) markRecovered(_ context.Context, id string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.failMark > 0 {
		q.failMark--

		return false, errors.New("db gone")
	}

	q.find(id).Status = failedRecovered

	return true, nil
}

func (q *fakeQueue) markSuperseded(_ context.Context, id string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	r := q.find(id)
	r.Status, r.Reason = failedRecovered, "superseded: "+r.Reason

	return true, nil
}

func (q *fakeQueue) recordRetryFailure(_ context.Context, id string, keep bool, reason string, _ time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	r := q.find(id)
	r.Attempts++
	r.Reason = reason

	if keep {
		delete(q.hidden, id)
	} else {
		r.Status = failedParked
	}

	return nil
}

// Releases every claim, as if the visibility timeout passed.
func (q *fakeQueue) release() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.hidden = map[string]bool{}
}

func (q *fakeQueue) statuses() map[string]int {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := map[string]int{}
	for _, r := range q.rows {
		out[r.Status]++
	}

	return out
}

// A store that writes into a map and can be switched off.
type memStore struct {
	mu     sync.Mutex
	down   error
	calls  int
	events map[string]HazardEvent
}

func (s *memStore) upsert(_ context.Context, h HazardEvent) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++

	if s.down != nil {
		return false, s.down
	}

	if s.events == nil {
		s.events = map[string]HazardEvent{}
	}

	_, had := s.events[h.SourceRefID]
	s.events[h.SourceRefID] = h

	return !had, nil
}

func (s *memStore) set(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.down = err
}

var errPgDown = &pgconn.PgError{Code: "57P01"}

func quickIngest(st *memStore, q *fakeQueue, rec *recorder, base time.Duration) (*ingest, chan pollResult) {
	in, ch := volcanicIngest(st.upsert, rec.publish)
	in.fail, in.retry, in.retryBase = q.insert, defaultRetryConfig, base

	return in, ch
}

func testWorker(q failedQueue, up func(context.Context, HazardEvent) (bool, error), rec *recorder) *retryWorker {
	return &retryWorker{q: q, upsert: up, publish: rec.publish, cfg: defaultRetryConfig}
}

func TestOutageKeepsRecordsWholeAndTheWorkerRecoversThemOnce(t *testing.T) { //nolint:gocyclo // one scenario, many checks
	t.Parallel()

	var (
		rec recorder
		st  = &memStore{down: errPgDown}
		q   = newFakeQueue()
	)

	in, ch := quickIngest(st, q, &rec, time.Millisecond)
	ch <- reports("R1", "R2")

	close(ch)
	in.run(context.Background())

	if got := q.statuses(); got[failedRetrying] != 2 || len(q.rows) != 2 {
		t.Fatalf("both records must be retrying: %v", got)
	}

	m := q.rows[0]

	var raw map[string]any
	if err := json.Unmarshal(m.Payload, &raw); err != nil || raw["report_id"] != "R1" {
		t.Fatalf("payload must be the record as received: %s %v", m.Payload, err)
	}

	var ev HazardEvent
	if err := json.Unmarshal(m.MappedEvent, &ev); err != nil || ev.SourceRefID != "R1" {
		t.Fatalf("mapped event missing: %s %v", m.MappedEvent, err)
	}

	if m.Attempts != 1 || m.NextAttemptAt.IsZero() || !strings.HasPrefix(m.Reason, codeStoreUnavailable+": ") || m.Source != sourcePVMBG {
		t.Fatalf("row fields wrong: %+v", m)
	}

	if _, pub := rec.snapshot(); len(pub) != 0 {
		t.Fatalf("nothing may be published while the store is down: %v", pub)
	}

	st.set(nil)

	w := testWorker(q, st.upsert, &rec)
	if n := w.tick(context.Background()); n != 2 {
		t.Fatalf("worker took %d rows, want 2", n)
	}

	if got := q.statuses(); got[failedRecovered] != 2 || len(st.events) != 2 {
		t.Fatalf("after recovery: %v events %d", got, len(st.events))
	}

	if _, pub := rec.snapshot(); !slices.Equal(pub, []string{"R1", "R2"}) {
		t.Fatalf("each recovered event is published once: %v", pub)
	}

	if w.tick(context.Background()) != 0 {
		t.Fatal("recovered rows must not be taken again")
	}
}

func TestIncompatibleSchemaIsParkedAtOnceAndNeverRetried(t *testing.T) {
	t.Parallel()

	var (
		rec recorder
		st  = &memStore{}
		q   = newFakeQueue()
	)

	in, ch := quickIngest(st, q, &rec, time.Millisecond)
	ch <- pollResult{records: []map[string]any{{"report_id": "R9", "volcano_id": 42}}} //nolint:goconst // test data

	close(ch)
	in.run(context.Background())

	if len(q.rows) != 1 || q.rows[0].Status != failedParked || q.rows[0].Attempts != 1 || q.rows[0].Retryable {
		t.Fatalf("want one parked row with attempts 1: %+v", q.rows)
	}

	if !strings.HasPrefix(q.rows[0].Reason, codeSchema+": ") && !strings.HasPrefix(q.rows[0].Reason, codeMalformed+": ") {
		t.Fatalf("reason %q", q.rows[0].Reason)
	}

	if testWorker(q, st.upsert, &rec).tick(context.Background()) != 0 || st.calls != 0 {
		t.Fatal("the worker must never touch a parked row")
	}
}

func TestWorkerBacksOffThenParksAtTheCap(t *testing.T) {
	t.Parallel()

	var rec recorder

	q := newFakeQueue()
	_ = q.insert(context.Background(), failedMessage{MessageID: "a", Status: failedRetrying, Attempts: 1, MappedEvent: []byte(`{"source_ref_id":"x"}`)})

	st := &memStore{down: errPgDown}
	w := testWorker(q, st.upsert, &rec)

	for want := 2; want <= defaultRetryConfig.maxAttempts; want++ {
		q.release()
		w.tick(context.Background())

		if q.rows[0].Attempts != want {
			t.Fatalf("attempts %d, want %d", q.rows[0].Attempts, want)
		}
	}

	if q.rows[0].Status != failedParked || w.parked.Load() != 1 || w.failed.Load() != int64(defaultRetryConfig.maxAttempts-2) {
		t.Fatalf("failure %d must park: %+v parked=%d failed=%d", defaultRetryConfig.maxAttempts, q.rows[0], w.parked.Load(), w.failed.Load())
	}
}

func TestWorkerParksAtOnceOnANonRetryableError(t *testing.T) {
	t.Parallel()

	var rec recorder

	q := newFakeQueue()
	_ = q.insert(context.Background(), failedMessage{MessageID: "a", Status: failedRetrying, Attempts: 1, MappedEvent: []byte(`{"source_ref_id":"x"}`)})
	_ = q.insert(context.Background(), failedMessage{MessageID: "b", Status: failedRetrying, Attempts: 1, MappedEvent: []byte(`not json`)})

	st := &memStore{down: &pgconn.PgError{Code: "23514"}}
	testWorker(q, st.upsert, &rec).tick(context.Background())

	if q.rows[0].Status != failedParked || q.rows[0].Attempts != 2 || q.rows[1].Status != failedParked {
		t.Fatalf("data errors must park on the first retry: %+v %+v", q.rows[0], q.rows[1])
	}
}

func TestSupersededRowIsRecoveredWithoutWriteOrPublish(t *testing.T) {
	t.Parallel()

	var rec recorder

	q := newFakeQueue()
	_ = q.insert(context.Background(), failedMessage{MessageID: "a", Status: failedRetrying, Reason: "timeout: x", Attempts: 1, MappedEvent: []byte(`{"source_ref_id":"x"}`)})

	w := testWorker(q, func(context.Context, HazardEvent) (bool, error) { return false, fmt.Errorf("copy: %w", errSuperseded) }, &rec)
	w.tick(context.Background())

	if r := q.rows[0]; r.Status != failedRecovered || !strings.HasPrefix(r.Reason, "superseded: ") {
		t.Fatalf("want recovered/superseded: %+v", r)
	}

	if _, pub := rec.snapshot(); len(pub) != 0 {
		t.Fatalf("published %v", pub)
	}
}

func TestTwoWorkersNeverTakeTheSameRow(t *testing.T) {
	t.Parallel()

	var (
		rec recorder
		st  = &memStore{}
		q   = newFakeQueue()
	)

	for i := range 200 {
		ev, _ := json.Marshal(HazardEvent{SourceRefID: fmt.Sprint("r", i)})
		_ = q.insert(context.Background(), failedMessage{MessageID: fmt.Sprint("m", i), Status: failedRetrying, Attempts: 1, MappedEvent: ev})
	}

	var (
		mu    sync.Mutex
		count = map[string]int{}
		wg    sync.WaitGroup
	)

	up := func(ctx context.Context, h HazardEvent) (bool, error) {
		mu.Lock()
		count[h.SourceRefID]++
		mu.Unlock()

		return st.upsert(ctx, h)
	}

	for range 2 {
		wg.Go(func() {
			w := testWorker(q, up, &rec)
			for n := 1; n > 0; {
				n = w.tick(context.Background())
			}
		})
	}

	wg.Wait()

	if len(count) != 200 {
		t.Fatalf("rows handled %d, want 200", len(count))
	}

	for id, n := range count {
		if n != 1 {
			t.Fatalf("%s written %d times", id, n)
		}
	}
}

func TestCrashBetweenUpsertAndMarkRecoveredChangesNothingTheSecondTime(t *testing.T) {
	t.Parallel()

	var (
		rec recorder
		st  = &memStore{}
		q   = newFakeQueue()
	)

	ev, _ := json.Marshal(HazardEvent{SourceRefID: "R1"})
	_ = q.insert(context.Background(), failedMessage{MessageID: "a", Status: failedRetrying, Attempts: 1, MappedEvent: ev})
	q.failMark = 1

	w := testWorker(q, st.upsert, &rec)
	w.tick(context.Background())

	if q.rows[0].Status != failedRetrying {
		t.Fatal("row must stay retrying when it could not be marked")
	}

	q.release()
	w.tick(context.Background())

	if _, pub := rec.snapshot(); q.rows[0].Status != failedRecovered || len(st.events) != 1 || len(pub) != 1 {
		t.Fatalf("second run: status %s events %d published %v", q.rows[0].Status, len(st.events), pub)
	}
}

func TestOutageDoesNotMakeEveryRecordOfTheBatchWaitForItsQuickTries(t *testing.T) {
	t.Parallel()

	var (
		rec recorder
		st  = &memStore{down: errPgDown}
		q   = newFakeQueue()
	)

	const base = 50 * time.Millisecond

	ids := make([]string, 60)
	for i := range ids {
		ids[i] = fmt.Sprint("R", i)
	}

	in, ch := quickIngest(st, q, &rec, base)
	ch <- reports(ids...)

	close(ch)

	start := time.Now()

	in.run(context.Background())

	oneRecord := base + 2*base // two waits between three tries
	if took := time.Since(start); took > oneRecord*2 {
		t.Fatalf("batch took %v, want about one record's quick tries (%v)", took, oneRecord)
	}

	if st.calls != quickTries || q.statuses()[failedRetrying] != 60 {
		t.Fatalf("store called %d times (want %d), rows %v", st.calls, quickTries, q.statuses())
	}
}

func TestIfTheFailedRowCannotBeStoredEitherTheBatchKeepsWaitingAndNothingIsDropped(t *testing.T) {
	t.Parallel()

	var (
		rec   recorder
		st    = &memStore{down: errPgDown}
		q     = newFakeQueue()
		fails = 4
	)

	in, ch := quickIngest(st, q, &rec, time.Millisecond)
	in.fail = func(ctx context.Context, m failedMessage) error {
		if fails > 0 {
			fails--

			return errors.New("db down")
		}

		return q.insert(ctx, m)
	}

	ch <- reports("R1")

	close(ch)
	in.run(context.Background())

	if len(q.rows) != 1 {
		t.Fatalf("the record must be kept once the table answers: %d", len(q.rows))
	}
}
