package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// An in-memory failed_messages for the replayer.
type fakeReplayQueue struct {
	mu       sync.Mutex
	rows     []failedMessage // all rows, parked unless in status
	status   map[string]string
	reasons  map[string]string
	attempts map[string]int
	busy     bool
	oversize int
}

func newFakeReplayQueue(rows ...failedMessage) *fakeReplayQueue {
	q := &fakeReplayQueue{rows: rows, status: map[string]string{}, reasons: map[string]string{}, attempts: map[string]int{}}
	for _, r := range rows {
		q.status[r.MessageID] = failedParked
	}

	return q
}

func (q *fakeReplayQueue) lockReplay(context.Context, string) (func(), bool, error) {
	return func() {}, !q.busy, nil
}

func (q *fakeReplayQueue) parkedPage(_ context.Context, source string, at time.Time, id string, limit int) ([]failedMessage, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var out []failedMessage

	for _, r := range q.rows {
		after := r.ReceivedAt.After(at) || r.ReceivedAt.Equal(at) && r.MessageID > id
		if q.status[r.MessageID] == failedParked && r.Source == source && after && len(out) < limit {
			out = append(out, r)
		}
	}

	return out, nil
}

func (q *fakeReplayQueue) countParkedOversize(context.Context, string) (int, error) {
	return q.oversize, nil
}

func (q *fakeReplayQueue) markReplayed(_ context.Context, id string, sup bool) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.status[id] = failedRecovered
	if sup {
		q.reasons[id] = "superseded"
	}

	return true, nil
}

func (q *fakeReplayQueue) recordReplayFailure(_ context.Context, id, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.attempts[id]++
	q.reasons[id] = reason

	return nil
}

var replayBase = time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

func pvmbgRow(msg string, recv int, report, volcano, at string) failedMessage {
	payload := fmt.Sprintf(`{"report_id":%q,"volcano_id":%q,"alert_level":"Siaga","reported_at":%q,"ash_column_height_m":1200,"eruption_count_24h":3}`, report, volcano, at)
	m := newFailedMessage(sourcePVMBG, []byte(payload), "incompatible: x", false, replayBase.Add(time.Duration(recv)*time.Minute))
	m.MessageID = msg

	return m
}

type replayRig struct {
	q      *fakeReplayQueue
	r      *replayer
	writes []HazardEvent
	pubs   []HazardEvent
	// stored holds the newest written time per ref id, like the guard in SQL.
	stored  map[string]time.Time
	upErr   error
	upErrAt string
}

func newReplayRig(rows ...failedMessage) *replayRig {
	g := &replayRig{q: newFakeReplayQueue(rows...), stored: map[string]time.Time{}}
	g.r = &replayer{
		q: g.q, m: testMapper(), batch: 500,
		upsert: func(_ context.Context, h HazardEvent) (bool, error) {
			if g.upErr != nil && h.SourceRefID == g.upErrAt {
				return false, g.upErr
			}

			if cur, ok := g.stored[h.SourceRefID]; ok && h.OccurredAt.Before(cur) {
				return false, errSuperseded
			}

			g.stored[h.SourceRefID] = h.OccurredAt
			g.writes = append(g.writes, h)

			return true, nil
		},
		publish: func(_ context.Context, h HazardEvent, _ string) { g.pubs = append(g.pubs, h) },
	}

	return g
}

func TestReplayRecoversAllAndSecondRunDoesNothing(t *testing.T) {
	t.Parallel()

	var rows []failedMessage
	for i := range 6 {
		rows = append(rows, pvmbgRow(fmt.Sprintf("m%d", i), i, fmt.Sprintf("R%d", i), "MERAPI", "2026-10-01T09:00:00Z"))
	}

	g := newReplayRig(rows...)

	rep, err := g.r.run(t.Context(), sourcePVMBG, 100)
	if err != nil || rep.Scanned != 6 || rep.Recovered != 6 || len(g.writes) != 6 || len(g.pubs) != 6 {
		t.Fatalf("first run: %+v err=%v writes=%d", rep, err, len(g.writes))
	}

	for id, st := range g.q.status {
		if st != failedRecovered {
			t.Fatalf("%s is %s", id, st)
		}
	}

	rep, err = g.r.run(t.Context(), sourcePVMBG, 100)
	if err != nil || rep.Scanned != 0 || len(g.writes) != 6 {
		t.Fatalf("second run must do nothing: %+v err=%v", rep, err)
	}
}

func TestReplayKeepsSourceOrderAndNeverOverwritesNewer(t *testing.T) {
	t.Parallel()

	newer := pvmbgRow("a-new", 1, "R1", "MERAPI", "2026-10-01T09:30:00Z")
	older := pvmbgRow("b-old", 2, "R1", "MERAPI", "2026-10-01T09:00:00Z")
	g := newReplayRig(newer, older)

	rep, err := g.r.run(t.Context(), sourcePVMBG, 100)
	if err != nil || rep.Recovered != 2 || len(g.writes) != 2 {
		t.Fatalf("%+v %v", rep, err)
	}

	if !g.writes[0].OccurredAt.Before(g.writes[1].OccurredAt) {
		t.Fatalf("versions must be written oldest first: %v then %v", g.writes[0].OccurredAt, g.writes[1].OccurredAt)
	}

	if g.pubs[1].OccurredAt.Format(time.RFC3339) != "2026-10-01T09:30:00Z" {
		t.Fatalf("stored must be the newer one: %v", g.stored["R1"])
	}

	// A stored row that is already newer turns the replayed one into superseded, with no publish.
	g2 := newReplayRig(pvmbgRow("c", 1, "R9", "MERAPI", "2026-10-01T09:00:00Z"))
	g2.stored["R9"] = replayBase

	rep, err = g2.r.run(t.Context(), sourcePVMBG, 100)
	if err != nil || rep.Superseded != 1 || rep.Recovered != 0 || len(g2.pubs) != 0 || g2.q.status["c"] != failedRecovered {
		t.Fatalf("%+v %v", rep, err)
	}
}

func TestReplayKeepsFailingRowParkedAndRecoversTheRest(t *testing.T) {
	t.Parallel()

	bad := pvmbgRow("bad", 1, "R1", "MERAPI", "not a time")
	notJSON := newFailedMessage(sourcePVMBG, []byte(`{"report_id":`), "invalid json", false, replayBase)
	g := newReplayRig(bad, notJSON, pvmbgRow("ok", 2, "R2", "MERAPI", "2026-10-01T09:00:00Z"))

	rep, err := g.r.run(t.Context(), sourcePVMBG, 100)
	if err != nil || rep.Recovered != 1 || rep.StillParked != 2 || len(rep.Errors) != 2 {
		t.Fatalf("%+v %v", rep, err)
	}

	if g.q.status["bad"] != failedParked || g.q.attempts["bad"] != 1 || g.q.status["ok"] != failedRecovered {
		t.Fatalf("status %v attempts %v", g.q.status, g.q.attempts)
	}
}

func TestReplayResponseTruncatesLongFailureReason(t *testing.T) {
	t.Parallel()

	secret := strings.Repeat("SECRETPAYLOAD", 100)
	g := newReplayRig(pvmbgRow("x", 1, "R1", "MERAPI", "2026-10-01T09:00:00Z"))
	g.upErr, g.upErrAt = fmt.Errorf("%w: %s", errBadEvent, secret), "R1"

	rep, err := g.r.run(t.Context(), sourcePVMBG, 100)
	if err != nil || rep.StillParked != 1 || len(rep.Errors) != 1 {
		t.Fatalf("%+v %v", rep, err)
	}

	got := rep.Errors[0].Reason
	if len([]rune(got)) > replayReasonMax+3 || strings.Contains(got, strings.Repeat("SECRETPAYLOAD", 20)) || !strings.HasSuffix(got, "...") {
		t.Fatalf("reason must be cut to %d chars: %d %q", replayReasonMax, len(got), got)
	}

	if short := truncateReason("short"); short != "short" {
		t.Fatalf("short reason must stay: %q", short)
	}
}

func TestReplayStopsOnStoreOutageAndMarksNothingAfter(t *testing.T) {
	t.Parallel()

	g := newReplayRig(pvmbgRow("a", 1, "R1", "MERAPI", "2026-10-01T09:00:00Z"), pvmbgRow("b", 2, "R2", "MERAPI", "2026-10-01T09:00:00Z"))
	g.upErr, g.upErrAt = context.DeadlineExceeded, "R2"

	rep, err := g.r.run(t.Context(), sourcePVMBG, 100)
	if err == nil || rep.Recovered != 1 || g.q.status["b"] != failedParked || g.q.attempts["b"] != 0 {
		t.Fatalf("%+v %v %v", rep, err, g.q.status)
	}
}

func TestReplayBusyAndLimitAndPaging(t *testing.T) {
	t.Parallel()

	var rows []failedMessage
	for i := range 7 {
		rows = append(rows, pvmbgRow(fmt.Sprintf("m%d", i), i, fmt.Sprintf("R%d", i), "MERAPI", "2026-10-01T09:00:00Z"))
	}

	g := newReplayRig(rows...)
	g.r.batch = 2

	rep, err := g.r.run(t.Context(), sourcePVMBG, 5)
	if err != nil || rep.Scanned != 5 || rep.Recovered != 5 {
		t.Fatalf("limit 5 over pages of 2: %+v %v", rep, err)
	}

	g.q.busy = true
	if _, err = g.r.run(t.Context(), sourcePVMBG, 5); !errors.Is(err, errReplayBusy) {
		t.Fatalf("want busy, got %v", err)
	}
}

func TestMapParkedBMKGAndPVMBG(t *testing.T) {
	t.Parallel()

	m := testMapper()

	ev, err := m.mapParked(pvmbgRow("a", 1, "R1", "MERAPI", "2026-10-01T09:00:00Z"))
	if err != nil || ev.Source != sourcePVMBG || ev.SourceRefID != "R1" {
		t.Fatalf("%+v %v", ev, err)
	}

	bmkg := newFailedMessage(sourceBMKG, []byte(`{"event_id":"E1","occurred_at":"2026-10-01T09:00:00Z","magnitude":5.1,"epicenter_lat":-6.2,"epicenter_lon":106.8,"region_name":"Jakarta"}`), "x", false, replayBase)
	if ev, err = m.mapParked(bmkg); err != nil || ev.SourceRefID != "E1" || ev.Latitude == nil {
		t.Fatalf("number fields must map as floats: %+v %v", ev, err)
	}

	for _, p := range []string{`{`, `[]`, `{"report_id":"R1"}`} {
		if _, err = m.mapParked(newFailedMessage(sourcePVMBG, []byte(p), "x", false, replayBase)); err == nil {
			t.Fatalf("payload %q must fail", p)
		}
	}

	if _, err = m.mapParked(newFailedMessage(sourceSatelit, []byte(`{}`), "x", false, replayBase)); err == nil {
		t.Fatal("other sources are not replayed")
	}
}

const adminTestToken = "s3cret"

// Sends one request through the aggregator mux.
func adminCall(t *testing.T, mux http.Handler, method, target, token string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), method, target, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	return rec
}

func TestAdminReplayRoute(t *testing.T) {
	t.Parallel()

	g := newReplayRig(pvmbgRow("a", 1, "R1", "MERAPI", "2026-10-01T09:00:00Z"))
	api := &readAPI{replay: g.r, adminTok: adminTestToken, auditTok: "audit"}
	mux := newMux(api)

	const base = "/internal/admin/replay"

	for _, c := range []struct {
		target, token string
		want          int
	}{
		{base + "?source=PVMBG", "", http.StatusUnauthorized},
		{base + "?source=PVMBG", "audit", http.StatusUnauthorized},
		{base + "?source=SATELIT", adminTestToken, http.StatusBadRequest},
		{base, adminTestToken, http.StatusBadRequest},
		{base + "?source=PVMBG&limit=0", adminTestToken, http.StatusBadRequest},
		{base + "?source=PVMBG&limit=50001", adminTestToken, http.StatusBadRequest},
		{base + "?source=PVMBG&limit=x", adminTestToken, http.StatusBadRequest},
	} {
		if got := adminCall(t, mux, http.MethodPost, c.target, c.token).Code; got != c.want {
			t.Errorf("%s token=%q: %d, want %d", c.target, c.token, got, c.want)
		}
	}

	rec := adminCall(t, mux, http.MethodPost, base+"?source=PVMBG", adminTestToken)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"recovered":1`) {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}

	if got := adminCall(t, mux, http.MethodGet, base, "").Code; got != http.StatusMethodNotAllowed {
		t.Errorf("GET must be refused, got %d", got)
	}

	api.adminTok = ""

	if got := adminCall(t, mux, http.MethodPost, base+"?source=PVMBG", "").Code; got != http.StatusServiceUnavailable {
		t.Errorf("no ADMIN_TOKEN must fail closed, got %d", got)
	}
}

func TestAdminReplayBusyIs409(t *testing.T) {
	t.Parallel()

	g := newReplayRig()
	g.q.busy = true
	mux := newMux(&readAPI{replay: g.r, adminTok: adminTestToken})

	if got := adminCall(t, mux, http.MethodPost, "/internal/admin/replay?source=BMKG", adminTestToken).Code; got != http.StatusConflict {
		t.Fatalf("got %d", got)
	}
}
