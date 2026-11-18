package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func nested(n int) []byte {
	return []byte(strings.Repeat(`{"a":`, n) + "1" + strings.Repeat("}", n))
}

func TestCheckRecord(t *testing.T) {
	t.Parallel()

	syn := errors.New("any syntax error")

	cases := []struct {
		name string
		raw  []byte
		want error
		code string
	}{
		{"valid", []byte(`{"a":"b"}`), nil, ""},
		{"unicode ok", []byte(`{"a":"héllo ✓"}`), nil, ""},
		{"escaped surrogate ok", []byte(`{"a":"\ud800"}`), nil, ""},
		{"brackets in a string do not count", []byte(`{"a":"` + strings.Repeat("[", 500) + `\"["}`), nil, ""},
		{"truncated", []byte(`{"a":`), syn, codeMalformedJSON},
		{"empty", nil, syn, codeMalformedJSON},
		{"depth 64 ok", nested(64), nil, ""},
		{"depth 65", nested(65), errTooDeep, codeTooDeep},
		{"100000 deep", nested(100000), errTooDeep, codeTooDeep},
		{"a million brackets", bytes.Repeat([]byte("["), 1<<20), errTooDeep, codeTooDeep},
		{"bad utf-8", []byte("{\"a\":\"\xff\"}"), errNotUTF8, codeInvalidUTF8},
		{"1 MiB ok", append(append([]byte(`{"a":"`), bytes.Repeat([]byte("x"), maxFailedPayload-8)...), `"}`...), nil, ""},
		{"1 MiB + 1", bytes.Repeat([]byte(" "), maxFailedPayload+1), errOversize, codeTooLarge},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			err := checkRecord(c.raw)

			var se *json.SyntaxError

			switch {
			case c.want == nil && err != nil:
				t.Fatalf("got %v, want valid", err)
			case errors.Is(c.want, syn) && !errors.As(err, &se):
				t.Fatalf("got %v, want a syntax error", err)
			case c.want != nil && !errors.Is(c.want, syn) && !errors.Is(err, c.want):
				t.Fatalf("got %v, want %v", err, c.want)
			}

			if c.want == nil {
				return
			}

			if retry, code := classifyFailure(err); retry || code != c.code {
				t.Fatalf("classify = (%v, %q), want (false, %q)", retry, code, c.code)
			}
		})
	}
}

func codesOf(bad []rejectedRaw) []string {
	var out []string

	for _, b := range bad {
		_, c := classifyFailure(b.err)
		out = append(out, c)
	}

	return out
}

func TestDecodeRecordsIsolatesBadRecords(t *testing.T) {
	t.Parallel()

	body := "[" + strings.Join([]string{
		`{"id":"a"}`,
		"{\"id\":\"\xff\"}",
		`{"id":"c"}`,
		string(nested(70)),
		`{"id":"e"}`,
		`5`,
	}, ",") + "]"

	recs, raws, bad, err := decodeRecords([]byte(body), false)
	if err != nil {
		t.Fatal(err)
	}

	if len(recs) != 3 || len(raws) != 3 || recs[0]["id"] != "a" || recs[1]["id"] != "c" || recs[2]["id"] != "e" {
		t.Fatalf("records %v, want a, c, e in order", recs)
	}

	if got := codesOf(bad); !slices.Equal(got, []string{codeInvalidUTF8, codeTooDeep, codeMalformed}) {
		t.Fatalf("codes %v", got)
	}
}

func TestDecodeRecordsBodyCutAfterRecordThree(t *testing.T) {
	t.Parallel()

	recs, _, bad, err := decodeRecords([]byte(`[{"id":1},{"id":2},{"id":3},{"id":4`), false)
	if err != nil || len(recs) != 3 {
		t.Fatalf("recs %d err %v, want 3 records and no error", len(recs), err)
	}

	if got := codesOf(bad); !slices.Equal(got, []string{codeMalformedJSON}) {
		t.Fatalf("codes %v, want one malformed_json", got)
	}
}

func TestDecodeRecordsGarbageBody(t *testing.T) {
	t.Parallel()

	for _, body := range []string{`{"a":1}`, `not json`, ``, "\xff"} {
		_, _, bad, err := decodeRecords([]byte(body), false)
		if err == nil || len(bad) != 1 {
			t.Fatalf("%q: err %v bad %d, want a failed poll and one row", body, err, len(bad))
		}
	}

	if _, _, bad, err := decodeRecords([]byte(`null`), false); err != nil || bad != nil {
		t.Fatalf("null: %v %v, want an empty poll", err, bad)
	}
}

type brokenReader struct{ data []byte }

func (b *brokenReader) Read(p []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, io.ErrUnexpectedEOF
	}

	n := copy(p, b.data)
	b.data = b.data[n:]

	return n, nil
}

func TestReadBody(t *testing.T) {
	t.Parallel()

	body, cut, err := readBody(strings.NewReader("12345"), 5)
	if err != nil || cut || string(body) != "12345" {
		t.Fatalf("at the cap: %q %v %v", body, cut, err)
	}

	body, cut, err = readBody(strings.NewReader("123456"), 5)
	if err != nil || !cut || string(body) != "12345" {
		t.Fatalf("over the cap: %q %v %v", body, cut, err)
	}

	// A network cut is a transport error, retryable, and does not become a parked row.
	_, _, err = readBody(&brokenReader{[]byte(`[{"id":1},`)}, 100)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v, want unexpected EOF", err)
	}

	if retry, _ := classifyFailure(err); !retry {
		t.Fatal("a body cut by the network must be retryable")
	}
}

func TestOversizeBodyKeepsEarlierRecords(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id":1},{"id":2},{"id":3,"pad":"` + strings.Repeat("x", 200) + `"}]`))
	}))

	t.Cleanup(srv.Close)

	c := &sourceClient{baseURL: srv.URL, timeout: time.Second, client: &http.Client{}, auth: bmkgAuth("k"), maxBody: 40}

	recs, _, bad, err := c.recordsRaw(context.Background(), "/x", time.Time{})
	if err != nil || len(recs) != 2 {
		t.Fatalf("recs %d err %v, want the 2 records before the cut", len(recs), err)
	}

	if got := codesOf(bad); !slices.Equal(got, []string{codeTooLarge}) {
		t.Fatalf("codes %v, want one too_large", got)
	}
}

func TestDecodeObservations(t *testing.T) {
	t.Parallel()

	body := `{"generated_at":"2026-01-01T00:00:00Z","items":[{"a":1},{"a":"` + "\xff" + `"},{"a":3}],"next_cursor":"c2","extra":{"x":[1]}}`

	b, bad, err := decodeObservations(strings.NewReader(body), 1<<20)
	if err != nil || len(b.Items) != 2 || b.NextCursor == nil || *b.NextCursor != "c2" || b.GeneratedAt == "" {
		t.Fatalf("items %d cursor %v err %v", len(b.Items), b.NextCursor, err)
	}

	if got := codesOf(bad); !slices.Equal(got, []string{codeInvalidUTF8}) {
		t.Fatalf("codes %v", got)
	}

	// Cut mid-array: earlier items stay, the cursor is left unread so the page is asked for again.
	b, bad, err = decodeObservations(strings.NewReader(`{"items":[{"a":1},{"a":2},{"a":`), 1<<20)
	if err != nil || len(b.Items) != 2 || b.NextCursor != nil || !slices.Equal(codesOf(bad), []string{codeMalformedJSON}) {
		t.Fatalf("items %d err %v bad %v", len(b.Items), err, codesOf(bad))
	}

	if _, bad, err = decodeObservations(strings.NewReader(`[1]`), 1<<20); err == nil || len(bad) != 1 {
		t.Fatalf("array body: err %v bad %d", err, len(bad))
	}
}

func TestIngestParksWhatThePollerSetAside(t *testing.T) {
	t.Parallel()

	var (
		rec  recorder
		mu   sync.Mutex
		rows []failedMessage
	)

	in, ch := volcanicIngest(rec.upsert(always), rec.publish)
	in.fail = func(_ context.Context, m failedMessage) error {
		mu.Lock()
		defer mu.Unlock()

		rows = append(rows, m)

		return nil
	}

	batch := reports("R1", "R2")
	batch.rejected = []rejectedRaw{{[]byte("{\"a\":\xff}"), errNotUTF8}, {nested(70), errTooDeep}}

	ch <- batch

	close(ch)
	in.run(context.Background())

	if up, _ := rec.snapshot(); !slices.Equal(up, []string{"R1", "R2"}) {
		t.Fatalf("upserts %v, want both good records", up)
	}

	if len(rows) != 2 || !strings.HasPrefix(rows[0].Reason, "invalid_utf8: ") || !strings.HasPrefix(rows[1].Reason, "too_deep: ") || rows[0].Retryable {
		t.Fatalf("rows %+v", rows)
	}
}

func TestPollKeepsGarbageBodyAsFailedMessage(t *testing.T) {
	t.Parallel()

	out := make(chan pollResult, 1)
	p := &poller{
		name: sourceBMKG, out: out, breaker: testBreaker(),
		fetch: func(context.Context, time.Time) (pollResult, error) {
			return pollResult{rejected: []rejectedRaw{{[]byte("nope"), errNotArray}}}, errNotArray
		},
	}

	if got := p.pollOnce(context.Background()); got != pollFailed {
		t.Fatalf("outcome %v, want a failed poll", got)
	}

	if res := <-out; len(res.rejected) != 1 {
		t.Fatalf("handed over %d rejected, want 1", len(res.rejected))
	}
}

func TestDecodeRecordsDeepMiddleRecordKeepsTheGoodOnes(t *testing.T) {
	t.Parallel()

	for _, d := range []int{65, 5000, 10001, 100000} {
		deep := strings.Repeat("[", d) + strings.Repeat("]", d)
		body := `[{"id":"a"},` + deep + `,{"id":"c"},{"id":"d"}]`

		recs, _, bad, err := decodeRecords([]byte(body), false)
		if err != nil || len(recs) != 3 || len(bad) != 1 {
			t.Fatalf("depth %d: good=%d bad=%d err=%v, want good=3 bad=1", d, len(recs), len(bad), err)
		}

		if got := codesOf(bad); !slices.Equal(got, []string{codeTooDeep}) {
			t.Fatalf("depth %d: codes %v", d, got)
		}

		if recs[1]["id"] != "c" || recs[2]["id"] != "d" {
			t.Fatalf("depth %d: records after the deep one are %v", d, recs)
		}
	}
}

func TestDecodeRecordsNullAndOddElements(t *testing.T) {
	t.Parallel()

	recs, _, bad, err := decodeRecords([]byte(`[{"id":1}, null ,"s\",]",[1,2],{"id":2} ]`), false)
	if err != nil || len(recs) != 2 || len(bad) != 3 {
		t.Fatalf("good=%d bad=%d err=%v, want good=2 bad=3", len(recs), len(bad), err)
	}

	if !errors.Is(bad[0].err, errNullRecord) {
		t.Fatalf("null element: %v, want a clear null reason", bad[0].err)
	}

	if _, _, bad, _ = decodeRecords([]byte(`[ ]`), false); len(bad) != 0 {
		t.Fatalf("empty array parked %d", len(bad))
	}
}

func TestDecodeObservationsDeepItemKeepsCursor(t *testing.T) {
	t.Parallel()

	deep := strings.Repeat("[", 100000) + strings.Repeat("]", 100000)
	body := `{"items":[{"a":1},` + deep + `,{"a":3}],"next_cursor":"c9"}`

	b, bad, err := decodeObservations(strings.NewReader(body), 1<<20)
	if err != nil || len(b.Items) != 2 || len(bad) != 1 || b.NextCursor == nil || *b.NextCursor != "c9" {
		t.Fatalf("items %d bad %d cursor %v err %v", len(b.Items), len(bad), b.NextCursor, err)
	}
}

func TestSetAsideFloodParksAtMostCapPlusOneRow(t *testing.T) {
	t.Parallel()

	const n = 2_000_000

	body := []byte("[" + strings.Repeat("1,", n-1) + "1]")

	_, _, bad, err := decodeRecords(body, false)
	if err != nil || len(bad) != n {
		t.Fatalf("bad %d err %v, want %d set aside", len(bad), err, n)
	}

	var rows []failedMessage

	in := &ingest{name: sourceBMKG, fail: func(_ context.Context, m failedMessage) error {
		rows = append(rows, m)

		return nil
	}}

	if !in.keepSetAside(context.Background(), pollResult{rejected: bad}) {
		t.Fatal("batch not clean")
	}

	if len(rows) != maxSetAsideParked+1 {
		t.Fatalf("%d upserts, want %d", len(rows), maxSetAsideParked+1)
	}

	if last := rows[len(rows)-1]; !strings.Contains(last.Reason, "1999900 more set aside in this batch") {
		t.Fatalf("summary reason %q", last.Reason)
	}
}

func TestDecodeRecordsTrailingGarbageIsOneRow(t *testing.T) {
	t.Parallel()

	recs, _, bad, err := decodeRecords([]byte("[{\"id\":1}] \n xyz [2]"), false)
	if err != nil || len(recs) != 1 || len(bad) != 1 || !errors.Is(bad[0].err, errTrailing) || string(bad[0].raw) != "xyz [2]" {
		t.Fatalf("recs %d bad %+v err %v", len(recs), bad, err)
	}

	if _, _, bad, _ = decodeRecords([]byte("[{\"id\":1}] \n"), false); len(bad) != 0 {
		t.Fatalf("trailing whitespace parked %d", len(bad))
	}
}

func TestSetAsideWithoutFailedStoreIsNotSilent(t *testing.T) {
	t.Parallel()

	in := &ingest{name: sourceBMKG}

	if in.keepSetAside(context.Background(), pollResult{rejected: []rejectedRaw{{[]byte("x"), errNotUTF8}}}) {
		t.Fatal("a dropped record must make the batch not clean")
	}
}

func TestSameBrokenPageParkedTwiceIsOneRow(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	in := &ingest{name: sourceSensorPantai, fail: s.insertFailed}
	page := pollResult{rejected: []rejectedRaw{{[]byte(`{"items":[{"a":1},{"a":`), errors.New("unexpected end of JSON input")}}}

	for range 2 {
		if !in.keepSetAside(context.Background(), page) {
			t.Fatal("park failed")
		}
	}

	rows, _, err := s.listFailed(context.Background(), failedParked, "", 10)
	if err != nil || len(rows) != 1 || rows[0].Attempts != 1 {
		t.Fatalf("rows %d err %v, want one row for the same page", len(rows), err)
	}
}

// Stand-in for the compose test with a stub upstream (that one needs images built with downloads): the real poller, ingest and
// Postgres run against a stub that rotates good pages, a 100000 deep record, bad UTF-8, a cut body and garbage. The pipeline must
// keep polling, store the good records and park each distinct bad payload once.
func TestPipelineSurvivesMixedGoodAndBadPayloads(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	prefix := ref(t, s)

	t.Cleanup(func() {
		_, _ = s.db.ExecContext(context.Background(), `DELETE FROM hazard_events WHERE source_ref_id LIKE $1`, prefix+"%")
	})

	good := func(n int) string {
		return fmt.Sprintf(`{"event_id":"%s-E%d","magnitude":4.0,"depth_km":10,"epicenter_lat":-8.5,"epicenter_lon":112.5,"region_name":"Jawa Timur","occurred_at":"2026-10-04T10:0%d:00Z","potential_tsunami":false}`, prefix, n, n)
	}
	deep := strings.Repeat("[", 100000) + strings.Repeat("]", 100000)
	pages := []string{
		"[" + good(1) + "," + good(2) + "]",
		"[" + good(1) + "," + deep + "," + good(2) + "," + good(3) + "]",
		"[" + good(1) + ",{\"event_id\":\"\xff\"}," + good(2) + "]",
		"[" + good(1) + "," + good(2) + ",{\"event_id\":",
		`<html>502 bad gateway</html>`,
	}

	var polls atomic.Int64

	bmkg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tsunami-warnings" {
			_, _ = w.Write([]byte(`[]`))

			return
		}

		_, _ = w.Write([]byte(pages[int(polls.Add(1)-1)%len(pages)]))
	}))
	pvmbg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`[]`)) }))

	t.Cleanup(bmkg.Close)
	t.Cleanup(pvmbg.Close)

	cfg := pollConfig{
		interval: 20 * time.Millisecond, timeout: 2 * time.Second, cbThreshold: 100000, cbOpenFor: time.Hour,
		bmkgURL: bmkg.URL, bmkgKey: "k", pvmbgURL: pvmbg.URL,
		token: func() string { return "tok" }, tokenErr: func() error { return nil },
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup

	for _, p := range newPipelines(cfg, s.upsert, func(context.Context, HazardEvent, string) {}) {
		p.ingest.fail = s.insertFailed

		wg.Go(func() { p.run(ctx) })
	}

	wg.Wait()

	var events, rows int
	if err := s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM hazard_events WHERE source_ref_id LIKE $1`, prefix+"%").Scan(&events); err != nil || events != 3 {
		t.Fatalf("%d hazard rows (err %v), want 3 good events", events, err)
	}

	if err := s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM failed_messages WHERE source = $1`, sourceBMKG).Scan(&rows); err != nil || rows != 4 {
		t.Fatalf("%d failed rows (err %v), want 4 distinct bad payloads, each once", rows, err)
	}

	t.Logf("polls=%d hazard_rows=%d failed_rows=%d", polls.Load(), events, rows)

	if polls.Load() < 20 {
		t.Fatalf("only %d polls: the pipeline did not keep going", polls.Load())
	}
}
