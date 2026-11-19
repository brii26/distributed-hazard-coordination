package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const fakeFeedToken = "test-token" // not a secret: only the fake feed below accepts it

// fakeFeed behaves like the satellite feed as measured in #308 (F8, F9):
//   - ?from is inclusive, Last-Event-ID is exclusive, and from wins when both are sent;
//   - the stream closes after perConn events, like the real server's periodic close;
//   - a second stream while one is open gets 429, and the open one is closed.
type fakeFeed struct {
	events   int64   // offsets 0 .. events-1 exist
	perConn  int     // close each stream after this many events; 0 sends all
	hold     bool    // keep a stream open and silent after its events instead of closing it
	statuses []int   // answers to the first requests, before any 200
	firstIDs []int64 // offsets the first stream sends instead of the normal run

	mu     sync.Mutex
	asked  []fakeRequest
	active int // index of the open stream, -1 when none
	close  context.CancelFunc
}

type fakeRequest struct{ from, lastEventID string }

func (f *fakeFeed) requests() []fakeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.asked)
}

func (f *fakeFeed) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()

	n, status := f.admit(req, cancel)
	if status != http.StatusOK {
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(w, `{"error":{"code":"%d","message":"fake"}}`, status)

		return
	}

	defer f.leave(n)

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)

	for i, off := range f.offsets(req, n) {
		if (f.perConn > 0 && i == f.perConn) || ctx.Err() != nil {
			return
		}

		_, _ = fmt.Fprintf(w, "id: %d\nevent: detection\ndata: {\"offset\":%d}\n\n", off, off)

		flusher.Flush()
	}

	if f.hold {
		<-ctx.Done()
	}
}

// admit records the request and decides the answer.
func (f *fakeFeed) admit(req *http.Request, cancel context.CancelFunc) (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	n := len(f.asked)
	f.asked = append(f.asked, fakeRequest{req.URL.Query().Get("from"), req.Header.Get("Last-Event-ID")})

	switch {
	case req.Header.Get("Authorization") != "Bearer "+fakeFeedToken:
		return n, http.StatusUnauthorized
	case n < len(f.statuses):
		return n, f.statuses[n]
	case f.close != nil: // one stream per group: the new one is refused and the open one closed
		f.close()

		return n, http.StatusTooManyRequests
	}

	f.active, f.close = n, cancel

	return n, http.StatusOK
}

func (f *fakeFeed) leave(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.active == n {
		f.active, f.close = -1, nil
	}
}

func (f *fakeFeed) offsets(req *http.Request, n int) []int64 {
	if n == 0 && f.firstIDs != nil {
		return f.firstIDs
	}

	start := int64(0)
	if v, err := strconv.ParseInt(req.Header.Get("Last-Event-ID"), 10, 64); err == nil {
		start = v + 1
	}

	if v, err := strconv.ParseInt(req.URL.Query().Get("from"), 10, 64); err == nil {
		start = v
	}

	var out []int64
	for off := start; off < f.events; off++ {
		out = append(out, off)
	}

	return out
}

// readFeed runs a reader against f until it has passed on want records or stopped. drain is how long the test waits before reading the pile; paused is how many requests the reader had made by then.
func readFeed(t *testing.T, f *fakeFeed, want int, silence, drain time.Duration) (got []int64, paused int, err error) {
	t.Helper()

	f.active = -1
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)

	r, err := newSatelitReader(srv.URL, fakeFeedToken)
	if err != nil {
		t.Fatal(err)
	}

	r.silence, r.backoff = silence, newBackoff(time.Millisecond, 5*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out := make(chan feedRecord)
	done := make(chan error, 1)

	go func() { done <- r.run(ctx, 0, out) }()

	time.Sleep(drain)

	paused = len(f.requests())

	for len(got) < want {
		select {
		case rec := <-out:
			got = append(got, rec.Offset)
		case err := <-done:
			return got, paused, err
		}
	}

	cancel()

	return got, paused, <-done
}

func wantOffsets(t *testing.T, got []int64, n int) {
	t.Helper()

	for i, off := range got {
		if off != int64(i) {
			t.Fatalf("offsets %v: position %d holds %d", got, i, off)
		}
	}

	if len(got) != n {
		t.Fatalf("%d offsets, want %d", len(got), n)
	}
}

// Criterion 8: the server closes the stream every 7 events; the reader resumes with Last-Event-ID only, and every offset arrives once, in order.
func TestSatelitReaderResumesWithoutGapOrRepeat(t *testing.T) {
	t.Parallel()

	f := &fakeFeed{events: 40, perConn: 7}
	got, _, err := readFeed(t, f, 40, time.Minute, 0)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}

	wantOffsets(t, got, 40)

	asked := f.requests()
	if asked[0] != (fakeRequest{from: "0"}) {
		t.Fatalf("first connect %+v, want ?from=0 and no header", asked[0])
	}

	for i := 1; i <= 5; i++ {
		if want := (fakeRequest{lastEventID: strconv.Itoa(7*i - 1)}); asked[i] != want {
			t.Fatalf("reconnect %d sent %+v, want %+v", i, asked[i], want)
		}
	}
}

func TestSatelitReaderStopsWhenRejected(t *testing.T) {
	t.Parallel()

	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()

			_, _, err := readFeed(t, &fakeFeed{events: 5, statuses: []int{status}}, 5, time.Minute, 0)
			if !errors.Is(err, errFeedRejected) {
				t.Fatalf("run: %v, want errFeedRejected", err)
			}
		})
	}
}

// 429 (another stream is open), 503 and 500 are retried. Nothing has arrived yet, so the retry still asks ?from=0.
func TestSatelitReaderRetriesBusyAndUnavailable(t *testing.T) {
	t.Parallel()

	f := &fakeFeed{events: 10, statuses: []int{http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusInternalServerError}}
	got, _, _ := readFeed(t, f, 10, time.Minute, 0)

	wantOffsets(t, got, 10)

	if asked := f.requests(); asked[3] != (fakeRequest{from: "0"}) {
		t.Fatalf("connect after errors %+v, want ?from=0", asked[3])
	}
}

// A stream that goes quiet is dropped after the silence timeout and resumed after the last offset.
func TestSatelitReaderReconnectsAfterSilence(t *testing.T) {
	t.Parallel()

	f := &fakeFeed{events: 6, perConn: 3, hold: true}
	got, _, _ := readFeed(t, f, 6, 50*time.Millisecond, 0)

	wantOffsets(t, got, 6)

	if asked := f.requests(); asked[1] != (fakeRequest{lastEventID: "2"}) {
		t.Fatalf("resume after silence %+v, want Last-Event-ID 2", asked[1])
	}
}

// A repeated offset is skipped; a gap drops the stream and the resume fetches the missing offset.
func TestSatelitReaderSkipsRepeatsAndRefetchesGaps(t *testing.T) {
	t.Parallel()

	f := &fakeFeed{events: 6, firstIDs: []int64{0, 1, 1, 2, 4, 5}}
	got, _, _ := readFeed(t, f, 6, time.Minute, 0)

	wantOffsets(t, got, 6)

	if asked := f.requests(); asked[1] != (fakeRequest{lastEventID: "2"}) {
		t.Fatalf("resume after gap %+v, want Last-Event-ID 2", asked[1])
	}
}

// With nobody reading the pile for 5 × the silence timeout, the reader waits on its one connection (waiting is not silence) and loses nothing.
func TestSatelitReaderPausesWhileThePileIsFull(t *testing.T) {
	t.Parallel()

	f := &fakeFeed{events: 30}
	got, paused, _ := readFeed(t, f, 30, 40*time.Millisecond, 200*time.Millisecond)

	wantOffsets(t, got, 30)

	if paused != 1 {
		t.Fatalf("%d requests while the pile was full, want 1", paused)
	}
}

func TestNewSatelitReaderChecksItsConfig(t *testing.T) {
	t.Parallel()

	if _, err := newSatelitReader("satellite.example", "t"); err == nil {
		t.Fatal("URL without a scheme accepted")
	}

	if _, err := newSatelitReader("https://satellite.example", ""); err == nil {
		t.Fatal("empty token accepted")
	}
}

func TestSSEReaderParsesBlocks(t *testing.T) {
	t.Parallel()

	in := "id: 1\nevent: detection\ndata: a\ndata: b\n\n: comment\r\nid:2\r\ndata:x\r\n\rid: 3\ndata: cut off"
	s := newSSEReader(strings.NewReader(in))

	for _, want := range []sseEvent{{Event: "detection", Data: "a\nb"}, {Event: "message", Data: "x"}} {
		got, err := s.next()
		if err != nil || got.Event != want.Event || got.Data != want.Data || got.ID == nil {
			t.Fatalf("got %+v %v, want %+v", got, err, want)
		}
	}

	if _, err := s.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("block cut off by EOF: %v, want io.EOF", err)
	}
}
