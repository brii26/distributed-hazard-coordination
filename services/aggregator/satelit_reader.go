package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"
)

// The satellite feed reader (#309) keeps one SSE stream open. Only the lease holder may run it: a second stream evicts the first (F9).
//
// Resume rule (doc/P4_FEED_NOTES.md, F8):
//   - The first connect of a run sends ?from=<start>, which is inclusive.
//   - Once an event has arrived, every reconnect sends only Last-Event-ID: <last offset>. The header is exclusive, so nothing repeats.
//
// Each offset is passed on exactly once and in order: a repeat is skipped, and a gap drops the connection so the resume asks for the missing offset again.

const (
	satelitSilence     = 65 * time.Second // 3 × the largest heartbeat gap seen at speed 60 (#308)
	satelitBackoffBase = 500 * time.Millisecond
	satelitBackoffMax  = 10 * time.Second
	satelitErrorBody   = 512
	satelitSessionBody = 4096 // a /v1/session answer is about 120 bytes
)

var (
	errFeedRejected = errors.New("satellite feed rejected the request")
	errFeedSequence = errors.New("satellite feed broke the offset sequence")
	errFeedSilent   = errors.New("satellite feed sent nothing within the silence timeout")
)

// One raw feed event. The pipeline parses it, and parks what does not parse.
type feedRecord struct {
	Offset int64
	Kind   string
	Data   []byte
	Read   time.Time // when the reader received it, for the read-to-saved metric only; the engine never reads it
}

type satelitReader struct {
	base    *url.URL
	token   string // only ever sent in the Authorization header, never logged
	client  *http.Client
	silence time.Duration
	backoff *backoff
}

func newSatelitReader(baseURL, token string) (*satelitReader, error) {
	base, err := url.Parse(baseURL)
	if err != nil || (base.Scheme != "https" && base.Scheme != "http") || base.Host == "" {
		return nil, fmt.Errorf("satellite base URL %q is not an http(s) URL", baseURL)
	}

	if token == "" {
		return nil, errors.New("satellite group token is empty")
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ForceAttemptHTTP2:     true,
	}

	return &satelitReader{
		base:    base,
		token:   token,
		client:  &http.Client{Transport: transport}, // no Client.Timeout: a stream stays open; the silence timer ends dead ones
		silence: satelitSilence,
		backoff: newBackoff(satelitBackoffBase, satelitBackoffMax),
	}, nil
}

// run streams the records from offset start onwards into out. It returns ctx.Err() when ctx ends, or errFeedRejected when the feed refuses the token or the request; every other failure reconnects after a backoff.
// out is the bounded pile. When it is full the reader stops reading; the server then holds or closes the stream (F10), and the resume continues from the last offset passed on.
func (r *satelitReader) run(ctx context.Context, start int64, out chan<- feedRecord) error {
	last := start - 1

	for {
		n, err := r.stream(ctx, start, &last, out)

		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, errFeedRejected):
			return err
		case n > 0:
			r.backoff.reset()
		}

		wait := r.backoff.failed()
		withID("").Info("satelit stream ended", "events", n, "last_offset", last, "reason", err.Error(), "reconnect_in", wait.String())

		if !sleepCtx(ctx, wait) {
			return ctx.Err()
		}
	}
}

// stream runs one connection and returns how many records it passed on. It always returns an error saying why the connection ended.
func (r *satelitReader) stream(ctx context.Context, start int64, last *int64, out chan<- feedRecord) (int, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	req, err := r.request(ctx, start, *last)
	if err != nil {
		return 0, err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("connect: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if err := streamStatus(resp); err != nil {
		return 0, err
	}

	if *last < start {
		withID("").Info("satelit stream connected", "from", start)
	} else {
		withID("").Info("satelit stream connected", "last_event_id", *last)
	}

	var silent atomic.Bool

	quiet := time.AfterFunc(r.silence, func() { silent.Store(true); cancel() })
	defer quiet.Stop()

	sse := newSSEReader(resp.Body)

	for n := 0; ; {
		ev, err := sse.next()
		if err != nil {
			if silent.Load() {
				return n, errFeedSilent
			}

			return n, fmt.Errorf("read: %w", err)
		}

		quiet.Reset(r.silence)

		rec, ok, err := nextRecord(ev, *last)
		if err != nil {
			return n, err
		}

		if !ok {
			continue
		}

		quiet.Stop() // waiting for room in the pile is not silence

		rec.Read = time.Now()

		select {
		case out <- rec:
		case <-ctx.Done():
			return n, fmt.Errorf("hand over: %w", ctx.Err())
		}

		quiet.Reset(r.silence)

		*last = rec.Offset
		n++
	}
}

func (r *satelitReader) request(ctx context.Context, start, last int64) (*http.Request, error) {
	u := r.base.JoinPath("/v1/stream")
	if last < start {
		u.RawQuery = url.Values{"from": {strconv.FormatInt(start, 10)}}.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+r.token)
	req.Header.Set("Accept", "text/event-stream")

	if last >= start {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(last, 10))
	}

	return req, nil
}

// The fields of GET /v1/session the lag metrics use.
type feedSession struct {
	LatestOffset int64     `json:"latest_offset"`
	VirtualNow   time.Time `json:"virtual_now"`
}

// session reads the feed's current position. It never opens a stream, so it does not evict the one running.
func (r *satelitReader) session(ctx context.Context) (feedSession, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.base.JoinPath("/v1/session").String(), nil)
	if err != nil {
		return feedSession{}, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+r.token)
	req.Header.Set("Accept", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return feedSession{}, fmt.Errorf("session: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return feedSession{}, fmt.Errorf("session answered %d", resp.StatusCode)
	}

	var s feedSession
	if err := json.NewDecoder(io.LimitReader(resp.Body, satelitSessionBody)).Decode(&s); err != nil {
		return feedSession{}, fmt.Errorf("session body: %w", err)
	}

	return s, nil
}

// streamStatus accepts 200. 400, 401 and 403 mean the request or the token is wrong, so retrying cannot help; anything else (429 while another stream is evicted, 503, 5xx) is retried.
func streamStatus(resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, satelitErrorBody))

	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("%w: %d %s", errFeedRejected, resp.StatusCode, body)
	default:
		return fmt.Errorf("satellite feed answered %d %s", resp.StatusCode, body)
	}
}

// nextRecord checks an event's id against the last offset passed on. ok is false for a block without an id and for a repeat. An id that is not the next offset ends the connection.
func nextRecord(ev sseEvent, last int64) (feedRecord, bool, error) {
	if ev.ID == nil {
		return feedRecord{}, false, nil
	}

	off, err := strconv.ParseInt(*ev.ID, 10, 64)
	if err != nil {
		return feedRecord{}, false, fmt.Errorf("%w: id %q is not an offset", errFeedSequence, *ev.ID)
	}

	switch {
	case off <= last:
		withID("").Warn("satelit repeated offset skipped", "offset", off, "last_offset", last)

		return feedRecord{}, false, nil
	case off > last+1:
		return feedRecord{}, false, fmt.Errorf("%w: offset %d after %d", errFeedSequence, off, last)
	}

	return feedRecord{Offset: off, Kind: ev.Event, Data: []byte(ev.Data)}, true, nil
}
