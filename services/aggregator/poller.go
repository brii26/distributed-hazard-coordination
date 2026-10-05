package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultPollInterval = 3 * time.Second
	defaultPollTimeout  = 4 * time.Second
	maxPollBody         = 8 << 20
)

// pollResult is what one poll cycle brings back from a source.
type pollResult struct {
	// records are the source's own records (BMKG seismic events, PVMBG
	// volcanic reports); warnings are BMKG's tsunami warnings polled with them.
	records, warnings []map[string]any
	// cursor is the newest record timestamp seen, used as the next ?since=.
	// Zero means the poll carried nothing to advance it with.
	cursor time.Time
	// correlationID names the poll cycle (doc/TRD_M1.md 0.1); it follows the
	// records into ingest and out on the broker message.
	correlationID string
}

// poller runs one source's polling loop. The loop is a single goroutine that
// does one call at a time, so there can never be two calls in flight for the
// same source, and a slow source can only slow its own loop (doc/TRD_M1.md 3.1).
type poller struct {
	name     string
	interval time.Duration
	fetch    func(ctx context.Context, since time.Time) (pollResult, error)
	breaker  *breaker
	// out carries each successful poll to this source's ingest goroutine. The
	// poller is its only writer and closes it on exit, which tells ingest to drain.
	out chan<- pollResult

	// since is only touched by the run goroutine.
	// ponytail: the cursor lives in memory, so a restart re-reads everything
	// once; the upsert dedup (#21) makes that harmless. It also assumes the
	// source's timestamps never go backwards.
	since time.Time
}

// nextWait is how long to sleep after a call that took elapsed: the rest of the
// interval, or nothing when the call already used it up (no pile-up, no extra wait).
func nextWait(interval, elapsed time.Duration) time.Duration {
	return max(interval-elapsed, 0)
}

// run polls until ctx is done, then closes out.
func (p *poller) run(ctx context.Context) {
	defer close(p.out)

	for {
		start := time.Now()

		p.pollOnce(ctx)

		select {
		case <-ctx.Done():
			return
		case <-time.After(nextWait(p.interval, time.Since(start))):
		}
	}
}

// pollOnce makes one call, logs how it ended and hands the records on. Any
// error, including a timeout, counts as a failed poll; the cursor only moves
// once the records have been handed over.
func (p *poller) pollOnce(ctx context.Context) {
	// While the breaker is OPEN the source is left alone; the loop keeps its
	// rhythm and asks again next cycle.
	if !p.breaker.allow() {
		return
	}

	id := newHazardID() // a random UUID is as good a correlation ID as any
	l := withID(id)

	start := time.Now()
	res, err := p.fetch(withCorrelation(ctx, id), p.since)
	elapsed := time.Since(start).Milliseconds()
	res.correlationID = id

	if err != nil {
		p.breaker.failure()
	} else {
		p.breaker.success()
	}

	switch {
	case err == nil:
		l.Info("poll", "source", p.name, "result", "success", "duration_ms", elapsed, "records", len(res.records)+len(res.warnings))
		p.handoff(ctx, res)
	case errors.Is(err, context.DeadlineExceeded):
		l.Warn("poll", "source", p.name, "result", "timeout", "duration_ms", elapsed, "error", err.Error())
	default:
		l.Warn("poll", "source", p.name, "result", "failure", "duration_ms", elapsed, "error", err.Error())
	}
}

// handoff sends a poll's records to ingest. A full channel makes it wait rather
// than drop: the cursor has moved past a dropped batch, which would never be
// fetched again. So the cursor moves only once the channel accepted the batch.
func (p *poller) handoff(ctx context.Context, res pollResult) {
	if len(res.records)+len(res.warnings) == 0 {
		return
	}

	select {
	case p.out <- res:
		if res.cursor.After(p.since) {
			p.since = res.cursor
		}
	case <-ctx.Done():
	}
}

// sourceClient makes the authenticated GETs for one source. Every call gets its
// own explicit timeout and is cancelled when it expires.
type sourceClient struct {
	baseURL string
	timeout time.Duration
	client  *http.Client
	// auth adds the source's own credential, or fails when it has none yet.
	auth func(*http.Request) error
}

// records GETs path (with ?since when set) and decodes a JSON array of objects.
func (c *sourceClient) records(ctx context.Context, path string, since time.Time) (_ []map[string]any, err error) {
	start := time.Now()
	defer func() { logCall(ctx, "GET "+c.baseURL+path, start, err) }()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	u := c.baseURL + path
	if !since.IsZero() {
		u += "?since=" + url.QueryEscape(since.UTC().Format(time.RFC3339Nano))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	if err := c.auth(req); err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s returned status %d", path, resp.StatusCode)
	}

	var out []map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxPollBody)).Decode(&out); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	return out, nil
}

// newest returns the latest RFC 3339 timestamp stored under key, or zero.
func newest(recs []map[string]any, key string) time.Time {
	var latest time.Time

	for _, r := range recs {
		s, _ := r[key].(string)
		if t, err := time.Parse(time.RFC3339, s); err == nil && t.After(latest) {
			latest = t
		}
	}

	return latest
}

// bmkgFetch polls BMKG: seismic events and tsunami warnings are separate
// endpoints, correlated later by the mapper (#17), so one poll reads both. The
// cursor follows the seismic events, which the warnings are filtered by.
func bmkgFetch(c *sourceClient) func(context.Context, time.Time) (pollResult, error) {
	return func(ctx context.Context, since time.Time) (pollResult, error) {
		events, err := c.records(ctx, "/seismic-events", since)
		if err != nil {
			return pollResult{}, err
		}

		warnings, err := c.records(ctx, "/tsunami-warnings", since)
		if err != nil {
			return pollResult{}, err
		}

		return pollResult{records: events, warnings: warnings, cursor: newest(events, "occurred_at")}, nil
	}
}

// pvmbgFetch polls PVMBG's volcanic reports.
// A failing token bootstrap or rotation (tokenErr) is a failed poll too: without
// a working token PVMBG cannot be polled, and the breaker has to see that.
func pvmbgFetch(c *sourceClient, tokenErr func() error) func(context.Context, time.Time) (pollResult, error) {
	return func(ctx context.Context, since time.Time) (pollResult, error) {
		if err := tokenErr(); err != nil {
			return pollResult{}, fmt.Errorf("PVMBG token: %w", err)
		}

		reports, err := c.records(ctx, "/volcanic-reports", since)
		if err != nil {
			return pollResult{}, err
		}

		return pollResult{records: reports, cursor: newest(reports, "reported_at")}, nil
	}
}

// bmkgAuth sends BMKG's static API key.
func bmkgAuth(key string) func(*http.Request) error {
	return func(r *http.Request) error {
		r.Header.Set("X-BMKG-Key", key)

		return nil
	}
}

// pvmbgAuth sends the current OAuth access token. With no token yet the call is
// not made at all and counts as a failed poll.
func pvmbgAuth(token func() string) func(*http.Request) error {
	return func(r *http.Request) error {
		t := token()
		if t == "" {
			return errors.New("no PVMBG access token yet")
		}

		r.Header.Set("Authorization", "Bearer "+t)

		return nil
	}
}

// pollConfig is everything newPollers needs, so its call site stays readable.
type pollConfig struct {
	interval, timeout time.Duration
	cbThreshold       int
	cbOpenFor         time.Duration
	bmkgURL, bmkgKey  string
	pvmbgURL          string
	token             func() string
	tokenErr          func() error
}

// newPollers builds the BMKG and PVMBG pollers, each with its own circuit breaker.
func newPollers(c pollConfig) []*poller {
	client := &http.Client{}

	bmkg := &sourceClient{baseURL: strings.TrimRight(c.bmkgURL, "/"), timeout: c.timeout, client: client, auth: bmkgAuth(c.bmkgKey)}
	pvmbg := &sourceClient{baseURL: strings.TrimRight(c.pvmbgURL, "/"), timeout: c.timeout, client: client, auth: pvmbgAuth(c.token)}

	return []*poller{
		{name: sourceBMKG, interval: c.interval, fetch: bmkgFetch(bmkg), breaker: newBreaker(sourceBMKG, c.cbThreshold, c.cbOpenFor)},
		{name: sourcePVMBG, interval: c.interval, fetch: pvmbgFetch(pvmbg, c.tokenErr), breaker: newBreaker(sourcePVMBG, c.cbThreshold, c.cbOpenFor)},
	}
}
