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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultPollInterval = 3 * time.Second
	defaultPollTimeout  = 4 * time.Second
	maxPollBody         = 8 << 20
)

// Result of one poll cycle from a source: records, warnings (BMKG), the cursor for the next ?since=, correlationID and the cycle's trace span.
type pollResult struct {
	records, warnings []map[string]any
	cursor            time.Time
	correlationID     string
	trace             trace.SpanContext
}

// One source's polling loop. One goroutine, one call at a time, so there are never two calls in flight for a source.
type poller struct {
	name     string
	interval time.Duration
	fetch    func(ctx context.Context, since time.Time) (pollResult, error)
	breaker  *breaker
	out      chan<- pollResult

	// since is only touched by the run goroutine.
	// ponytail: the cursor lives in memory, so a restart re-reads everything once (upsert dedup makes that safe). Assumes source timestamps never go backwards.
	since time.Time
}

// Time left in the interval after a call, or zero if the call used it all.
func nextWait(interval, elapsed time.Duration) time.Duration {
	return max(interval-elapsed, 0)
}

// Polls until ctx is done, then closes out.
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

// One call: log the result, hand over the records. Any error, timeout included, is a failed poll, and the cursor only moves after the hand-over.
func (p *poller) pollOnce(ctx context.Context) {
	if !p.breaker.allow() {
		return
	}

	ctx, span := tracer.Start(ctx, "poll "+p.name)
	defer span.End()

	id := traceIDOf(ctx)
	if id == "" {
		id = newHazardID()
	}

	l := withID(id)

	start := time.Now()
	res, err := p.fetch(withCorrelation(ctx, id), p.since)
	elapsed := time.Since(start).Milliseconds()
	res.correlationID = id
	res.trace = span.SpanContext()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
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

// Sends records to ingest and waits if the channel is full, since a dropped batch would never be fetched again.
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

// Authenticated GETs for one source, each with its own timeout.
type sourceClient struct {
	baseURL string
	timeout time.Duration
	client  *http.Client
	auth    func(*http.Request) error
	// Runs on a 401, the credential is no longer good.
	onUnauthorized func()
}

// GETs path (with ?since if set) and decodes a JSON array of objects.
func (c *sourceClient) records(ctx context.Context, path string, since time.Time) (_ []map[string]any, err error) {
	start := time.Now()
	ctx = startCall(ctx, "GET "+c.baseURL+path)

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

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	if id := correlationOf(ctx); id != "" {
		req.Header.Set("X-Correlation-ID", id)
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
		if resp.StatusCode == http.StatusUnauthorized && c.onUnauthorized != nil {
			c.onUnauthorized()
		}

		return nil, fmt.Errorf("%s returned status %d", path, resp.StatusCode)
	}

	var out []map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxPollBody)).Decode(&out); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	return out, nil
}

// Latest RFC 3339 timestamp under key, or zero.
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

// Polls BMKG: seismic events and tsunami warnings are two endpoints read together. The cursor follows the seismic events.
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

// Polls PVMBG volcanic reports. A token failure (tokenErr) counts as a failed poll.
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

// Sends BMKG's static API key.
func bmkgAuth(key string) func(*http.Request) error {
	return func(r *http.Request) error {
		r.Header.Set("X-BMKG-Key", key)

		return nil
	}
}

// Sends the current OAuth access token. No token yet means no call, counted as a failed poll.
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

// Everything newPollers needs.
type pollConfig struct {
	interval, timeout time.Duration
	cbThreshold       int
	cbOpenFor         time.Duration
	bmkgURL, bmkgKey  string
	pvmbgURL          string
	token             func() string
	tokenErr          func() error
	rejectToken       func() // PVMBG answered 401: renew the token now
}

// Builds the BMKG and PVMBG pollers, each with its own circuit breaker.
func newPollers(c pollConfig) []*poller {
	client := &http.Client{}

	bmkg := &sourceClient{baseURL: strings.TrimRight(c.bmkgURL, "/"), timeout: c.timeout, client: client, auth: bmkgAuth(c.bmkgKey)}
	pvmbg := &sourceClient{baseURL: strings.TrimRight(c.pvmbgURL, "/"), timeout: c.timeout, client: client, auth: pvmbgAuth(c.token), onUnauthorized: c.rejectToken}

	return []*poller{
		{name: sourceBMKG, interval: c.interval, fetch: bmkgFetch(bmkg), breaker: newBreaker(sourceBMKG, c.cbThreshold, c.cbOpenFor)},
		{name: sourcePVMBG, interval: c.interval, fetch: pvmbgFetch(pvmbg, c.tokenErr), breaker: newBreaker(sourcePVMBG, c.cbThreshold, c.cbOpenFor)},
	}
}
