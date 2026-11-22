package main

import (
	"context"
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
	// Each entry of records as received, byte for byte. Nil when unknown.
	rawRecords [][]byte
	// Records and stretches of body that were set aside before mapping (bad JSON, too deep, bad UTF-8, too large).
	rejected      []rejectedRaw
	cursor        time.Time
	correlationID string
	trace         trace.SpanContext

	// Sensor Pantai only: each record as received (parallel to records, a nil record is an item that was not a JSON
	// object), the sensor's clock for the response and the cursor that follows this batch, empty when it did not move.
	raws        [][]byte
	generatedAt time.Time
	nextCursor  string
	gen         uint64
	slow        bool // some call answered 200 but over the slow limit

	// Fencing token of the lease term that fetched this batch, 0 when no lease is involved.
	epoch int64
}

// One source's polling loop. One goroutine, one call at a time, so there are never two calls in flight for a source.
type poller struct {
	name     string
	interval time.Duration
	fetch    func(ctx context.Context, since time.Time) (pollResult, error)
	breaker  *breaker
	out      chan<- pollResult

	// loadSince reads the stored cursor. Set, it is read at the start of every cycle and a new lease term starts from it.
	loadSince func(context.Context) (time.Time, error)

	// Runs at the start of every lease term.
	onTerm func()

	// Optional. backoff stretches the wait after failed polls, onReject runs when the breaker turns a poll away and onWait gets
	// the extra delay in use.
	backoff  *backoff
	onReject func()
	onWait   func(time.Duration)

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

	p.loop(ctx)
}

// Polls until ctx is done and leaves out open, so a later lease term can use it again.
func (p *poller) loop(ctx context.Context) {
	if p.onTerm != nil {
		p.onTerm()
	}

	if p.loadSince != nil {
		p.since = time.Time{}
	}

	if p.backoff != nil {
		p.backoff.reset()
	}

	for {
		start := time.Now()
		out := p.pollOnce(ctx)
		wait := nextWait(p.interval, time.Since(start))

		switch out {
		case pollFailed:
			if p.backoff != nil {
				wait = max(wait, p.backoff.failed())
			}
		case pollOK:
			if p.backoff != nil {
				p.backoff.reset()
			}
		case pollSkipped:
		}

		if p.backoff != nil && p.onWait != nil {
			p.onWait(p.backoff.last)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

type pollOutcome int

const (
	pollOK pollOutcome = iota
	pollFailed
	pollSkipped // the breaker did not let the poll through
)

// One call: log the result, hand over the records. Any error, timeout included, is a failed poll, and the cursor only moves after the hand-over.
func (p *poller) pollOnce(ctx context.Context) pollOutcome {
	if !p.breaker.allow() {
		if p.onReject != nil {
			p.onReject()
		}

		return pollSkipped
	}

	ctx, span := tracer.Start(ctx, "poll "+p.name)
	defer span.End()

	id := traceIDOf(ctx)
	if id == "" {
		id = newHazardID()
	}

	l := withID(id)

	if p.loadSince != nil {
		if t, err := p.loadSince(ctx); err != nil {
			l.Warn("poll cursor read failed", "source", p.name, "error", err.Error())
		} else if t.After(p.since) {
			p.since = t
		}
	}

	start := time.Now()
	res, err := p.fetch(withCorrelation(ctx, id), p.since)
	elapsed := time.Since(start).Milliseconds()
	res.correlationID = id
	res.trace = span.SpanContext()
	res.epoch, _ = leaseEpoch(ctx)

	outcome := pollOK

	switch {
	case err != nil:
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.breaker.failure()

		outcome = pollFailed
	case res.slow:
		// The data is good and is kept, but a slow sensor counts against the breaker.
		l.Warn("poll slow", "source", p.name, "duration_ms", elapsed)
		p.breaker.failure()

		outcome = pollFailed
	default:
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

	if err != nil && len(res.rejected) > 0 {
		// A garbage body fails the poll, but it is still kept as a failed message.
		p.handoff(ctx, pollResult{rejected: res.rejected, correlationID: id, trace: res.trace, epoch: res.epoch})
	}

	return outcome
}

// Sends records to ingest and waits if the channel is full, since a dropped batch would never be fetched again.
func (p *poller) handoff(ctx context.Context, res pollResult) {
	if len(res.records)+len(res.warnings)+len(res.rejected) == 0 {
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
	// Overrides maxPollBody when set.
	maxBody int
}

// GETs path (with ?since if set) and decodes a JSON array of objects.
func (c *sourceClient) records(ctx context.Context, path string, since time.Time) ([]map[string]any, error) {
	recs, _, _, err := c.recordsRaw(ctx, path, since)

	return recs, err
}

// Like records, plus each element's bytes as received and the elements that were set aside. A bad element does not fail the call,
// only a body that is not an array at all does (the body then comes back as the one rejected stretch).
func (c *sourceClient) recordsRaw(ctx context.Context, path string, since time.Time) ([]map[string]any, [][]byte, []rejectedRaw, error) {
	query := ""
	if !since.IsZero() {
		query = "since=" + url.QueryEscape(since.UTC().Format(time.RFC3339Nano))
	}

	var (
		out  []map[string]any
		raws [][]byte
		bad  []rejectedRaw
	)

	err := c.get(ctx, path, query, func(r io.Reader) error {
		body, cut, err := readBody(r, c.bodyCap())
		if err != nil {
			return err
		}

		out, raws, bad, err = decodeRecords(body, cut)

		return err
	})
	if err != nil {
		return nil, nil, bad, err
	}

	return out, raws, bad, nil
}

// The most a poll body may be, 8 MiB unless a test sets less.
func (c *sourceClient) bodyCap() int {
	if c.maxBody > 0 {
		return c.maxBody
	}

	return maxPollBody
}

// GETs path with a raw query string and hands the body (one byte over the cap, so a decoder can tell a cut) to decode. Any status but 200 is an error.
func (c *sourceClient) get(ctx context.Context, path, query string, decode func(io.Reader) error) (err error) {
	start := time.Now()
	ctx = startCall(ctx, "GET "+c.baseURL+path)

	defer func() { logCall(ctx, "GET "+c.baseURL+path, start, err) }()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	u := c.baseURL + path
	if query != "" {
		u += "?" + query
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	if id := correlationOf(ctx); id != "" {
		req.Header.Set("X-Correlation-ID", id)
	}

	if err := c.auth(req); err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusUnauthorized && c.onUnauthorized != nil {
			c.onUnauthorized()
		}

		return fmt.Errorf("%s returned status %d", path, resp.StatusCode)
	}

	if err := decode(io.LimitReader(resp.Body, int64(c.bodyCap())+1)); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}

	return nil
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
		events, raws, bad, err := c.recordsRaw(ctx, "/seismic-events", since)
		if err != nil {
			return pollResult{rejected: bad}, err
		}

		warnings, _, wbad, err := c.recordsRaw(ctx, "/tsunami-warnings", since)
		if err != nil {
			return pollResult{rejected: append(bad, wbad...)}, err
		}

		return pollResult{records: events, rawRecords: raws, warnings: warnings, rejected: append(bad, wbad...), cursor: newest(events, "occurred_at")}, nil
	}
}

// Polls PVMBG volcanic reports. A token failure (tokenErr) counts as a failed poll.
func pvmbgFetch(c *sourceClient, tokenErr func() error) func(context.Context, time.Time) (pollResult, error) {
	return func(ctx context.Context, since time.Time) (pollResult, error) {
		if err := tokenErr(); err != nil {
			return pollResult{}, fmt.Errorf("PVMBG token: %w", err)
		}

		reports, raws, bad, err := c.recordsRaw(ctx, "/volcanic-reports", since)
		if err != nil {
			return pollResult{rejected: bad}, err
		}

		return pollResult{records: reports, rawRecords: raws, rejected: bad, cursor: newest(reports, "reported_at")}, nil
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
	pantaiURL         string // empty leaves Sensor Pantai off
	pantaiToken       string
	sensor            sensorConfig
	pantai            *sensorClient // built in run once the store is open
	adjust            schemaAdjust
}

// Builds the BMKG and PVMBG pollers, and Sensor Pantai's when configured, each with its own circuit breaker.
func newPollers(c pollConfig) []*poller {
	client := &http.Client{}

	bmkg := &sourceClient{baseURL: strings.TrimRight(c.bmkgURL, "/"), timeout: c.timeout, client: client, auth: bmkgAuth(c.bmkgKey)}
	pvmbg := &sourceClient{baseURL: strings.TrimRight(c.pvmbgURL, "/"), timeout: c.timeout, client: client, auth: pvmbgAuth(c.token), onUnauthorized: c.rejectToken}

	out := []*poller{
		{name: sourceBMKG, interval: c.interval, fetch: bmkgFetch(bmkg), breaker: newBreaker(sourceBMKG, c.cbThreshold, c.cbOpenFor)},
		{name: sourcePVMBG, interval: c.interval, fetch: pvmbgFetch(pvmbg, c.tokenErr), breaker: newBreaker(sourcePVMBG, c.cbThreshold, c.cbOpenFor)},
	}

	if c.pantai != nil {
		sc := c.sensor.orDefaults()
		br := newBreaker(sourceSensorPantai, sc.cbThreshold, sc.cbOpenFor)

		if c.pantai.stats == nil {
			c.pantai.stats = &sensorStats{}
		}

		st := c.pantai.stats
		st.breaker = br
		c.pantai.staleAfter = sc.staleAfter
		c.pantai.onStale = func(stale bool, since time.Time) {
			br.setStale(stale, since)
			st.setStale(stale)
		}

		out = append(out, &poller{
			name: sourceSensorPantai, interval: c.interval, fetch: c.pantai.fetch, onTerm: c.pantai.newTerm, breaker: br,
			backoff: newBackoff(sc.backoffBase, sc.backoffMax), onReject: func() { st.breakerRejected() }, onWait: st.setBackoff,
		})
	}

	return out
}
