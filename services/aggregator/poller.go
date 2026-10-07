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

// hasil satu poll cycle dari satu source: records, warnings (BMKG), cursor buat ?since= berikutnya, correlationID, dan span trace-nya.
type pollResult struct {
	records, warnings []map[string]any
	cursor            time.Time
	correlationID     string
	trace             trace.SpanContext
}

// loop polling satu source, satu goroutine satu call jadi nggak mungkin ada dua call bareng.
type poller struct {
	name     string
	interval time.Duration
	fetch    func(ctx context.Context, since time.Time) (pollResult, error)
	breaker  *breaker
	out      chan<- pollResult

	// since cuma disentuh goroutine run.
	// ponytail: cursor ada di memory, restart baca ulang semuanya sekali (upsert dedup bikin aman) dan mengasumsikan timestamp source nggak mundur.
	since time.Time
}

// sisa interval setelah call, atau nol kalau call sudah memakan semuanya.
func nextWait(interval, elapsed time.Duration) time.Duration {
	return max(interval-elapsed, 0)
}

// polling sampai ctx selesai lalu tutup out.
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

// satu call, log hasilnya, serahkan record. Error apa pun termasuk timeout dihitung gagal dan cursor baru maju setelah record diserahkan.
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

// kirim record ke ingest dan menunggu kalau channel penuh, karena batch yang dibuang nggak akan diambil lagi.
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

// GET terautentikasi per source dengan timeout eksplisit tiap call.
type sourceClient struct {
	baseURL string
	timeout time.Duration
	client  *http.Client
	auth    func(*http.Request) error
}

// GET path (dengan ?since kalau ada) dan decode JSON array of object.
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
		return nil, fmt.Errorf("%s returned status %d", path, resp.StatusCode)
	}

	var out []map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxPollBody)).Decode(&out); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	return out, nil
}

// timestamp RFC 3339 terbaru di key, atau nol.
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

// poll BMKG: seismik dan tsunami warning dua endpoint, dibaca bareng. Cursor mengikuti seismik.
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

// poll laporan vulkanik PVMBG, gagal token (tokenErr) dihitung poll gagal.
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

// kirim API key statis BMKG.
func bmkgAuth(key string) func(*http.Request) error {
	return func(r *http.Request) error {
		r.Header.Set("X-BMKG-Key", key)

		return nil
	}
}

// kirim access token OAuth saat ini, belum ada token berarti call nggak dibuat dan dihitung gagal.
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

// semua yang dibutuhkan newPollers.
type pollConfig struct {
	interval, timeout time.Duration
	cbThreshold       int
	cbOpenFor         time.Duration
	bmkgURL, bmkgKey  string
	pvmbgURL          string
	token             func() string
	tokenErr          func() error
}

// bangun poller BMKG dan PVMBG, masing-masing dengan circuit breaker sendiri.
func newPollers(c pollConfig) []*poller {
	client := &http.Client{}

	bmkg := &sourceClient{baseURL: strings.TrimRight(c.bmkgURL, "/"), timeout: c.timeout, client: client, auth: bmkgAuth(c.bmkgKey)}
	pvmbg := &sourceClient{baseURL: strings.TrimRight(c.pvmbgURL, "/"), timeout: c.timeout, client: client, auth: pvmbgAuth(c.token)}

	return []*poller{
		{name: sourceBMKG, interval: c.interval, fetch: bmkgFetch(bmkg), breaker: newBreaker(sourceBMKG, c.cbThreshold, c.cbOpenFor)},
		{name: sourcePVMBG, interval: c.interval, fetch: pvmbgFetch(pvmbg, c.tokenErr), breaker: newBreaker(sourcePVMBG, c.cbThreshold, c.cbOpenFor)},
	}
}
