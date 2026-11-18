package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"
)

const (
	backoffStart = 500 * time.Millisecond
	backoffMax   = 10 * time.Second
)

// eventRecord is one line of the capture file: one SSE block or comment, as received.
type eventRecord struct {
	Wall    string   `json:"wall_rfc3339nano"`
	MonoNs  int64    `json:"mono_ns"`
	ConnSeq int      `json:"conn_seq"`
	SSEID   *string  `json:"sse_id"`
	Event   string   `json:"event"`
	Retry   *int64   `json:"retry"`
	DataRaw string   `json:"data_raw"`
	HasData bool     `json:"has_data"`
	Comment bool     `json:"comment,omitempty"`
	Unknown []string `json:"unknown_fields,omitempty"`
}

// connRecord is one line of the .conn.ndjson file: something that happened to a connection.
type connRecord struct {
	Wall        string            `json:"wall_rfc3339nano"`
	MonoNs      int64             `json:"mono_ns"`
	ConnSeq     int               `json:"conn_seq"`
	Kind        string            `json:"kind"`
	URL         string            `json:"url,omitempty"`
	LastEventID string            `json:"last_event_id,omitempty"`
	Status      int               `json:"status,omitempty"`
	Proto       string            `json:"proto,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Session     json.RawMessage   `json:"session,omitempty"`
	Body        string            `json:"body,omitempty"`
	Reason      string            `json:"reason,omitempty"`
	WaitMs      int64             `json:"wait_ms,omitempty"`
	Events      int               `json:"events,omitempty"`
}

// Kinds of connRecord.
const (
	kindSession    = "session"
	kindConnect    = "connect"
	kindResponse   = "response"
	kindDisconnect = "disconnect"
	kindWait       = "reconnect_wait"
	kindPauseStart = "pause_start"
	kindPauseEnd   = "pause_end"
	kindLive       = "live_reached"
	kindStop       = "stop"
)

// errLiveTailDone ends a capture once --live-tail has run after replay caught up.
var errLiveTailDone = errors.New("live tail done")

type captureOpts struct {
	baseURL         string
	from            string
	lastEventID     string
	out             string
	duration        time.Duration
	disconnectAt    time.Duration
	pauseAt         time.Duration
	pauseFor        time.Duration
	retryFixed      time.Duration
	liveTail        time.Duration
	lastEventIDOnly bool
	once            bool
	badToken        bool
}

func parseCaptureFlags(args []string) (captureOpts, error) {
	var o captureOpts

	fs := flag.NewFlagSet("capture", flag.ContinueOnError)
	fs.StringVar(&o.baseURL, "base-url", defaultBaseURL, "feed base URL")
	fs.StringVar(&o.from, "from", "", `first offset: a number, "latest" (latest_offset from /v1/session), or empty to omit ?from`)
	fs.StringVar(&o.lastEventID, "last-event-id", "", "Last-Event-ID header for the first connection")
	fs.StringVar(&o.out, "out", "", "capture file (NDJSON); connection log goes to <name>.conn.ndjson")
	fs.DurationVar(&o.duration, "duration", 0, "total capture time")
	fs.DurationVar(&o.disconnectAt, "disconnect-at", 0, "close the connection once at this time, then resume")
	fs.DurationVar(&o.pauseAt, "pause-at", 0, "stop reading the socket at this time")
	fs.DurationVar(&o.pauseFor, "pause-for", 0, "how long to stop reading")
	fs.DurationVar(&o.retryFixed, "retry-fixed", 0, "retry at this fixed interval instead of exponential backoff")
	fs.BoolVar(&o.lastEventIDOnly, "last-event-id-only", false, "resume with the Last-Event-ID header only (default: ?from=<last id> only)")
	fs.BoolVar(&o.once, "once", false, "make exactly one connection attempt")
	fs.BoolVar(&o.badToken, "bad-token", false, "send an invalid token to observe the 401 answer")
	fs.DurationVar(&o.liveTail, "live-tail", 0, "stop this long after reaching the latest_offset seen at connect (replay caught up)")

	if err := fs.Parse(args); err != nil {
		return o, err
	}

	switch {
	case o.out == "":
		return o, errors.New("--out is required")
	case o.duration <= 0:
		return o, errors.New("--duration must be positive")
	case (o.pauseAt > 0) != (o.pauseFor > 0):
		return o, errors.New("--pause-at and --pause-for go together")
	}

	return o, nil
}

func runCapture(args []string) (err error) {
	opts, err := parseCaptureFlags(args)
	if err != nil {
		return err
	}

	client, err := newFeedClient(opts.baseURL, opts.badToken)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(opts.out), 0o750); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	events, err := os.Create(opts.out)
	if err != nil {
		return fmt.Errorf("create capture file: %w", err)
	}

	defer func() { err = errors.Join(err, events.Close()) }()

	conns, err := os.Create(connPath(opts.out))
	if err != nil {
		return fmt.Errorf("create connection log: %w", err)
	}

	defer func() { err = errors.Join(err, conns.Close()) }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	cr := &captureRun{opts: opts, client: client, start: time.Now(), events: json.NewEncoder(events), conns: json.NewEncoder(conns)}

	ctx, cancel := context.WithDeadline(ctx, cr.start.Add(opts.duration))
	defer cancel()

	return cr.run(ctx)
}

// connPath is the connection log that sits next to a capture file.
func connPath(capture string) string {
	return strings.TrimSuffix(capture, ".ndjson") + ".conn.ndjson"
}

type captureRun struct {
	opts           captureOpts
	client         *feedClient
	start          time.Time
	events         *json.Encoder
	conns          *json.Encoder
	connSeq        int
	lastID         string
	disconnectDone bool
	pauseDone      bool
	latestAtConn   int64 // latest_offset from /v1/session just before this connection
	liveAt         time.Time
	finished       bool
}

type connResult struct {
	retryAfter time.Duration
	events     int
	forced     bool // closed on purpose by --disconnect-at
}

func (cr *captureRun) run(ctx context.Context) error {
	from, lastEventID := cr.opts.from, cr.opts.lastEventID
	backoff := backoffStart

	if from == "latest" {
		latest, err := cr.latestOffset(ctx)
		if err != nil {
			return err
		}

		from = latest
	}

	for attempt := 0; cr.keepGoing(ctx, attempt); attempt++ {
		res, err := cr.connect(ctx, from, lastEventID)
		if err != nil {
			return err
		}

		if cr.lastID != "" {
			from, lastEventID = cr.resumeParams()
		}

		if res.forced || !cr.keepGoing(ctx, attempt+1) {
			continue
		}

		wait := cr.nextWait(res, &backoff)
		if err := cr.logConn(connRecord{Kind: kindWait, WaitMs: wait.Milliseconds()}); err != nil {
			return err
		}

		sleepCtx(ctx, wait)
	}

	return cr.logConn(connRecord{Kind: kindStop, Reason: "capture finished"})
}

// keepGoing reports whether another connection attempt is allowed.
func (cr *captureRun) keepGoing(ctx context.Context, attempt int) bool {
	return ctx.Err() == nil && !cr.finished && (!cr.opts.once || attempt == 0)
}

// resumeParams continues after the last id seen: ?from=<id> by default, or only the
// Last-Event-ID header with --last-event-id-only, so each mechanism is observed on its own.
func (cr *captureRun) resumeParams() (from, lastEventID string) {
	if cr.opts.lastEventIDOnly {
		return "", cr.lastID
	}

	return cr.lastID, ""
}

// nextWait is the delay before reconnecting: fixed with --retry-fixed, otherwise
// exponential from 0.5 s to 10 s, never shorter than the server's Retry-After.
func (cr *captureRun) nextWait(res connResult, backoff *time.Duration) time.Duration {
	if cr.opts.retryFixed > 0 {
		return cr.opts.retryFixed
	}

	if res.events > 0 {
		*backoff = backoffStart
	}

	wait := max(*backoff, res.retryAfter)
	*backoff = min(*backoff*2, backoffMax)

	return wait
}

// latestOffset resolves --from latest from /v1/session (the stream itself is only given a number).
func (cr *captureRun) latestOffset(ctx context.Context) (string, error) {
	body, err := cr.logSession(ctx)
	if err != nil {
		return "", err
	}

	snap, ok := parseSnapshot(body)
	if !ok || snap.latestOffset == "" {
		return "", errors.New("/v1/session has no latest_offset; pass a number to --from")
	}

	return snap.latestOffset, nil
}

// connect makes one stream connection and reads it until it ends. Only write errors are returned.
func (cr *captureRun) connect(ctx context.Context, from, lastEventID string) (connResult, error) {
	cr.connSeq++

	body, err := cr.logSession(ctx)
	if err != nil {
		return connResult{}, err
	}

	cr.latestAtConn = latestOffsetIn(body)

	connCtx, cancel := cr.connContext(ctx)
	defer cancel()

	query := url.Values{}
	if from != "" {
		query.Set("from", from)
	}

	req, err := cr.client.request(connCtx, "/v1/stream", query, lastEventID)
	if err != nil {
		return connResult{}, err
	}

	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	if err := cr.logConn(connRecord{Kind: kindConnect, URL: req.URL.RequestURI(), LastEventID: lastEventID}); err != nil {
		return connResult{}, err
	}

	resp, err := cr.client.http.Do(req)
	if err != nil {
		return cr.ended(ctx, connCtx, connResult{}, err)
	}

	defer func() { _ = resp.Body.Close() }() // the stream is already over; nothing to recover

	res := connResult{retryAfter: retryAfter(resp.Header, time.Now())}
	rec := connRecord{Kind: kindResponse, Status: resp.StatusCode, Proto: resp.Proto, Headers: safeHeaders(resp.Header)}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
		rec.Body = string(body)

		return res, cr.logConn(rec)
	}

	if err := cr.logConn(rec); err != nil {
		return res, err
	}

	res.events, err = cr.readStream(connCtx, resp.Body)

	return cr.ended(ctx, connCtx, res, err)
}

// latestOffsetIn reads latest_offset from a /v1/session body; -1 when absent.
func latestOffsetIn(body []byte) int64 {
	snap, ok := parseSnapshot(body)
	if !ok {
		return -1
	}

	if n, ok := parseOffset(snap.latestOffset); ok {
		return n
	}

	return -1
}

// connContext ends the connection at --disconnect-at, once per capture.
func (cr *captureRun) connContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if cr.opts.disconnectAt > 0 && !cr.disconnectDone {
		return context.WithDeadline(ctx, cr.start.Add(cr.opts.disconnectAt))
	}

	return context.WithCancel(ctx)
}

// ended logs why a connection stopped and marks a --disconnect-at close as forced.
func (cr *captureRun) ended(ctx, connCtx context.Context, res connResult, err error) (connResult, error) {
	if _, ok := errors.AsType[*recordWriteError](err); ok {
		return res, err
	}

	reason := "unknown"

	switch {
	case errors.Is(err, errLiveTailDone):
		reason = "live tail done"
		cr.finished = true
	case ctx.Err() != nil:
		reason = "capture duration reached"
	case connCtx.Err() != nil:
		reason = "closed by --disconnect-at"
		res.forced = true
		cr.disconnectDone = true
	case errors.Is(err, io.EOF):
		reason = "server closed the stream (EOF)"
	case err != nil:
		reason = err.Error()
	}

	return res, cr.logConn(connRecord{Kind: kindDisconnect, Reason: reason, Events: res.events, LastEventID: cr.lastID})
}

type recordWriteError struct{ err error }

func (e *recordWriteError) Error() string { return "write record: " + e.err.Error() }
func (e *recordWriteError) Unwrap() error { return e.err }

func (cr *captureRun) readStream(ctx context.Context, body io.Reader) (int, error) {
	r := newSSEReader(body)
	n := 0

	for {
		if err := cr.maybePause(ctx); err != nil {
			return n, err
		}

		ev, err := r.Next()
		if err != nil {
			return n, err
		}

		n++

		if ev.ID != nil {
			cr.lastID = *ev.ID
		}

		wall, mono := cr.stamp()
		rec := eventRecord{
			Wall: wall, MonoNs: mono, ConnSeq: cr.connSeq, SSEID: ev.ID, Event: ev.Event,
			Retry: ev.Retry, DataRaw: ev.Data, HasData: ev.HasData, Comment: ev.Comment, Unknown: ev.Unknown,
		}

		if err := cr.events.Encode(rec); err != nil {
			return n, &recordWriteError{err}
		}

		if err := cr.checkLive(ev); err != nil {
			return n, err
		}
	}
}

// checkLive marks when replay reaches the offset that was latest at connect, and ends
// the capture --live-tail later.
func (cr *captureRun) checkLive(ev sseEvent) error {
	if cr.opts.liveTail <= 0 {
		return nil
	}

	if cr.liveAt.IsZero() && ev.ID != nil && cr.latestAtConn >= 0 {
		if n, ok := parseOffset(*ev.ID); ok && n >= cr.latestAtConn {
			cr.liveAt = time.Now()

			if err := cr.logConn(connRecord{Kind: kindLive, LastEventID: *ev.ID}); err != nil {
				return err
			}
		}
	}

	if !cr.liveAt.IsZero() && time.Since(cr.liveAt) >= cr.opts.liveTail {
		return errLiveTailDone
	}

	return nil
}

// maybePause stops reading the socket once, keeping the connection open, to see what a slow reader gets.
func (cr *captureRun) maybePause(ctx context.Context) error {
	if cr.pauseDone || cr.opts.pauseAt <= 0 || time.Since(cr.start) < cr.opts.pauseAt {
		return nil
	}

	cr.pauseDone = true

	if err := cr.logConn(connRecord{Kind: kindPauseStart, WaitMs: cr.opts.pauseFor.Milliseconds()}); err != nil {
		return err
	}

	sleepCtx(ctx, cr.opts.pauseFor)

	return cr.logConn(connRecord{Kind: kindPauseEnd})
}

// logSession records /v1/session next to each connection, for the replay/live boundary and the clock.
func (cr *captureRun) logSession(ctx context.Context) ([]byte, error) {
	status, body, err := cr.client.session(ctx)
	rec := connRecord{Kind: kindSession, Status: status}
	setBody(&rec.Session, &rec.Body, body)

	if err != nil {
		rec.Reason = err.Error()
	}

	return body, cr.logConn(rec)
}

func (cr *captureRun) logConn(rec connRecord) error {
	rec.Wall, rec.MonoNs = cr.stamp()
	rec.ConnSeq = cr.connSeq

	if err := cr.conns.Encode(rec); err != nil {
		return &recordWriteError{err}
	}

	return nil
}

func (cr *captureRun) stamp() (string, int64) {
	now := time.Now()

	return now.UTC().Format(time.RFC3339Nano), now.Sub(cr.start).Nanoseconds()
}
