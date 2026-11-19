package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // see uuidV5
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	reportQueue      = "field-reports"
	maxReportBody    = 8 << 10
	maxDescription   = 4096
	maxKind          = 64
	confirmTimeout   = 2 * time.Second
	dedupeTTL        = 10 * time.Minute
	dedupeMaxEntries = 10000
)

var (
	errBrokerDown = errors.New("broker not connected")
	errNacked     = errors.New("broker nacked the message")
	errUnroutable = errors.New("broker returned the message, queue is gone")
	validKey      = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	// Upper bounds in seconds for the confirm latency histogram.
	confirmBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2}
)

// What the API puts on the queue.
type reportEnvelope struct {
	ReportID     string   `json:"report_id"`
	Description  string   `json:"description"`
	Latitude     *float64 `json:"latitude,omitempty"`
	Longitude    *float64 `json:"longitude,omitempty"`
	Kind         string   `json:"kind,omitempty"`
	ReporterID   string   `json:"reporter_id"`
	ReporterRole string   `json:"reporter_role"`
	ReceivedAt   string   `json:"received_at"`
}

type reportMsg struct {
	id, correlationID string
	body              []byte
	trace             map[string]any
	at                time.Time
}

// Hands a message to the broker and returns only after the broker confirmed it.
type reportBroker interface {
	publish(ctx context.Context, m reportMsg) error
}

type seenReport struct {
	sum [sha256.Size]byte
	at  time.Time
}

// POST /field-reports: validates, puts the report on the durable queue and answers 202 only after the broker confirmed the write. Per pod, a repeated Idempotency-Key with the same body is answered from a small cache without publishing again.
type fieldReportsAPI struct {
	broker reportBroker
	now    func() time.Time

	mu   sync.Mutex
	seen map[string]seenReport

	accepted, deduped, unavailable atomic.Int64
	buckets                        [10]atomic.Int64 // one per confirmBuckets entry plus +Inf, not cumulative
	sumMicros                      atomic.Int64
}

func newFieldReportsAPI(b reportBroker) *fieldReportsAPI {
	return &fieldReportsAPI{broker: b, now: time.Now, seen: map[string]seenReport{}}
}

func newReportID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// Fixed namespace for report ids. Changing it changes every keyed report id.
var reportNamespace = [16]byte{0x3a, 0x91, 0x5c, 0x0d, 0x7e, 0x42, 0x4b, 0x16, 0x9f, 0x28, 0xc4, 0x6d, 0x10, 0xe7, 0x5b, 0xa3}

// RFC 4122 UUID v5 of the name, so a retry with the same key gets the same id on every pod. Same code as the aggregator's hazard ids, the modules are separate.
func uuidV5(ns [16]byte, name string) string {
	h := sha1.New() //nolint:gosec // UUID v5 is defined with SHA-1, it is an id and not a security hash

	_, _ = h.Write(ns[:])
	_, _ = h.Write([]byte(name))

	b := h.Sum(nil)[:16]
	b[6] = b[6]&0x0f | 0x50
	b[8] = b[8]&0x3f | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// True when any text holds a NUL. It is valid JSON and valid UTF-8, but Postgres refuses it in text, so the report could never be saved.
func hasNUL(texts ...string) bool {
	return slices.ContainsFunc(texts, func(s string) bool { return strings.ContainsRune(s, 0) })
}

func outOfRange(in reportEnvelope) bool {
	return in.Latitude != nil && in.Longitude != nil && (*in.Latitude < -90 || *in.Latitude > 90 || *in.Longitude < -180 || *in.Longitude > 180)
}

// Decodes and checks the body, nothing is touched before it is valid.
func parseReport(r *http.Request, w http.ResponseWriter) (reportEnvelope, []byte, string) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxReportBody))
	if err != nil {
		return reportEnvelope{}, nil, "body too large or unreadable"
	}

	var in reportEnvelope

	if !utf8.Valid(raw) {
		return in, nil, "body must be valid UTF-8"
	}

	if err := json.Unmarshal(raw, &in); err != nil {
		return in, nil, "invalid JSON"
	}

	switch {
	case hasNUL(in.Description, in.Kind):
		return in, nil, "text must not contain a NUL character"
	case strings.TrimSpace(in.Description) == "":
		return in, nil, "description is required"
	case len(in.Description) > maxDescription:
		return in, nil, "description is longer than 4096 bytes"
	case (in.Latitude == nil) != (in.Longitude == nil):
		return in, nil, "latitude and longitude go together"
	case outOfRange(in):
		return in, nil, "latitude or longitude out of range"
	case utf8.RuneCountInString(in.Kind) > maxKind:
		return in, nil, "kind is longer than 64 characters"
	}

	return in, raw, ""
}

func (a *fieldReportsAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	claims, _ := claimsFrom(r.Context())
	if claims.Role != roleLapangan && claims.Role != roleOperator {
		writeJSON(w, http.StatusForbidden, errBody("field reports are for Lapangan and Operator"))

		return
	}

	in, raw, problem := parseReport(r, w)
	if problem != "" {
		writeJSON(w, http.StatusBadRequest, errBody(problem))

		return
	}

	id, scope := newReportID(), ""

	if key := r.Header.Get("Idempotency-Key"); key != "" {
		if !validKey.MatchString(key) {
			writeJSON(w, http.StatusBadRequest, errBody("Idempotency-Key must be 1 to 64 characters of A-Z a-z 0-9 . _ -"))

			return
		}

		scope = claims.Email + "|" + key
		id = uuidV5(reportNamespace, scope)
	}

	sum := sha256.Sum256(raw)

	if dup, conflict := a.lookup(scope, sum); scope != "" && conflict {
		writeJSON(w, http.StatusConflict, errBody("Idempotency-Key was used with a different body"))

		return
	} else if scope != "" && dup {
		a.deduped.Add(1)
		writeJSON(w, http.StatusAccepted, map[string]string{"report_id": id})

		return
	}

	at := a.now().UTC()
	in.ReportID, in.ReporterID, in.ReporterRole, in.ReceivedAt = id, claims.Email, claims.Role, at.Format(time.RFC3339Nano)
	body, _ := json.Marshal(in)
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(r.Context(), carrier)

	trace := map[string]any{}
	for k, v := range carrier {
		trace[k] = v
	}

	ctx, cancel := context.WithTimeout(r.Context(), confirmTimeout)
	defer cancel()

	start := a.now()

	err := a.broker.publish(ctx, reportMsg{id: id, correlationID: w.Header().Get(correlationHeader), body: body, trace: trace, at: at})
	if err != nil {
		a.unavailable.Add(1)
		logFor(r.Context()).Warn("field-report", "report_id", id, "result", "failure", "error", err.Error())
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, errBody("report not stored, retry with the same Idempotency-Key"))

		return
	}

	a.observe(a.now().Sub(start))
	if scope != "" {
		a.remember(scope, sum)
	}

	a.accepted.Add(1)
	writeJSON(w, http.StatusAccepted, map[string]string{"report_id": id})
}

// Whether id was already confirmed with this same body (dup) or with another one (conflict).
func (a *fieldReportsAPI) lookup(id string, sum [sha256.Size]byte) (dup, conflict bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	e, ok := a.seen[id]
	if !ok || a.now().Sub(e.at) > dedupeTTL {
		return false, false
	}

	return e.sum == sum, e.sum != sum
}

// ponytail: per pod and in memory, so a retry landing on another replica can queue twice. The consumer acks a known report_id without a second write; move this to a shared store if that is not enough.
func (a *fieldReportsAPI) remember(id string, sum [sha256.Size]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := a.now()
	if len(a.seen) >= dedupeMaxEntries {
		for k, e := range a.seen {
			if now.Sub(e.at) > dedupeTTL {
				delete(a.seen, k)
			}
		}
	}

	for k := range a.seen {
		if len(a.seen) < dedupeMaxEntries {
			break
		}

		delete(a.seen, k)
	}

	a.seen[id] = seenReport{sum: sum, at: now}
}

func (a *fieldReportsAPI) observe(d time.Duration) {
	i := 0
	for i < len(confirmBuckets) && d.Seconds() > confirmBuckets[i] {
		i++
	}

	a.buckets[i].Add(1)
	a.sumMicros.Add(d.Microseconds())
}

// Appends the field report series to the /metrics text.
func (a *fieldReportsAPI) writeMetrics(w io.Writer) {
	_, _ = fmt.Fprintf(w, "# HELP client_api_field_reports_accepted_total Reports answered 202 after a broker confirm.\n# TYPE client_api_field_reports_accepted_total counter\nclient_api_field_reports_accepted_total %d\n"+
		"# HELP client_api_field_reports_deduped_total Retries answered 202 from the idempotency cache.\n# TYPE client_api_field_reports_deduped_total counter\nclient_api_field_reports_deduped_total %d\n"+
		"# HELP client_api_field_reports_unavailable_total Reports answered 503 (broker down, nack or confirm timeout).\n# TYPE client_api_field_reports_unavailable_total counter\nclient_api_field_reports_unavailable_total %d\n"+
		"# HELP client_api_field_report_confirm_seconds Time from publish to broker confirm.\n# TYPE client_api_field_report_confirm_seconds histogram\n",
		a.accepted.Load(), a.deduped.Load(), a.unavailable.Load())

	var cum int64

	for i, le := range confirmBuckets {
		cum += a.buckets[i].Load()
		_, _ = fmt.Fprintf(w, "client_api_field_report_confirm_seconds_bucket{le=\"%g\"} %d\n", le, cum)
	}

	cum += a.buckets[len(confirmBuckets)].Load()
	_, _ = fmt.Fprintf(w, "client_api_field_report_confirm_seconds_bucket{le=\"+Inf\"} %d\nclient_api_field_report_confirm_seconds_sum %g\nclient_api_field_report_confirm_seconds_count %d\n", cum, float64(a.sumMicros.Load())/1e6, cum)
}

// One connection and one confirm channel, kept alive by run. publish fails at once while it is down.
type brokerInbox struct {
	mu  sync.RWMutex
	ch  *amqp.Channel
	ret *returnLog
}

// Remembers which message ids the broker sent back as unroutable.
type returnLog struct {
	mu  sync.Mutex
	in  chan amqp.Return
	ids map[string]bool
}

func newReturnLog(in chan amqp.Return) *returnLog {
	return &returnLog{in: in, ids: map[string]bool{}}
}

// Whether the broker returned message id. A return always arrives before the confirm, so call this after the confirm.
func (l *returnLog) unroutable(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	for drained := false; !drained; {
		select {
		case r := <-l.in:
			l.ids[r.MessageId] = true
		default:
			drained = true
		}
	}

	ok := l.ids[id]
	delete(l.ids, id)

	return ok
}

// Blocks until the connection or the channel closes, or ctx is done.
func waitClosed(ctx context.Context, conn, ch <-chan *amqp.Error) {
	select {
	case <-conn:
	case <-ch:
	case <-ctx.Done():
	}
}

// Keeps a connection to the broker until ctx is done, redialing with a growing delay.
func (b *brokerInbox) run(ctx context.Context, url string) {
	delay := 200 * time.Millisecond

	for ctx.Err() == nil {
		conn, connClosed, chClosed, err := b.connect(url)
		if err == nil {
			delay = 200 * time.Millisecond

			waitClosed(ctx, connClosed, chClosed)
			b.set(nil, nil)

			_ = conn.Close()
		} else {
			delay = min(delay*2, 5*time.Second)
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
		}
	}
}

func (b *brokerInbox) connect(url string) (*amqp.Connection, chan *amqp.Error, chan *amqp.Error, error) {
	conn, err := amqp.DialConfig(url, amqp.Config{Heartbeat: 10 * time.Second, Dial: amqp.DefaultDial(2 * time.Second)})
	if err != nil {
		return nil, nil, nil, err
	}

	ch, err := conn.Channel()
	if err == nil {
		_, err = ch.QueueDeclare(reportQueue, true, false, false, false, nil)
	}

	if err == nil {
		err = ch.Confirm(false)
	}

	if err != nil {
		_ = conn.Close()

		return nil, nil, nil, err
	}

	connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))
	chClosed := ch.NotifyClose(make(chan *amqp.Error, 1))

	b.set(ch, newReturnLog(ch.NotifyReturn(make(chan amqp.Return, 256))))

	return conn, connClosed, chClosed, nil
}

func (b *brokerInbox) set(ch *amqp.Channel, ret *returnLog) {
	b.mu.Lock()
	b.ch, b.ret = ch, ret
	b.mu.Unlock()
}

func (b *brokerInbox) publish(ctx context.Context, m reportMsg) error {
	b.mu.RLock()
	ch, ret := b.ch, b.ret
	b.mu.RUnlock()

	if ch == nil || ch.IsClosed() {
		return errBrokerDown
	}

	conf, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", reportQueue, true, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		CorrelationId: m.correlationID,
		MessageId:     m.id,
		Timestamp:     m.at,
		Headers:       m.trace,
		Body:          bytes.Clone(m.body),
	})
	if err != nil {
		return err
	}

	acked, err := conf.WaitContext(ctx)
	if err != nil {
		return err
	}

	if !acked {
		return errNacked
	}

	if ret.unroutable(m.id) {
		return errUnroutable
	}

	return nil
}
