package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	retryBatch = 20
	// A claimed row is invisible this long, so a dead worker's rows come back.
	retryLease = 60 * time.Second
)

// What the worker needs from the failed_messages table.
type failedQueue interface {
	claimDue(ctx context.Context, limit int, lease time.Duration) ([]failedMessage, error)
	markRecovered(ctx context.Context, id string) (bool, error)
	markSuperseded(ctx context.Context, id string) (bool, error)
	recordRetryFailure(ctx context.Context, id string, keep bool, reason string, wait time.Duration) error
}

// Takes due retrying rows and writes their mapped event again with the same upsert as the live path.
type retryWorker struct {
	q       failedQueue
	upsert  func(context.Context, HazardEvent) (bool, error)
	publish func(ctx context.Context, h HazardEvent, correlationID string)
	cfg     retryConfig

	recovered, failed, parked atomic.Int64
}

// Polls every cfg.poll until ctx is done. The row in hand is finished first.
func (w *retryWorker) run(ctx context.Context) {
	t := time.NewTicker(w.cfg.poll)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.tick(ctx)
		}
	}
}

// Claims one batch and processes it. Returns how many rows it handled.
func (w *retryWorker) tick(ctx context.Context) int {
	rows, err := w.q.claimDue(ctx, retryBatch, retryLease)
	if err != nil {
		withID("").Warn("retry claim failed", "error", err.Error())

		return 0
	}

	for n, m := range rows {
		if ctx.Err() != nil {
			return n
		}

		w.process(context.WithoutCancel(ctx), m)
	}

	return len(rows)
}

// Writes one row's event. m.Attempts is the failure count so far.
func (w *retryWorker) process(ctx context.Context, m failedMessage) {
	l := withID(m.CorrelationID)

	var h HazardEvent

	dec := json.NewDecoder(bytes.NewReader(m.MappedEvent))
	dec.UseNumber()

	err := dec.Decode(&h)
	if err != nil {
		err = fmt.Errorf("%w: %w", errBadEvent, err)
	} else {
		var changed bool

		changed, err = w.upsert(withNotNewerThan(withCorrelation(ctx, m.CorrelationID), m.ReceivedAt), h)
		if err == nil && changed {
			w.publish(ctx, h, m.CorrelationID)
		}
	}

	switch {
	case err == nil || errors.Is(err, errSuperseded):
		mark := w.q.markRecovered
		if err != nil {
			mark = w.q.markSuperseded
		}

		if _, merr := mark(ctx, m.MessageID); merr != nil {
			l.Warn("retry mark recovered failed", "message_id", m.MessageID, "error", merr.Error())

			return
		}

		w.recovered.Add(1)
		l.Info("retry recovered", "message_id", m.MessageID, "source", m.Source, "attempts", m.Attempts+1, "superseded", err != nil)
	default:
		w.recordFailure(ctx, m, err)
	}
}

func (w *retryWorker) recordFailure(ctx context.Context, m failedMessage, err error) {
	retryable, code := classifyFailure(err)
	next := m.Attempts + 1
	keep := retryable && next < w.cfg.maxAttempts

	if rerr := w.q.recordRetryFailure(ctx, m.MessageID, keep, code+": "+err.Error(), w.cfg.delay(next)); rerr != nil {
		withID(m.CorrelationID).Warn("retry record failed", "message_id", m.MessageID, "error", rerr.Error())

		return
	}

	if keep {
		w.failed.Add(1)
	} else {
		w.parked.Add(1)
	}

	withID(m.CorrelationID).Warn("retry failed", "message_id", m.MessageID, "attempts", next, "retrying", keep, "error", err.Error())
}

// Reads RETRY_BASE, RETRY_CAP, RETRY_POLL (durations such as 10s) and RETRY_MAX_ATTEMPTS.
func loadRetryConfig() (retryConfig, error) {
	c := defaultRetryConfig

	for name, dst := range map[string]*time.Duration{"RETRY_BASE": &c.base, "RETRY_CAP": &c.cap, "RETRY_POLL": &c.poll} {
		raw := os.Getenv(name)
		if raw == "" {
			continue
		}

		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return c, fmt.Errorf("invalid %s value: %q", name, raw)
		}

		*dst = d
	}

	n, err := envPositiveInt("RETRY_MAX_ATTEMPTS", c.maxAttempts)
	if err != nil {
		return c, err
	}

	c.maxAttempts = n

	if c.cap < c.base {
		return c, errors.New("RETRY_CAP must not be below RETRY_BASE")
	}

	return c, nil
}

// Gauge of failed_messages by status and reason (cached 5 s) and the retry counters, appended to /metrics.
type failedMetrics struct {
	counts func(context.Context) ([]failedCount, error)
	w      *retryWorker

	mu   sync.Mutex
	at   time.Time
	rows []failedCount
}

func (m *failedMetrics) write(ctx context.Context, out io.Writer) {
	m.mu.Lock()

	if time.Since(m.at) > 5*time.Second {
		if rows, err := m.counts(ctx); err == nil {
			m.rows, m.at = rows, time.Now()
		}
	}

	rows := m.rows
	m.mu.Unlock()

	_, _ = fmt.Fprint(out, "# TYPE aggregator_failed_messages gauge\n")

	for _, r := range rows {
		_, _ = fmt.Fprintf(out, "aggregator_failed_messages{status=%q,reason=%q} %d\n", r.status, r.reason, r.n)
	}

	_, _ = fmt.Fprint(out, "# TYPE aggregator_retry_total counter\n")

	for res, c := range map[string]*atomic.Int64{"recovered": &m.w.recovered, "failed": &m.w.failed, "parked": &m.w.parked} {
		_, _ = fmt.Fprintf(out, "aggregator_retry_total{result=%q} %d\n", res, c.Load())
	}
}

// Lease metrics first, then the failed message series.
func metricsHandler(lease http.HandlerFunc, fm *failedMetrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		lease(w, r)
		fm.write(r.Context(), w)
	}
}
