package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultCriticalQueue = "critical-alerts"
	outboxBatch          = 100
	outboxPoll           = time.Second
	outboxKeepSent       = time.Hour
)

var errUnroutable = errors.New("broker returned the message as unroutable")

// One critical observation that still has to reach the broker.
type outboxRow struct {
	id, correlationID string
	body              []byte
}

// Durable list of critical events not yet confirmed by the broker.
type outboxStore interface {
	add(ctx context.Context, id, correlationID string, body []byte) error
	pending(ctx context.Context, limit int) ([]outboxRow, error)
	sent(ctx context.Context, id string) error
}

// Postgres outbox on the Canonical Store's own pool.
type pgOutbox struct{ db *sql.DB }

func (o pgOutbox) add(ctx context.Context, id, correlationID string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	_, err := o.db.ExecContext(ctx, `INSERT INTO critical_outbox (observation_id, correlation_id, body) VALUES ($1, $2, $3) ON CONFLICT (observation_id) DO NOTHING`, id, correlationID, body)

	return err
}

func (o pgOutbox) pending(ctx context.Context, limit int) ([]outboxRow, error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	rows, err := o.db.QueryContext(ctx, `SELECT observation_id, correlation_id, body FROM critical_outbox WHERE sent_at IS NULL ORDER BY created_at, observation_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var out []outboxRow

	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.id, &r.correlationID, &r.body); err != nil {
			return nil, err
		}

		out = append(out, r)
	}

	return out, rows.Err()
}

// Marks id sent and drops sent rows older than outboxKeepSent.
func (o pgOutbox) sent(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	if _, err := o.db.ExecContext(ctx, `UPDATE critical_outbox SET sent_at = now() WHERE observation_id = $1`, id); err != nil {
		return err
	}

	_, _ = o.db.ExecContext(ctx, `DELETE FROM critical_outbox WHERE sent_at < now() - make_interval(secs => $1)`, outboxKeepSent.Seconds())

	return nil
}

// A critical coastal observation, the only kind that takes the critical path.
func isCritical(h HazardEvent) bool {
	c, _ := h.Attributes["critical"].(bool)

	return h.Source == sourceSensorPantai && c
}

// One broker connection for the critical queue. publish returns only after the broker confirmed the message.
type criticalSession interface {
	publish(ctx context.Context, r outboxRow) error
	alive() bool
	close()
}

// Moves critical events from the outbox to the critical queue, oldest first, retrying with a growing delay until the broker confirms. It has its own connection and loop, so the general fan-out backlog never sits in front of it.
type criticalRelay struct {
	store               outboxStore
	dial                func(ctx context.Context) (criticalSession, error)
	retryBase, retryMax time.Duration
	wake                chan struct{}

	staged, published, failures atomic.Int64
}

func newCriticalRelay(store outboxStore, dial func(context.Context) (criticalSession, error)) *criticalRelay {
	return &criticalRelay{store: store, dial: dial, retryBase: retryBase, retryMax: retryMax, wake: make(chan struct{}, 1)}
}

// Writes h to the outbox and wakes the relay. Call it before the upsert: once it returns, a crash or an outage cannot lose the alert.
func (r *criticalRelay) stage(ctx context.Context, h HazardEvent, correlationID string) error {
	body, err := json.Marshal(h)
	if err != nil {
		return err
	}

	if err := r.store.add(ctx, h.SourceRefID, correlationID, body); err != nil {
		return err
	}

	r.staged.Add(1)

	select {
	case r.wake <- struct{}{}:
	default:
	}

	return nil
}

func (r *criticalRelay) run(ctx context.Context) {
	var s criticalSession

	failures := 0

	defer func() {
		if s != nil {
			s.close()
		}
	}()

	idle := time.NewTicker(outboxPoll)
	defer idle.Stop()

	backoff := func() {
		failures++

		r.failures.Add(1)

		select {
		case <-ctx.Done():
		case <-time.After(min(r.retryBase*time.Duration(1<<min(failures-1, 10)), r.retryMax)):
		}
	}

	for ctx.Err() == nil {
		if s == nil {
			var err error

			if s, err = r.dial(ctx); err != nil {
				log.Printf("critical relay connect failed: %v", err)
				backoff()

				continue
			}
		}

		rows, err := r.store.pending(ctx, outboxBatch)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("critical outbox read failed: %v", err)
				backoff()
			}

			continue
		}

		if len(rows) == 0 {
			select {
			case <-ctx.Done():
			case <-r.wake:
			case <-idle.C:
				if !s.alive() {
					s.close()

					s = nil
				}
			}

			continue
		}

		if err := r.drain(ctx, s, rows); err != nil {
			if ctx.Err() != nil {
				continue
			}

			s.close()

			s = nil

			backoff()

			continue
		}

		failures = 0
	}
}

// Publishes rows in order and marks each sent. The first error stops the batch.
func (r *criticalRelay) drain(ctx context.Context, s criticalSession, rows []outboxRow) error {
	for _, row := range rows {
		if err := s.publish(ctx, row); err != nil {
			withID(row.correlationID).Warn("critical publish", "observation_id", row.id, "result", "failure", "error", err.Error())

			return err
		}

		r.published.Add(1)
		withID(row.correlationID).Info("critical publish", "observation_id", row.id, "result", "success")

		if err := r.store.sent(ctx, row.id); err != nil {
			log.Printf("critical outbox mark failed id=%s: %v", row.id, err)

			return err
		}
	}

	return nil
}

func (r *criticalRelay) writeMetrics(w io.Writer) {
	_, _ = fmt.Fprintf(w, "# HELP aggregator_critical_staged_total Critical events written to the outbox.\n# TYPE aggregator_critical_staged_total counter\naggregator_critical_staged_total %d\n"+
		"# HELP aggregator_critical_published_total Critical events confirmed by the broker.\n# TYPE aggregator_critical_published_total counter\naggregator_critical_published_total %d\n"+
		"# HELP aggregator_critical_relay_failures_total Failed relay attempts (broker or database).\n# TYPE aggregator_critical_relay_failures_total counter\naggregator_critical_relay_failures_total %d\n",
		r.staged.Load(), r.published.Load(), r.failures.Load())
}

// A real AMQP connection with the durable critical queue declared and confirms on.
type criticalAMQP struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	queue   string
	returns chan amqp.Return
}

func dialCritical(url, queue string) func(context.Context) (criticalSession, error) {
	return func(context.Context) (criticalSession, error) {
		conn, err := amqp.DialConfig(url, amqp.Config{Heartbeat: 10 * time.Second, Dial: amqp.DefaultDial(rabbitDialWait)})
		if err != nil {
			return nil, err
		}

		ch, err := conn.Channel()
		if err == nil {
			_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
		}

		if err == nil {
			err = ch.Confirm(false)
		}

		if err != nil {
			_ = conn.Close()

			return nil, err
		}

		return &criticalAMQP{conn: conn, ch: ch, queue: queue, returns: ch.NotifyReturn(make(chan amqp.Return, 16))}, nil
	}
}

// Sends persistent and mandatory through the default exchange straight to the queue, then waits for the confirm.
func (s *criticalAMQP) publish(ctx context.Context, r outboxRow) error {
	conf, err := s.ch.PublishWithDeferredConfirmWithContext(ctx, "", s.queue, true, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		CorrelationId: r.correlationID,
		MessageId:     r.id,
		Timestamp:     time.Now(),
		Body:          r.body,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, confirmWait)
	defer cancel()

	acked, err := conf.WaitContext(ctx)
	if err != nil {
		return err
	}

	if !acked {
		return errNacked
	}

	select {
	case <-s.returns:
		return errUnroutable
	default:
	}

	return nil
}

func (s *criticalAMQP) alive() bool { return !s.conn.IsClosed() }

func (s *criticalAMQP) close() { _ = s.conn.Close() }
