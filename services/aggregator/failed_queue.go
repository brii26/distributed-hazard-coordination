package main

import (
	"context"
	"time"
)

// Recovered without a write: a newer write for the same record already exists.
func (s *store) markSuperseded(ctx context.Context, id string) (bool, error) {
	return s.updateFailed(ctx, `UPDATE failed_messages SET status = 'recovered', next_attempt_at = NULL, reason = 'superseded: ' || reason WHERE message_id = $1`, id)
}

// Takes up to limit due retrying rows for lease. SKIP LOCKED keeps two replicas off the same row, and the pushed next_attempt_at brings the row back if the worker dies.
const claimDueSQL = `
UPDATE failed_messages SET next_attempt_at = now() + make_interval(secs => $2::float8)
WHERE message_id IN (
    SELECT message_id FROM failed_messages
    WHERE status = 'retrying' AND next_attempt_at <= now()
    ORDER BY next_attempt_at LIMIT $1 FOR UPDATE SKIP LOCKED)
RETURNING message_id, source, received_at, attempts, reason, COALESCE(mapped_event::text, ''), COALESCE(correlation_id, '')`

func (s *store) claimDue(ctx context.Context, limit int, lease time.Duration) ([]failedMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	q, err := s.db.QueryContext(ctx, claimDueSQL, limit, lease.Seconds())
	if err != nil {
		return nil, err
	}

	defer func() { _ = q.Close() }()

	var out []failedMessage

	for q.Next() {
		var (
			m  failedMessage
			ev string
		)

		if err = q.Scan(&m.MessageID, &m.Source, &m.ReceivedAt, &m.Attempts, &m.Reason, &ev, &m.CorrelationID); err != nil {
			return nil, err
		}

		m.MappedEvent = []byte(ev)
		out = append(out, m)
	}

	return out, q.Err()
}

// One failed retry: attempts + 1 in SQL, then retrying with the next try in wait, or parked when keep is false.
func (s *store) recordRetryFailure(ctx context.Context, id string, keep bool, reason string, wait time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	_, err := s.db.ExecContext(ctx, `
UPDATE failed_messages SET attempts = attempts + 1, last_error_at = now(), reason = $2, retryable = $3::bool,
    status = CASE WHEN $3::bool THEN 'retrying' ELSE 'parked' END,
    next_attempt_at = CASE WHEN $3::bool THEN now() + make_interval(secs => $4::float8) END
WHERE message_id = $1 AND status = 'retrying'`, id, reason, keep, wait.Seconds())

	return err
}

type failedCount struct {
	status, reason string
	n              int64
}

// Rows per status and reason code, for /metrics.
func (s *store) failedCounts(ctx context.Context) ([]failedCount, error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	q, err := s.db.QueryContext(ctx, `SELECT status, split_part(reason, ':', 1), count(*) FROM failed_messages GROUP BY 1, 2 ORDER BY 1, 2`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = q.Close() }()

	var out []failedCount

	for q.Next() {
		var c failedCount
		if err = q.Scan(&c.status, &c.reason, &c.n); err != nil {
			return nil, err
		}

		out = append(out, c)
	}

	return out, q.Err()
}
