package main

import (
	"context"
	"time"
)

// Recovered without a write: a newer write for the same record already exists.
func (s *store) markSuperseded(ctx context.Context, id string) (bool, error) {
	return s.updateFailed(ctx, `UPDATE failed_messages SET status = 'recovered', next_attempt_at = NULL, reason = 'superseded: ' || reason WHERE message_id = $1 AND status = 'retrying'`, id)
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

	q, err := s.coordDB().QueryContext(ctx, claimDueSQL, limit, lease.Seconds())
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

	_, err := s.coordDB().ExecContext(ctx, `
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

	q, err := s.coordDB().QueryContext(ctx, `SELECT status, split_part(reason, ':', 1), count(*) FROM failed_messages GROUP BY 1, 2 ORDER BY 1, 2`)
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

// One page of parked, not oversize rows of source in (received_at, message_id) order, after the given position. A zero time starts at the beginning.
func (s *store) parkedPage(ctx context.Context, source string, afterAt time.Time, afterID string, limit int) ([]failedMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	q, err := s.coordDB().QueryContext(ctx, `
SELECT message_id, source, received_at, payload, attempts
FROM failed_messages
WHERE status = 'parked' AND NOT oversize AND source = $1 AND (received_at, message_id) > ($2, $3)
ORDER BY received_at, message_id
LIMIT $4`, source, afterAt, afterID, limit)
	if err != nil {
		return nil, err
	}

	defer func() { _ = q.Close() }()

	var out []failedMessage

	for q.Next() {
		var m failedMessage
		if err = q.Scan(&m.MessageID, &m.Source, &m.ReceivedAt, &m.Payload, &m.Attempts); err != nil {
			return nil, err
		}

		out = append(out, m)
	}

	return out, q.Err()
}

// Parked rows of source that replay skips because only the head of the payload is stored.
func (s *store) countParkedOversize(ctx context.Context, source string) (n int, err error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	err = s.coordDB().QueryRowContext(ctx, `SELECT count(*) FROM failed_messages WHERE status = 'parked' AND oversize AND source = $1`, source).Scan(&n)

	return n, err
}

// Sets a parked row to recovered, with a superseded note when it was skipped for a newer write. False if the row is no longer parked.
func (s *store) markReplayed(ctx context.Context, id string, superseded bool) (bool, error) {
	if superseded {
		return s.updateFailed(ctx, `UPDATE failed_messages SET status = 'recovered', reason = 'superseded: ' || reason WHERE message_id = $1 AND status = 'parked'`, id)
	}

	return s.updateFailed(ctx, `UPDATE failed_messages SET status = 'recovered' WHERE message_id = $1 AND status = 'parked'`, id)
}

// A replay that failed again: the row stays parked with attempts + 1 and the new reason.
func (s *store) recordReplayFailure(ctx context.Context, id, reason string) error {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	_, err := s.coordDB().ExecContext(ctx, `UPDATE failed_messages SET attempts = attempts + 1, last_error_at = now(), reason = $2 WHERE message_id = $1 AND status = 'parked'`, id, reason)

	return err
}

// Takes the per-source replay lock on a dedicated connection, so a crashed pod frees it when the connection drops. ok is false when another run holds it.
func (s *store) lockReplay(ctx context.Context, source string) (release func(), ok bool, err error) {
	conn, err := s.coordDB().Conn(ctx)
	if err != nil {
		return nil, false, err
	}

	if err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, "replay:"+source).Scan(&ok); err != nil || !ok {
		_ = conn.Close()

		return nil, false, err
	}

	return func() {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtext($1))`, "replay:"+source)
		_ = conn.Close()
	}, true, nil
}
