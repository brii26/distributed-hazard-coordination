package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

const (
	maxFailedPayload  = 1 << 20  // above this only the head is kept
	failedPayloadHead = 64 << 10 // how much of an oversize payload is kept
)

const (
	failedRetrying  = "retrying"
	failedParked    = "parked"
	failedRecovered = "recovered"
)

// One rejected message. Payload is the whole body, or its first 64 KiB when Oversize (such a row cannot be replayed).
type failedMessage struct {
	MessageID   string
	Source      string
	ReceivedAt  time.Time
	Payload     []byte
	PayloadSHA  []byte
	PayloadSize int
	Oversize    bool
	Reason      string
	Retryable   bool
	Attempts    int
	Status      string
	SourceRefID string
	LastErrorAt time.Time
	// MappedEvent is the HazardEvent as JSON, set on rows the retry worker writes. Payload stays the record as received.
	MappedEvent   []byte
	NextAttemptAt time.Time
	CorrelationID string
}

// Builds the row for raw (the first failure is attempt 1), id = source + ":" + hex(sha256(raw)) over the full bytes, and cuts the payload when it is over 1 MiB.
func newFailedMessage(source string, raw []byte, reason string, retryable bool, receivedAt time.Time) failedMessage {
	sum := sha256.Sum256(raw)
	m := failedMessage{
		MessageID:   source + ":" + hex.EncodeToString(sum[:]),
		Source:      source,
		ReceivedAt:  receivedAt,
		Payload:     raw,
		PayloadSHA:  sum[:],
		PayloadSize: len(raw),
		Reason:      reason,
		Retryable:   retryable,
		Status:      failedParked,
		Attempts:    1,
		LastErrorAt: receivedAt,
	}

	if retryable {
		m.Status = failedRetrying
	}

	if len(raw) > maxFailedPayload {
		m.Oversize = true
		m.Payload = raw[:failedPayloadHead]
	}

	return m
}

// Inserts m once. The same bytes failing again change nothing, unless the row was recovered: then it is reopened with attempts + 1.
const insertFailedSQL = `
INSERT INTO failed_messages
    (message_id, source, received_at, payload, payload_sha256, payload_size, oversize,
     reason, retryable, attempts, status, source_ref_id, last_error_at,
     mapped_event, next_attempt_at, correlation_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULLIF($12, ''), $13,
        NULLIF($14::text, '')::jsonb, $15, NULLIF($16, ''))
ON CONFLICT (message_id) DO UPDATE SET
    status = EXCLUDED.status, attempts = failed_messages.attempts + 1,
    last_error_at = EXCLUDED.last_error_at, reason = EXCLUDED.reason,
    retryable = EXCLUDED.retryable, mapped_event = EXCLUDED.mapped_event,
    next_attempt_at = EXCLUDED.next_attempt_at, correlation_id = EXCLUDED.correlation_id
WHERE failed_messages.status = 'recovered'`

func (s *store) insertFailed(ctx context.Context, m failedMessage) error {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	var next *time.Time
	if !m.NextAttemptAt.IsZero() {
		next = &m.NextAttemptAt
	}

	_, err := s.coordDB().ExecContext(ctx, insertFailedSQL, m.MessageID, m.Source, m.ReceivedAt, m.Payload,
		m.PayloadSHA, m.PayloadSize, m.Oversize, m.Reason, m.Retryable, m.Attempts, m.Status, m.SourceRefID, m.LastErrorAt,
		string(m.MappedEvent), next, m.CorrelationID)

	return err
}

// Opaque cursor: base64 of the last row's received_at and message_id.
func encodeFailedCursor(t time.Time, id string) string {
	return base64.URLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano) + "|" + id))
}

func decodeFailedCursor(c string) (time.Time, string, error) {
	b, err := base64.URLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, "", err
	}

	ts, id, ok := strings.Cut(string(b), "|")
	if !ok {
		return time.Time{}, "", errors.New("bad cursor")
	}

	t, err := time.Parse(time.RFC3339Nano, ts)

	return t, id, err
}

// Which rows listFailed returns: any of Statuses, and only Source when it is set.
type failedFilter struct {
	Statuses []string
	Source   string
}

// One page of rows matching f in (received_at, message_id) order, after cursor ("" for the first page). next is "" on the last page.
func (s *store) listFailed(ctx context.Context, f failedFilter, cursor string, limit int) (rows []failedMessage, next string, err error) {
	if limit <= 0 {
		return nil, "", errors.New("listFailed: limit must be positive")
	}

	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	after, afterID := time.Time{}, ""

	if cursor != "" {
		if after, afterID, err = decodeFailedCursor(cursor); err != nil {
			return nil, "", err
		}
	}

	q, err := s.coordDB().QueryContext(ctx, `
SELECT message_id, source, received_at, payload, payload_sha256, payload_size, oversize,
       reason, retryable, attempts, status, COALESCE(source_ref_id, ''), last_error_at
FROM failed_messages
WHERE status = ANY($1) AND ($5 = '' OR source = $5) AND ($2 = '' OR (received_at, message_id) > ($3, $2))
ORDER BY received_at, message_id
LIMIT $4`, f.Statuses, afterID, after, limit+1, f.Source)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = q.Close() }()

	for q.Next() {
		var m failedMessage
		if err = q.Scan(&m.MessageID, &m.Source, &m.ReceivedAt, &m.Payload, &m.PayloadSHA, &m.PayloadSize,
			&m.Oversize, &m.Reason, &m.Retryable, &m.Attempts, &m.Status, &m.SourceRefID, &m.LastErrorAt); err != nil {
			return nil, "", err
		}

		rows = append(rows, m)
	}

	if err = q.Err(); err != nil {
		return nil, "", err
	}

	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[limit-1]
		next = encodeFailedCursor(last.ReceivedAt, last.MessageID)
	}

	return rows, next, nil
}

// Sets a retrying row to recovered. False if the id does not exist or the row is no longer retrying.
func (s *store) markRecovered(ctx context.Context, id string) (bool, error) {
	return s.updateFailed(ctx, `UPDATE failed_messages SET status = 'recovered', next_attempt_at = NULL WHERE message_id = $1 AND status = 'retrying'`, id)
}

// Adds 1 to attempts and stamps last_error_at in SQL, so parallel callers never lose a count.
func (s *store) incrementAttempts(ctx context.Context, id string) (bool, error) {
	return s.updateFailed(ctx, `UPDATE failed_messages SET attempts = attempts + 1, last_error_at = now() WHERE message_id = $1`, id)
}

func (s *store) updateFailed(ctx context.Context, query, id string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	res, err := s.coordDB().ExecContext(ctx, query, id)
	if err != nil {
		return false, err
	}

	n, err := res.RowsAffected()

	return n > 0, err
}
