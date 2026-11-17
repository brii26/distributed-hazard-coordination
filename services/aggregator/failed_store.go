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
}

// Builds the row for raw, id = source + ":" + hex(sha256(raw)) over the full bytes, and cuts the payload when it is over 1 MiB.
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

// Inserts m once. The same bytes failing again change nothing, unless the row was recovered: then it is reopened as parked with attempts + 1.
const insertFailedSQL = `
INSERT INTO failed_messages
    (message_id, source, received_at, payload, payload_sha256, payload_size, oversize,
     reason, retryable, status, source_ref_id, last_error_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULLIF($11, ''), $12)
ON CONFLICT (message_id) DO UPDATE SET
    status = 'parked', attempts = failed_messages.attempts + 1,
    last_error_at = EXCLUDED.last_error_at, reason = EXCLUDED.reason
WHERE failed_messages.status = 'recovered'`

func (s *store) insertFailed(ctx context.Context, m failedMessage) error {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	_, err := s.db.ExecContext(ctx, insertFailedSQL, m.MessageID, m.Source, m.ReceivedAt, m.Payload,
		m.PayloadSHA, m.PayloadSize, m.Oversize, m.Reason, m.Retryable, m.Status, m.SourceRefID, m.LastErrorAt)

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

// One page of rows with the given status in (received_at, message_id) order, after cursor ("" for the first page). next is "" on the last page.
func (s *store) listFailed(ctx context.Context, status, cursor string, limit int) (rows []failedMessage, next string, err error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	after, afterID := time.Time{}, ""

	if cursor != "" {
		if after, afterID, err = decodeFailedCursor(cursor); err != nil {
			return nil, "", err
		}
	}

	q, err := s.db.QueryContext(ctx, `
SELECT message_id, source, received_at, payload, payload_sha256, payload_size, oversize,
       reason, retryable, attempts, status, COALESCE(source_ref_id, ''), last_error_at
FROM failed_messages
WHERE status = $1 AND ($2 = '' OR (received_at, message_id) > ($3, $2))
ORDER BY received_at, message_id
LIMIT $4`, status, afterID, after, limit+1)
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

// Sets status to recovered. False if the id does not exist.
func (s *store) markRecovered(ctx context.Context, id string) (bool, error) {
	return s.updateFailed(ctx, `UPDATE failed_messages SET status = 'recovered' WHERE message_id = $1`, id)
}

// Adds 1 to attempts and stamps last_error_at in SQL, so parallel callers never lose a count.
func (s *store) incrementAttempts(ctx context.Context, id string) (bool, error) {
	return s.updateFailed(ctx, `UPDATE failed_messages SET attempts = attempts + 1, last_error_at = now() WHERE message_id = $1`, id)
}

func (s *store) updateFailed(ctx context.Context, query, id string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	res, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return false, err
	}

	n, err := res.RowsAffected()

	return n > 0, err
}
