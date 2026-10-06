package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// HazardEvent is the canonical record the Aggregator publishes (doc/TRD_M1.md
// 3.2). Only the fields this service reads are typed; the log keeps the message
// as received, so a field the Aggregator adds later is recorded too.
type HazardEvent struct {
	HazardID string  `json:"hazard_id"`
	Severity *string `json:"severity"`
}

func orDash(s *string) string {
	if s == nil || *s == "" {
		return "-"
	}

	return *s
}

// entry is one line of the audit log.
type entry struct {
	ReceivedAt    time.Time       `json:"received_at"`
	CorrelationID string          `json:"correlation_id"`
	Event         json.RawMessage `json:"event"`
}

// auditLog is the append-only record of every HazardEvent received (doc/TRD_M1.md
// 8.3): one JSON object per line, never rewritten.
type auditLog struct {
	mu   sync.Mutex
	file *os.File
	now  func() time.Time
}

func openAuditLog(path string) (*auditLog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // AUDIT_LOG_PATH is operator config, not request input
	if err != nil {
		return nil, err
	}

	return &auditLog{file: f, now: time.Now}, nil
}

// append writes one entry and syncs it, so an acked message is always on disk.
func (l *auditLog) append(correlationID string, event []byte) error {
	line, err := json.Marshal(entry{ReceivedAt: l.now().UTC(), CorrelationID: correlationID, Event: event})
	if err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.file.Write(append(line, '\n')); err != nil {
		return err
	}

	return l.file.Sync()
}

func (l *auditLog) close() error { return l.file.Close() }

// auditor logs each (hazard_id, severity) once, with the same dedup scheme as the
// Notifier (doc/TRD_M1.md 8.3).
type auditor struct {
	log   *auditLog
	dedup *dedupStore
}

// handle appends a new (hazard_id, severity) to the audit log and acks a repeat
// without logging it again. The entry, then the dedup record, are on disk before
// the ack. A crash between the two can log one entry twice; it is never lost.
func (a *auditor) handle(m message) outcome {
	var e HazardEvent
	if err := json.Unmarshal(m.body, &e); err != nil || e.HazardID == "" {
		withID(m.correlationID).Warn("message received", "result", "rejected", "reason", "malformed")

		return drop
	}

	k := dedupKey{e.HazardID, orDash(e.Severity)}
	withID(m.correlationID).Info("message received", "hazard_id", k.hazardID, "severity", k.severity)

	if a.dedup.has(k) {
		withID(m.correlationID).Info("message processed", "hazard_id", k.hazardID, "severity", k.severity, "result", "skipped", "reason", "duplicate")

		return ack
	}

	if err := a.log.append(m.correlationID, m.body); err != nil {
		withID(m.correlationID).Warn("audit append failed", "hazard_id", k.hazardID, "error", err)

		return requeue
	}

	if err := a.dedup.record(k); err != nil {
		withID(m.correlationID).Warn("dedup record failed", "hazard_id", k.hazardID, "error", err)

		return requeue
	}

	withID(m.correlationID).Info("dedup record written", "hazard_id", k.hazardID, "severity", k.severity)
	withID(m.correlationID).Info("message processed", "hazard_id", k.hazardID, "severity", k.severity, "result", "logged")

	return ack
}
