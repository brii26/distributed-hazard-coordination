package main

import (
	"encoding/json"
	"time"
)

// Canonical hazard record from the Aggregator. This service keeps its own copy, no Go code is shared across services.
type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    *string        `json:"severity"`
	AreaName    *string        `json:"area_name"`
	Latitude    *float64       `json:"latitude"`
	Longitude   *float64       `json:"longitude"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}

func orDash(s *string) string {
	if s == nil || *s == "" {
		return "-"
	}

	return *s
}

// Alerts field teams once per (hazard_id, severity).
type notifier struct {
	dedup *dedupStore
	// Delivers one alert to the field teams' devices.
	send func(e HazardEvent, correlationID string)
}

// The alert channel for now: no device gateway exists, so an alert is a structured log line.
func logAlert(e HazardEvent, correlationID string) {
	withID(correlationID).Info("alert sent", "hazard_id", e.HazardID, "hazard_type", e.HazardType, "severity", orDash(e.Severity), "area", orDash(e.AreaName))
}

// Alerts on a new (hazard_id, severity) and acks a repeat without alerting. The dedup record hits disk before the ack. A crash between sending and recording can alert twice.
func (n *notifier) handle(m message) outcome {
	var e HazardEvent
	if err := json.Unmarshal(m.body, &e); err != nil || e.HazardID == "" {
		withID(m.correlationID).Warn("message received", "result", "rejected", "reason", "malformed")

		return drop
	}

	k := dedupKey{e.HazardID, orDash(e.Severity)}
	withID(m.correlationID).Info("message received", "hazard_id", k.hazardID, "severity", k.severity)

	if n.dedup.has(k) {
		withID(m.correlationID).Info("message processed", "hazard_id", k.hazardID, "severity", k.severity, "result", "skipped", "reason", "duplicate")

		return ack
	}

	n.send(e, m.correlationID)

	if err := n.dedup.record(k); err != nil {
		withID(m.correlationID).Warn("dedup record failed", "hazard_id", k.hazardID, "error", err)

		return requeue
	}

	withID(m.correlationID).Info("dedup record written", "hazard_id", k.hazardID, "severity", k.severity)
	withID(m.correlationID).Info("message processed", "hazard_id", k.hazardID, "severity", k.severity, "result", "processed")

	return ack
}
