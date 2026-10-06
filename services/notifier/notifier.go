package main

import (
	"encoding/json"
	"log"
	"time"
)

// HazardEvent is the canonical record the Aggregator publishes (doc/TRD_M1.md
// 3.2). Kept here as this service's own copy: no Go code is shared across services.
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

// notifier alerts field teams once per (hazard_id, severity) (doc/TRD_M1.md 8.2).
type notifier struct {
	dedup *dedupStore
	// send delivers one alert to the field teams' devices.
	send func(e HazardEvent, correlationID string)
}

// logAlert is the alert channel of this milestone: the TRD names no device
// gateway, so an alert is a structured log line the demo can point at.
func logAlert(e HazardEvent, correlationID string) {
	log.Printf("alert sent correlation_id=%s hazard_id=%s hazard_type=%s severity=%s area=%q",
		correlationID, e.HazardID, e.HazardType, orDash(e.Severity), orDash(e.AreaName))
}

// handle alerts on a new (hazard_id, severity) and acks a repeat without alerting.
// The dedup record is on disk before the ack: a crash before the ack only
// reprocesses (TRD 8.2 accepts a duplicate alert if the crash comes between
// sending and recording).
func (n *notifier) handle(m message) outcome {
	var e HazardEvent
	if err := json.Unmarshal(m.body, &e); err != nil || e.HazardID == "" {
		log.Printf("message received correlation_id=%s result=rejected reason=malformed", m.correlationID)

		return drop
	}

	k := dedupKey{e.HazardID, orDash(e.Severity)}
	log.Printf("message received correlation_id=%s hazard_id=%s severity=%s", m.correlationID, k.hazardID, k.severity)

	if n.dedup.has(k) {
		log.Printf("message processed correlation_id=%s hazard_id=%s severity=%s result=skipped reason=duplicate", m.correlationID, k.hazardID, k.severity)

		return ack
	}

	n.send(e, m.correlationID)

	if err := n.dedup.record(k); err != nil {
		log.Printf("dedup record failed correlation_id=%s hazard_id=%s: %v", m.correlationID, k.hazardID, err)

		return requeue
	}

	log.Printf("dedup record written correlation_id=%s hazard_id=%s severity=%s", m.correlationID, k.hazardID, k.severity)
	log.Printf("message processed correlation_id=%s hazard_id=%s severity=%s result=processed", m.correlationID, k.hazardID, k.severity)

	return ack
}
