package main

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
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

func severityOf(e HazardEvent) string {
	if e.Severity == nil {
		return "-"
	}

	return *e.Severity
}

// state is the dashboard's in-memory view, hazard_id -> latest HazardEvent
// (doc/TRD_M1.md 8.1). It is lost on restart and not resynced from the
// Aggregator, a known limitation of the TRD.
type state struct {
	mu     sync.RWMutex
	events map[string]HazardEvent
	hub    *hub
}

func newState() *state {
	return &state{events: map[string]HazardEvent{}, hub: newHub()}
}

// handle upserts one event. Keyed by hazard_id, so a redelivered message just
// overwrites the entry with the same data: idempotent without any dedup record.
func (s *state) handle(m message) outcome {
	var e HazardEvent
	if err := json.Unmarshal(m.body, &e); err != nil || e.HazardID == "" {
		withID(m.correlationID).Warn("message received", "result", "rejected", "reason", "malformed")

		return drop
	}

	withID(m.correlationID).Info("message received", "hazard_id", e.HazardID, "severity", severityOf(e))

	s.mu.Lock()
	_, existed := s.events[e.HazardID]
	s.events[e.HazardID] = e
	s.mu.Unlock()

	s.hub.publish(m.body)

	result := "inserted"
	if existed {
		result = "updated"
	}

	withID(m.correlationID).Info("message processed", "hazard_id", e.HazardID, "result", result)

	return ack
}

// snapshot is every event, newest first, for the dashboard view.
func (s *state) snapshot() []HazardEvent {
	s.mu.RLock()
	out := make([]HazardEvent, 0, len(s.events))

	for _, e := range s.events {
		out = append(out, e)
	}

	s.mu.RUnlock()

	slices.SortFunc(out, func(a, b HazardEvent) int {
		if c := b.OccurredAt.Compare(a.OccurredAt); c != 0 {
			return c
		}

		return strings.Compare(a.HazardID, b.HazardID)
	})

	return out
}
