package main

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
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
	// Satellite fires only: version grows with every change, and withdrawn_at is set when the fire merged into another or shrank away.
	Version     int64      `json:"version,omitempty"`
	WithdrawnAt *time.Time `json:"withdrawn_at,omitempty"`
}

func severityOf(e HazardEvent) string {
	if e.Severity == nil {
		return "-"
	}

	return *e.Severity
}

// The dashboard's in-memory view, hazard_id to latest HazardEvent. Lost on restart and not resynced, a known limitation.
type state struct {
	mu     sync.RWMutex
	events map[string]HazardEvent
	hub    *hub
}

func newState() *state {
	return &state{events: map[string]HazardEvent{}, hub: newHub()}
}

// Upserts one event. Keyed by hazard_id, so a redelivery just overwrites the same data, idempotent without a dedup record.
// Satellite fires arrive at least once and may arrive out of order: an event older than the kept version is ignored, and a withdrawn fire is removed.
func (s *state) handle(m message) outcome {
	var e HazardEvent
	if err := json.Unmarshal(m.body, &e); err != nil || e.HazardID == "" {
		withID(m.correlationID).Warn("message received", "result", "rejected", "reason", "malformed")

		return drop
	}

	withID(m.correlationID).Info("message received", "hazard_id", e.HazardID, "severity", severityOf(e))

	result := s.apply(e)
	if result != "stale" {
		s.hub.publish(m.body)
	}

	withID(m.correlationID).Info("message processed", "hazard_id", e.HazardID, "result", result)

	return ack
}

// apply keeps e unless a newer version is already kept. A withdrawn fire is kept as a tombstone, hidden from snapshot, so an older copy that arrives later cannot bring it back.
func (s *state) apply(e HazardEvent) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	old, existed := s.events[e.HazardID]

	switch {
	case existed && e.Version < old.Version:
		return "stale"
	case e.WithdrawnAt != nil:
		s.events[e.HazardID] = e

		return "withdrawn"
	}

	s.events[e.HazardID] = e

	if existed && old.WithdrawnAt == nil {
		return "updated"
	}

	return "inserted"
}

// Every live event, newest first, for the dashboard view. Withdrawn fires are left out.
func (s *state) snapshot() []HazardEvent {
	s.mu.RLock()
	out := make([]HazardEvent, 0, len(s.events))

	for _, e := range s.events {
		if e.WithdrawnAt == nil {
			out = append(out, e)
		}
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
