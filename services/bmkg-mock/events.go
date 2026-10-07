package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"
)

const (
	defaultSeedCount     = 20
	defaultEventInterval = 10 * time.Second
)

// The BMKG seismic event.
type SeismicEvent struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKm          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
}

// Only generated for events with PotentialTsunami=true.
type TsunamiWarning struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"`
	AffectedZones    []string  `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
}

// The fixed list new events draw from. lat/lon is the center each epicenter is jittered around.
var regions = []struct {
	name     string
	lat, lon float64
}{
	{"Aceh", 4.7, 96.0},
	{"Sumatera Barat", -0.9, 99.5},
	{"Banten", -6.5, 105.8},
	{"Jawa Barat", -7.6, 107.5},
	{"Jawa Timur", -8.2, 112.5},
	{"Bali", -8.6, 115.2},
	{"Nusa Tenggara Timur", -9.5, 122.0},
	{"Sulawesi Tengah", -1.4, 120.0},
	{"Maluku", -3.7, 128.2},
	{"Papua", -4.0, 138.0},
}

// The mock's in-memory dataset. The generator appends while the handlers read, so access goes through the RWMutex.
type eventStore struct {
	mu       sync.RWMutex
	events   []SeismicEvent
	warnings []TsunamiWarning
	seq      int
}

// A value in [lo, hi) rounded to one decimal.
func randFloat(lo, hi float64) float64 {
	return float64(int((lo+rand.Float64()*(hi-lo))*10)) / 10 //nolint:gosec // mock data, not security-sensitive
}

// Generates one event stamped with the given time and appends it. If it can cause a tsunami its warning is appended under the same lock, so a reader never sees an event without its warning.
func (s *eventStore) add(at time.Time) SeismicEvent {
	reg := regions[rand.IntN(len(regions))] //nolint:gosec // mock data, not security-sensitive
	tsunami := rand.IntN(5) == 0            //nolint:gosec // mock data, not security-sensitive

	mag, depth := randFloat(3, 6.5), randFloat(5, 300)
	if tsunami {
		mag, depth = randFloat(6.5, 8.5), randFloat(5, 60)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.seq++

	e := SeismicEvent{
		EventID:          fmt.Sprintf("BMKG-%06d", s.seq),
		Magnitude:        mag,
		DepthKm:          depth,
		EpicenterLat:     reg.lat + randFloat(-1, 1),
		EpicenterLon:     reg.lon + randFloat(-1, 1),
		RegionName:       reg.name,
		OccurredAt:       at,
		PotentialTsunami: tsunami,
	}
	s.events = append(s.events, e)

	if tsunami {
		level := "Waspada"
		if mag >= 8 {
			level = "Awas"
		} else if mag >= 7 {
			level = "Siaga"
		}

		s.warnings = append(s.warnings, TsunamiWarning{
			WarningID:        fmt.Sprintf("TSU-%06d", s.seq),
			RelatedEventID:   e.EventID,
			ThreatLevel:      level,
			AffectedZones:    []string{reg.name},
			EstimatedArrival: at.Add(time.Duration(10+rand.IntN(50)) * time.Minute), //nolint:gosec // mock data
		})
	}

	return e
}

// Backfills n events spaced interval apart, ending now.
func (s *eventStore) seed(n int, interval time.Duration, now time.Time) {
	for i := n; i > 0; i-- {
		s.add(now.Add(-time.Duration(i) * interval))
	}

	log.Printf("seeded %d seismic events", n)
}

// Appends one event every interval until ctx is done.
func (s *eventStore) run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			e := s.add(now)
			log.Printf("generated seismic event event_id=%s region_name=%s", e.EventID, e.RegionName)
		}
	}
}

// A copy of the events with OccurredAt after t.
func (s *eventStore) eventsSince(t time.Time) []SeismicEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := []SeismicEvent{}

	for _, e := range s.events {
		if e.OccurredAt.After(t) {
			out = append(out, e)
		}
	}

	return out
}

// A copy of the warnings whose related event occurred after t.
func (s *eventStore) warningsSince(t time.Time) []TsunamiWarning {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := map[string]bool{}

	for _, e := range s.events {
		if e.PotentialTsunami && e.OccurredAt.After(t) {
			ids[e.EventID] = true
		}
	}

	out := []TsunamiWarning{}

	for _, w := range s.warnings {
		if ids[w.RelatedEventID] {
			out = append(out, w)
		}
	}

	return out
}
