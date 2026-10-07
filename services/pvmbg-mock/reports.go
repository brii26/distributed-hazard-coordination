package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultSeedCount     = 20
	defaultEventInterval = 10 * time.Second
	defaultDelayMinMS    = 500
	defaultDelayMaxMS    = 3000
)

// The fixed list new reports draw from.
var volcanoIDs = []string{"MERAPI", "SEMERU", "BROMO", "SINABUNG", "KRAKATAU", "AGUNG", "RINJANI", "KELUD"}

var alertLevels = []string{"Normal", "Waspada", "Siaga", "Awas"}

// The PVMBG report. ConfidenceLevel is only present once the schema is extended.
type VolcanicReport struct {
	ReportID         string    `json:"report_id"`
	VolcanoID        string    `json:"volcano_id"`
	AlertLevel       string    `json:"alert_level"`
	EruptionCount24h int       `json:"eruption_count_24h"`
	AshColumnHeightM float64   `json:"ash_column_height_m"`
	ReportedAt       time.Time `json:"reported_at"`
	ConfidenceLevel  *float64  `json:"confidence_level,omitempty"`
}

// The mock's in-memory dataset. The generator appends while the handler reads, so access goes through the RWMutex.
type reportStore struct {
	mu      sync.RWMutex
	reports []VolcanicReport
	seq     int
	startID string // makes ids unique across restarts, empty in tests
}

// The next report id, with the start time of this run in it so a restart never reuses an id.
func (s *reportStore) id() string {
	if s.startID == "" {
		return fmt.Sprintf("PVMBG-%06d", s.seq)
	}

	return fmt.Sprintf("PVMBG-%s-%06d", s.startID, s.seq)
}

// A value in [lo, hi].
func randInt(lo, hi int) int {
	return lo + rand.IntN(hi-lo+1) //nolint:gosec // mock data, not security-sensitive
}

// Generates one report stamped with the given time and appends it.
func (s *reportStore) add(at time.Time, extended bool) VolcanicReport {
	level := randInt(0, len(alertLevels)-1)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.seq++

	r := VolcanicReport{
		ReportID:         s.id(),
		VolcanoID:        volcanoIDs[randInt(0, len(volcanoIDs)-1)],
		AlertLevel:       alertLevels[level],
		EruptionCount24h: randInt(0, 10*(level+1)),
		AshColumnHeightM: float64(randInt(0, 2000*level+500)),
		ReportedAt:       at,
	}

	if extended {
		c := float64(randInt(50, 100)) / 100
		r.ConfidenceLevel = &c
	}

	s.reports = append(s.reports, r)

	return r
}

// Backfills n reports spaced interval apart, ending now, with the schema current at startup.
func (s *reportStore) seed(n int, interval time.Duration, now time.Time, extended bool) {
	for i := n; i > 0; i-- {
		s.add(now.Add(-time.Duration(i)*interval), extended)
	}

	log.Printf("seeded %d volcanic reports", n)
}

// Appends one report every interval until ctx is done. It ignores outage state, so data keeps accumulating and is there the moment the outage ends.
func (s *reportStore) run(ctx context.Context, interval time.Duration, extended func() bool) {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			r := s.add(now, extended())
			log.Printf("generated volcanic report report_id=%s alert_level=%s", r.ReportID, r.AlertLevel)
		}
	}
}

// A copy of the reports with ReportedAt after t.
func (s *reportStore) since(t time.Time) []VolcanicReport {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := []VolcanicReport{}

	for _, r := range s.reports {
		if r.ReportedAt.After(t) {
			out = append(out, r)
		}
	}

	return out
}

// Guards a handler with the OAuth access token this mock issued. Missing, malformed or superseded tokens get 401, the token is never logged.
func requireBearer(tokens *tokenStore, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || !tokens.validAccess(token) {
			log.Printf("volcanic-reports rejected: bearer token missing or invalid")
			w.Header().Set("WWW-Authenticate", "Bearer")
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		next(w, r)
	}
}

// GET /volcanic-reports?since=<ISO8601>. Every served request first sleeps a random time in [minDelay, maxDelay] (the slow legacy system), and a client that gives up mid-delay frees the handler at once.
func reportsHandler(s *reportStore, minDelay, maxDelay time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var since time.Time

		if raw := r.URL.Query().Get("since"); raw != "" {
			var err error

			since, err = time.Parse(time.RFC3339, raw)
			if err != nil {
				http.Error(w, "invalid since: want ISO8601 (RFC 3339)", http.StatusBadRequest)

				return
			}
		}

		delay := time.Duration(randInt(int(minDelay/time.Millisecond), int(maxDelay/time.Millisecond))) * time.Millisecond
		log.Printf("volcanic-reports request delay_ms=%d", delay.Milliseconds())

		select {
		case <-r.Context().Done():
			return
		case <-time.After(delay):
		}

		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(s.since(since)); err != nil {
			log.Printf("failed to write volcanic-reports response: %v", err)
		}
	}
}
