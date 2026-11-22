package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"strconv"
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

	changes schemaMode // the incompatible changes this report was published with, fixed at creation
}

// The schema a report is generated in: legacy (zero), extended, or legacy plus a set of incompatible changes.
type schemaMode uint8

const (
	modeLegacy    schemaMode = 0
	modeExtended  schemaMode = 1
	changeRename  schemaMode = 2
	changeType    schemaMode = 4
	changeMissing schemaMode = 8

	allChanges = changeRename | changeType | changeMissing
)

var changeNames = map[string]schemaMode{"rename": changeRename, "type": changeType, "missing": changeMissing, "all": allChanges}

// Parses the optional "change" of an incompatible switch. Absent or null means all three. A name is trimmed and
// case insensitive. Anything else falls back to all three and the second result says why, so the caller can log
// it: the mock never answers 4xx for a body it does not like.
func incompatibleMode(raw json.RawMessage) (schemaMode, string) {
	if len(raw) == 0 || string(raw) == "null" {
		return allChanges, ""
	}

	var name string
	if json.Unmarshal(raw, &name) != nil {
		return allChanges, "change is not a string, using all"
	}

	if m, ok := changeNames[strings.ToLower(strings.TrimSpace(name))]; ok {
		return m, ""
	}

	return allChanges, "unknown change, using all"
}

func (m schemaMode) incompatible() bool { return m&allChanges != 0 }

// Empty body behaviour: legacy and extended swap, incompatible goes back to legacy.
func (m schemaMode) toggled() schemaMode {
	if m == modeLegacy {
		return modeExtended
	}

	return modeLegacy
}

func (m schemaMode) name() string {
	switch {
	case m.incompatible():
		return "incompatible"
	case m == modeExtended:
		return "extended"
	}

	return "legacy"
}

func (m schemaMode) changeName() string {
	for _, n := range []string{"rename", "type", "missing"} {
		if m == changeNames[n] {
			return n
		}
	}

	return "all"
}

// Renders the contract fields, then applies the report's own changes, so old reports never change shape.
func (r VolcanicReport) MarshalJSON() ([]byte, error) {
	type plain VolcanicReport

	b, err := json.Marshal(plain(r))
	if err != nil || r.changes == modeLegacy {
		return b, err
	}

	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(b, &fields); err != nil {
		return nil, err
	}

	if r.changes&changeRename != 0 {
		fields["eruption_count"] = fields["eruption_count_24h"]
		delete(fields, "eruption_count_24h")
	}

	if r.changes&changeType != 0 {
		fields["ash_column_height_m"] = json.RawMessage(strconv.Quote(strconv.FormatFloat(r.AshColumnHeightM, 'f', -1, 64)))
	}

	if r.changes&changeMissing != 0 {
		delete(fields, "volcano_id")
	}

	return json.Marshal(fields)
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

// Generates one report stamped with the given time and schema mode, and appends it.
func (s *reportStore) add(at time.Time, mode schemaMode) VolcanicReport {
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

	r.changes = mode & allChanges

	if mode&modeExtended != 0 {
		c := float64(randInt(50, 100)) / 100
		r.ConfidenceLevel = &c
	}

	s.reports = append(s.reports, r)

	return r
}

// Backfills n reports spaced interval apart, ending now, with the mode current at startup.
func (s *reportStore) seed(n int, interval time.Duration, now time.Time, mode schemaMode) {
	for i := n; i > 0; i-- {
		s.add(now.Add(-time.Duration(i)*interval), mode)
	}

	log.Printf("seeded %d volcanic reports", n)
}

// Appends one report every interval until ctx is done. It ignores outage state, so data keeps accumulating and is there the moment the outage ends.
func (s *reportStore) run(ctx context.Context, interval time.Duration, mode func() schemaMode) {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			r := s.add(now, mode())
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

		if err := writeReports(w, s.since(since)); err != nil {
			log.Printf("failed to write volcanic-reports response: %v", err)
		}
	}
}
