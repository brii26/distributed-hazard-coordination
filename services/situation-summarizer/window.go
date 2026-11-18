package main

import (
	"encoding/json"
	"math"
	"sort"
	"sync"
	"time"
)

// The part of a HazardEvent this service reads. It keeps its own copy, no Go code is shared across services.
type hazardEvent struct {
	HazardID   string  `json:"hazard_id"`
	HazardType string  `json:"hazard_type"`
	Severity   *string `json:"severity"`
	AreaName   *string `json:"area_name"`
}

const (
	unknownArea = "unknown"
	maxEvents   = 50000 // memory guard, the oldest events go first
)

type stamped struct {
	at       time.Time
	area     string
	hazard   string
	severity string
}

// Recent events kept in memory, enough for the summary window and the anomaly history.
type window struct {
	mu     sync.Mutex
	keep   time.Duration
	events []stamped
	now    func() time.Time
}

func newWindow(keep time.Duration) *window {
	return &window{keep: keep, now: time.Now}
}

func orDefault(s *string, def string) string {
	if s == nil || *s == "" {
		return def
	}

	return *s
}

// Reads one message body, false when it is not a HazardEvent.
func (w *window) add(body []byte) bool {
	var e hazardEvent
	if err := json.Unmarshal(body, &e); err != nil || e.HazardID == "" {
		return false
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	now := w.now()
	w.events = append(w.events, stamped{at: now, area: orDefault(e.AreaName, unknownArea), hazard: e.HazardType, severity: orDefault(e.Severity, "-")})
	w.trim(now)

	return true
}

func (w *window) trim(now time.Time) {
	cut := 0
	for cut < len(w.events) && now.Sub(w.events[cut].at) > w.keep {
		cut++
	}

	cut = max(cut, len(w.events)-maxEvents)
	if cut > 0 {
		w.events = append([]stamped(nil), w.events[cut:]...)
	}
}

// Copy of the events newer than now-span.
func (w *window) since(span time.Duration) []stamped {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := w.now()
	w.trim(now)

	i := sort.Search(len(w.events), func(i int) bool { return now.Sub(w.events[i].at) <= span })

	return append([]stamped(nil), w.events[i:]...)
}

// Counts per area in the buckets before the newest one, oldest first, and the count of the newest bucket: the last width, then width-sized steps back.
func (w *window) areaSeries(width time.Duration, history int) (series map[string][]float64, current map[string]float64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := w.now()
	series, current = map[string][]float64{}, map[string]float64{}

	for _, e := range w.events {
		age := now.Sub(e.at)
		if age < 0 {
			age = 0
		}

		idx := int(age / width) // 0 is the current bucket
		if idx > history {
			continue
		}

		if idx == 0 {
			current[e.area]++

			continue
		}

		s := series[e.area]
		if s == nil {
			s = make([]float64, history)
			series[e.area] = s
		}

		s[history-idx]++ // oldest first
	}

	for a := range current {
		if series[a] == nil {
			series[a] = make([]float64, history)
		}
	}

	return series, current
}

// Fewest history buckets before a z-score means anything.
const minHistory = 5

// z-score of current against the history buckets, false with too little history. The deviation has a floor of 1 event, so a quiet area that gets two events is not flagged as infinite.
func zScore(history []float64, current float64) (z float64, ok bool) {
	if len(history) < minHistory {
		return 0, false
	}

	var sum float64
	for _, v := range history {
		sum += v
	}

	mean := sum / float64(len(history))

	var sq float64
	for _, v := range history {
		sq += (v - mean) * (v - mean)
	}

	sd := max(math.Sqrt(sq/float64(len(history))), 1)

	return (current - mean) / sd, true
}

type anomaly struct {
	Area  string  `json:"area"`
	Count float64 `json:"count"`
	Mean  float64 `json:"mean"`
	Z     float64 `json:"z"`
}

// Areas whose newest bucket is at least threshold deviations above their own history and has at least minEvents events.
func (w *window) anomalies(width time.Duration, history int, threshold, minEvents float64) []anomaly {
	series, current := w.areaSeries(width, history)

	var out []anomaly

	for area, cur := range current {
		z, ok := zScore(series[area], cur)
		if !ok || z < threshold || cur < minEvents {
			continue
		}

		var sum float64
		for _, v := range series[area] {
			sum += v
		}

		out = append(out, anomaly{Area: area, Count: cur, Mean: round2(sum / float64(len(series[area]))), Z: round2(z)})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Z > out[j].Z || (out[i].Z == out[j].Z && out[i].Area < out[j].Area) })

	return out
}

func round2(f float64) float64 { return math.Round(f*100) / 100 }
