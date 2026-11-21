package main

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func event(id, area, sev string) []byte {
	return fmt.Appendf(nil, `{"hazard_id":%q,"hazard_type":"SEISMIC","severity":%q,"area_name":%q}`, id, sev, area)
}

func TestZScoreMatchesTheDefinition(t *testing.T) {
	t.Parallel()

	// mean 2, variance 1.2, current 6 gives (6-2)/sqrt(1.2).
	z, ok := zScore([]float64{1, 2, 3, 4, 0, 2, 2, 2, 1, 3}, 6)
	if !ok || math.Abs(z-4/math.Sqrt(1.2)) > 1e-9 {
		t.Fatalf("z=%v ok=%v", z, ok)
	}
}

func TestZScoreNeedsHistory(t *testing.T) {
	t.Parallel()

	if _, ok := zScore([]float64{1, 1, 1}, 50); ok {
		t.Fatal("three buckets are not enough history")
	}
}

func TestZScoreFloorsTheDeviationForAFlatHistory(t *testing.T) {
	t.Parallel()

	z, ok := zScore(make([]float64, 10), 4)
	if !ok || z != 4 {
		t.Fatalf("z=%v ok=%v, want 4 with the floor of 1", z, ok)
	}
}

// Fills n minutes of history with perMinute events for area, one bucket at a time.
func fillHistory(w *window, now *time.Time, area string, minutes, perMinute int) {
	for range minutes {
		*now = now.Add(time.Minute)

		for i := range perMinute {
			w.add(event(fmt.Sprintf("%s-%d-%d", area, now.Unix(), i), area, "WASPADA"))
		}
	}
}

func testWindow() (*window, *time.Time) {
	now := time.Unix(100000, 0)
	w := newWindow(time.Hour)
	w.now = func() time.Time { return now }

	return w, &now
}

func TestAnomalyFlagsASpikeAndIgnoresASteadyArea(t *testing.T) {
	t.Parallel()

	w, now := testWindow()
	fillHistory(w, now, "Jakarta", 10, 2)
	fillHistory(w, now, "Padang", 10, 2)

	// The newest minute: Padang jumps from 2 to 12, Jakarta stays at 2.
	*now = now.Add(time.Second)

	for i := range 12 {
		w.add(event(fmt.Sprintf("spike-%d", i), "Padang", "AWAS"))
	}

	for i := range 2 {
		w.add(event(fmt.Sprintf("calm-%d", i), "Jakarta", "WASPADA"))
	}

	got := w.anomalies(time.Minute, 10, 3, 5)
	if len(got) != 1 || got[0].Area != "Padang" || got[0].Z < 3 {
		t.Fatalf("got %+v, want only Padang", got)
	}
}

func TestAnomalyNeedsAMinimumCount(t *testing.T) {
	t.Parallel()

	w, now := testWindow()
	fillHistory(w, now, "Padang", 10, 0)
	*now = now.Add(time.Second)

	for i := range 3 {
		w.add(event(fmt.Sprintf("few-%d", i), "Padang", "AWAS"))
	}

	if got := w.anomalies(time.Minute, 10, 3, 5); len(got) != 0 {
		t.Fatalf("got %+v, three events are below the minimum", got)
	}
}

func TestWindowDropsMalformedAndOldEvents(t *testing.T) {
	t.Parallel()

	w, now := testWindow()

	if w.add([]byte("not json")) || w.add([]byte(`{"hazard_type":"SEISMIC"}`)) {
		t.Fatal("malformed events must be rejected")
	}

	w.add(event("old", "Padang", "AWAS"))

	*now = now.Add(2 * time.Hour)
	w.add(event("new", "Padang", "AWAS"))

	if got := w.since(time.Hour); len(got) != 1 {
		t.Fatalf("got %d events, want only the new one", len(got))
	}
}

func TestWindowCapsMemory(t *testing.T) {
	t.Parallel()

	w, _ := testWindow()

	for i := range maxEvents + 10 {
		w.add(event(fmt.Sprint(i), "A", "AWAS"))
	}

	if len(w.events) > maxEvents {
		t.Fatalf("%d events kept, cap is %d", len(w.events), maxEvents)
	}
}
