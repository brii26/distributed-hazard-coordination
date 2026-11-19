//go:build capture

package main

// Offline check of the fire engine on the #308 E2 replay capture (about 436k events). Run from services/aggregator:
//
//	go test -tags capture -run TestFireEngineOnCapture -v .
//
// The capture stays local (tools/feedprobe/out/ is never committed), so the test skips when the file is missing.

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

const capturePath = "../../tools/feedprobe/out/e2-replay.ndjson"

type captureLine struct {
	Event   string `json:"event"`
	Data    string `json:"data_raw"`
	HasData bool   `json:"has_data"`
}

// loadCapture parses the capture in offset order, skipping the repeats a resume causes, and counts lines that do not parse.
func loadCapture(t *testing.T) ([]feedEvent, int) {
	t.Helper()

	f, err := os.Open(capturePath)
	if err != nil {
		t.Skipf("no capture: %v", err)
	}

	defer func() { _ = f.Close() }()

	var (
		events []feedEvent
		bad    int
		last   int64 = -1
	)

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)

	for sc.Scan() {
		var l captureLine
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil || !l.HasData {
			continue
		}

		ev, err := parseFeedEvent(l.Event, []byte(l.Data))
		if err != nil {
			bad++

			continue
		}

		if ev.Offset > last {
			last = ev.Offset
			events = append(events, ev)
		}
	}

	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}

	return events, bad
}

type captureRun struct {
	saved                 map[string]fireSnapshot
	parks                 map[string]int
	counts                fireCounts
	openSingles, peakDots int
	peakHeapMB            float64
	took                  time.Duration
}

func runCapture(events []feedEvent) captureRun {
	e := newFireEngine(12 * time.Hour)
	run := captureRun{saved: map[string]fireSnapshot{}, parks: map[string]int{}}
	start := time.Now()

	for i, ev := range events {
		if p := e.apply(ev); p != "" {
			run.parks[p]++
		}

		if (i+1)%500 == 0 || i == len(events)-1 {
			for _, s := range e.drain() {
				run.saved[s.Name] = s
			}

			run.peakDots = max(run.peakDots, len(e.dots))
		}

		if (i+1)%50000 == 0 {
			var m runtime.MemStats

			runtime.ReadMemStats(&m)
			run.peakHeapMB = max(run.peakHeapMB, float64(m.HeapAlloc)/(1<<20))
		}
	}

	run.took = time.Since(start)
	run.counts = e.counts

	for _, d := range e.dots {
		if d.fire == nil && d.eligible() {
			run.openSingles++
		}
	}

	return run
}

// checksum follows the audit API: SHA-256 of the live fire names, sorted, joined by newlines.
func (r captureRun) checksum() (int, string) {
	var names []string

	for name, s := range r.saved {
		if !s.Withdrawn {
			names = append(names, name)
		}
	}

	slices.Sort(names)
	sum := sha256.Sum256([]byte(strings.Join(names, "\n")))

	return len(names), hex.EncodeToString(sum[:])
}

func TestFireEngineOnCapture(t *testing.T) { //nolint:paralleltest // measures time and memory, so it runs alone
	events, bad := loadCapture(t)

	first := runCapture(events)
	second := runCapture(events)

	fires, sum := first.checksum()
	_, sum2 := second.checksum()
	rate := float64(len(events)) / first.took.Seconds()

	t.Logf("events %d, unparsable %d, took %s (%.0f events/s)", len(events), bad, first.took.Round(time.Millisecond), rate)
	t.Logf("live fires %d, withdrawn %d, checksum %s", fires, len(first.saved)-fires, sum)
	t.Logf("counts %+v, open singles %d, parks %v", first.counts, first.openSingles, first.parks)
	t.Logf("peak dots in memory %d, peak heap %.1f MiB", first.peakDots, first.peakHeapMB)

	if sum != sum2 {
		t.Fatalf("two runs gave different fires: %s vs %s", sum, sum2)
	}

	if rate < 3000 {
		t.Fatalf("%.0f events/s is below 3,000; a replay from offset 0 would be too slow", rate)
	}
}
