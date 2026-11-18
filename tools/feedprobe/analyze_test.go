package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAcqDateTime(t *testing.T) {
	t.Parallel()

	const day = "2026-11-12"

	tests := []struct {
		date, clock string
		want        string
		wantErr     bool
	}{
		{date: day, clock: "243", want: day + "T02:43:00Z"},
		{date: day, clock: "0005", want: day + "T00:05:00Z"},
		{date: day, clock: "14:07", want: day + "T14:07:00Z"},
		{date: day, clock: "14:07:30", want: day + "T14:07:30Z"},
		{date: day, clock: "2460", wantErr: true},
		{date: "12/11/2026", clock: "0100", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.date+" "+tc.clock, func(t *testing.T) {
			t.Parallel()

			got, err := acqDateTime(tc.date, tc.clock)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %s", got)
				}

				return
			}

			if err != nil || got.Format(time.RFC3339) != tc.want {
				t.Fatalf("got %s, %v; want %s", got.Format(time.RFC3339), err, tc.want)
			}
		})
	}
}

func TestPercentile(t *testing.T) {
	t.Parallel()

	sorted := make([]time.Duration, 100)
	for i := range sorted {
		sorted[i] = time.Duration(i+1) * time.Second
	}

	for p, want := range map[float64]time.Duration{50: 50 * time.Second, 90: 90 * time.Second, 99.9: 100 * time.Second, 0: time.Second} {
		if got := percentile(sorted, p); got != want {
			t.Errorf("p%v = %s, want %s", p, got, want)
		}
	}
}

func det(off int64, sat, acq string) *rec {
	t, err := time.Parse(time.RFC3339, acq)
	if err != nil {
		panic(err)
	}

	return &rec{off: off, offOK: true, sat: sat, acq: t, acqOK: true}
}

func TestOutOfOrderLags(t *testing.T) {
	t.Parallel()

	// Given out of offset order on purpose: the function must sort by offset first.
	dets := []*rec{
		det(3, "N21", "2026-11-12T01:00:00Z"), // 30 min behind N20's 01:30, newest for N21
		det(1, "N20", "2026-11-12T01:00:00Z"),
		det(2, "N20", "2026-11-12T01:30:00Z"),
		det(4, "N20", "2026-11-12T01:20:00Z"), // 10 min behind
	}

	all, own := outOfOrderLags(dets)

	wantAll := []time.Duration{0, 0, 30 * time.Minute, 10 * time.Minute}
	wantOwn := []time.Duration{0, 0, 0, 10 * time.Minute}

	if !reflect.DeepEqual(all, wantAll) || !reflect.DeepEqual(own, wantOwn) {
		t.Fatalf("all=%v own=%v; want %v %v", all, own, wantAll, wantOwn)
	}
}

func TestMarkOffsetsSplitsResumeRepeats(t *testing.T) {
	t.Parallel()

	ev := func(conn int, off int64) *rec {
		return &rec{eventRecord: eventRecord{ConnSeq: conn}, off: off, offOK: true}
	}

	a := &analysis{recs: []*rec{
		ev(1, 1), ev(1, 2), ev(1, 3),
		ev(2, 3), ev(2, 4), // resume with ?from=3 repeats 3
		ev(3, 2), ev(3, 5), ev(3, 5), // rewind to 2 and an in-connection repeat: real repeats
	}}
	a.markOffsets()

	var gotDup, gotResume []int64

	for _, r := range a.recs {
		if r.dup {
			gotDup = append(gotDup, r.off)
		}

		if r.resume {
			gotResume = append(gotResume, r.off)
		}
	}

	if !reflect.DeepEqual(gotDup, []int64{3, 2, 5}) || !reflect.DeepEqual(gotResume, []int64{3}) {
		t.Fatalf("dup=%v resume=%v; want dup [3 2 5], resume [3]", gotDup, gotResume)
	}
}

// TestDelayFromHeartbeat runs analyze on a hand-made capture: one session snapshot,
// a heartbeat with virtual_now, then two live detections at 60× speed.
func TestDelayFromHeartbeat(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	capture := filepath.Join(dir, "c.ndjson")

	conns := `{"wall_rfc3339nano":"2026-11-18T10:00:00Z","mono_ns":0,"conn_seq":1,"kind":"session","status":200,"session":{"profile":"practice","speed":60,"virtual_now":"2026-11-12T03:00:00Z","latest_offset":10}}
{"wall_rfc3339nano":"2026-11-18T10:00:00Z","mono_ns":0,"conn_seq":1,"kind":"connect","url":"/v1/stream?from=10"}
`
	events := `{"wall_rfc3339nano":"2026-11-18T10:00:01Z","mono_ns":1000000000,"conn_seq":1,"sse_id":"11","event":"heartbeat","data_raw":"{\"virtual_now\":\"2026-11-12T03:01:00Z\"}","has_data":true}
{"wall_rfc3339nano":"2026-11-18T10:00:02Z","mono_ns":2000000000,"conn_seq":1,"sse_id":"12","event":"detection","data_raw":"{\"satellite\":\"N20\",\"acq_date\":\"2026-11-12\",\"acq_time\":\"0243\",\"latitude\":-2.51230,\"longitude\":113.80440,\"confidence\":\"h\"}","has_data":true}
{"wall_rfc3339nano":"2026-11-18T10:00:02Z","mono_ns":2000000000,"conn_seq":1,"sse_id":"12","event":"detection","data_raw":"{}","has_data":true}
`

	writeFile(t, capture, events)
	writeFile(t, connPath(capture), conns)

	a, err := loadAnalysis(analyzeOpts{in: capture, passGap: 20 * time.Minute, burstGap: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	d := a.ofKind(kindDetection)
	if len(d) != 1 {
		t.Fatalf("want 1 unique detection (the repeat of id 12 dropped), got %d", len(d))
	}

	// Heartbeat at 03:01:00 feed time, 1 s wall later at 60× is 03:02:00; acq is 02:43.
	if got := d[0].virt.Sub(d[0].acq); got != 19*time.Minute || d[0].replay {
		t.Fatalf("delay %s replay %v; want 19m live", got, d[0].replay)
	}

	if d[0].key != "N20|2026-11-12|0243|-2.51230|113.80440" {
		t.Fatalf("detection key %q must keep the raw text", d[0].key)
	}

	var out bytes.Buffer
	if err := a.report(&out); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(out.String(), "| satellite N20 | 1 | 19m0s |") {
		t.Fatalf("report misses the per-satellite delay row:\n%s", out.String())
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
