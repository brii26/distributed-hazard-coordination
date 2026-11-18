package fire

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// kmToDegLat converts a north-south distance in km to degrees of latitude,
// using the same earth radius as the code under test.
func kmToDegLat(km float64) float64 { return km / earthRadiusKm * 180 / math.Pi }

func num(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

// det builds a detection at lat, lon on the given date and HHMM.
func det(sat, date string, hhmm int, lat, lon float64, conf string) Detection {
	return Detection{
		Satellite: sat, AcqDate: date, AcqTime: strconv.Itoa(hhmm),
		Latitude: num(lat), Longitude: num(lon), Confidence: conf, FRP: 1,
	}
}

// at is a detection on a fixed day at the given latitude offset in km north of
// (-2.5, 115.0) and minutes after 00:00.
func at(sat string, northKm float64, minutes int, conf string) Detection {
	return det(sat, "2026-11-27", minutes/60*100+minutes%60, -2.5+kmToDegLat(northKm), 115.0, conf)
}

// sizes returns the fire sizes in output order.
func sizes(r Result) []int {
	out := make([]int, 0, len(r.Fires))
	for _, f := range r.Fires {
		out = append(out, len(f.Detections))
	}

	return out
}

func TestCluster_Rules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		in         []Detection
		wantSizes  []int
		wantSingle int
		wantDup    int
		wantLow    int
		wantBad    int
	}{
		{name: "empty input", in: nil, wantSizes: []int{}},
		{name: "single point is not a fire", in: []Detection{at("N", 0, 60, "n")}, wantSizes: []int{}, wantSingle: 1},
		{
			name:      "two close points make a fire",
			in:        []Detection{at("N", 0, 60, "n"), at("N", 0.5, 70, "h")},
			wantSizes: []int{2},
		},
		{
			name:       "duplicate key counts once",
			in:         []Detection{at("N", 0, 60, "n"), at("N", 0, 60, "n")},
			wantSizes:  []int{},
			wantSingle: 1, wantDup: 1,
		},
		{
			name:      "duplicate key does not inflate a fire",
			in:        []Detection{at("N", 0, 60, "n"), at("N", 0, 60, "n"), at("N", 0.5, 60, "n")},
			wantSizes: []int{2}, wantDup: 1,
		},
		{
			name:       "low confidence is excluded",
			in:         []Detection{at("N", 0, 60, "l"), at("N", 0.5, 60, "n")},
			wantSizes:  []int{},
			wantSingle: 1, wantLow: 1,
		},
		{
			name:      "low confidence does not bridge a chain",
			in:        []Detection{at("N", 0, 60, "n"), at("N", 0.8, 60, "l"), at("N", 1.6, 60, "n")},
			wantSizes: []int{}, wantSingle: 2, wantLow: 1,
		},
		{
			name:      "chain A to B to C joins although A to C is over 1 km",
			in:        []Detection{at("N", 0, 60, "n"), at("N", 0.8, 60, "n"), at("N", 1.6, 60, "n")},
			wantSizes: []int{3},
		},
		{
			name:      "distance exactly 1.0 km is connected",
			in:        []Detection{at("N", 0, 60, "n"), at("N", 1.0, 60, "n")},
			wantSizes: []int{2},
		},
		{
			name:       "distance 1.0001 km is not connected",
			in:         []Detection{at("N", 0, 60, "n"), at("N", 1.0001, 60, "n")},
			wantSizes:  []int{},
			wantSingle: 2,
		},
		{
			name:      "time gap exactly 360 minutes is connected",
			in:        []Detection{at("N", 0, 0, "n"), at("N", 0, 360, "n")},
			wantSizes: []int{2},
		},
		{
			name:       "time gap 361 minutes is not connected",
			in:         []Detection{at("N", 0, 0, "n"), at("N", 0, 361, "n")},
			wantSizes:  []int{},
			wantSingle: 2,
		},
		{
			name:      "different satellites can join",
			in:        []Detection{at("N", 0, 60, "n"), at("N20", 0.3, 65, "n"), at("N21", 0.6, 70, "h")},
			wantSizes: []int{3},
		},
		{
			name: "far apart groups stay separate fires and singles are counted",
			in: []Detection{
				at("N", 0, 60, "n"), at("N", 0.5, 60, "n"),
				at("N", 50, 60, "n"), at("N", 50.5, 60, "n"),
				at("N", 100, 60, "n"),
			},
			wantSizes: []int{2, 2}, wantSingle: 1,
		},
		{
			name:       "invalid detections are counted and skipped",
			in:         []Detection{{Satellite: "N", AcqDate: "bad", AcqTime: "1", Latitude: "0", Longitude: "0"}},
			wantSizes:  []int{},
			wantBad:    1,
			wantSingle: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := Cluster(tt.in)

			if !reflect.DeepEqual(sizes(got), tt.wantSizes) {
				t.Errorf("fire sizes = %v, want %v", sizes(got), tt.wantSizes)
			}

			if got.Singles != tt.wantSingle || got.Duplicates != tt.wantDup ||
				got.LowConfidence != tt.wantLow || got.Invalid != tt.wantBad {
				t.Errorf("singles/dups/low/invalid = %d/%d/%d/%d, want %d/%d/%d/%d",
					got.Singles, got.Duplicates, got.LowConfidence, got.Invalid,
					tt.wantSingle, tt.wantDup, tt.wantLow, tt.wantBad)
			}
		})
	}
}

func TestCluster_AcqTimePadding(t *testing.T) {
	t.Parallel()

	// "243" is 02:43, so it is 360 minutes before 08:43 and connected, and
	// 361 minutes before 08:44. Reading "243" as 24:3 or 2:43 wrongly would differ.
	a := det("N", "2026-11-27", 243, -2.5, 115, "n")

	if got := Cluster([]Detection{a, det("N", "2026-11-27", 843, -2.5, 115, "n")}); len(got.Fires) != 1 {
		t.Errorf("02:43 and 08:43 should connect, got %v", sizes(got))
	}

	if got := Cluster([]Detection{a, det("N", "2026-11-27", 844, -2.5, 115, "n")}); len(got.Fires) != 0 {
		t.Errorf("02:43 and 08:44 should not connect, got %v", sizes(got))
	}

	// Midnight is "0" and 00:25 is "25".
	b := det("N", "2026-11-27", 0, -2.5, 115, "n")
	c := det("N", "2026-11-27", 25, -2.5, 115, "n")

	if got := Cluster([]Detection{b, c}); len(got.Fires) != 1 {
		t.Errorf("00:00 and 00:25 should connect, got %v", sizes(got))
	}
}

func TestCluster_AcrossMidnight(t *testing.T) {
	t.Parallel()

	a := det("N", "2026-11-27", 2350, -2.5, 115, "n")
	b := det("N", "2026-11-28", 10, -2.5, 115, "n")

	if got := Cluster([]Detection{a, b}); len(got.Fires) != 1 {
		t.Errorf("23:50 and 00:10 next day should connect, got %v", sizes(got))
	}
}

func TestCluster_EastWestAndAntimeridian(t *testing.T) {
	t.Parallel()

	// 0.0089 degrees of longitude at the equator is about 0.99 km.
	e1 := det("N", "2026-11-27", 100, 0, 179.9996, "n")
	e2 := det("N", "2026-11-27", 100, 0, -179.9996, "n")

	if got := Cluster([]Detection{e1, e2}); len(got.Fires) != 1 {
		t.Errorf("points across the antimeridian should connect, got %v", sizes(got))
	}

	// At 60 degrees north, 1 km is about 0.018 degrees of longitude.
	w1 := det("N", "2026-11-27", 100, 60, 10, "n")
	w2 := det("N", "2026-11-27", 100, 60, 10.017, "n")
	w3 := det("N", "2026-11-27", 100, 60, 10.019, "n")

	if got := Cluster([]Detection{w1, w2}); len(got.Fires) != 1 {
		t.Errorf("0.017 degrees at 60N is under 1 km and should connect, got %v", sizes(got))
	}

	if got := Cluster([]Detection{w1, w3}); len(got.Fires) != 0 {
		t.Errorf("0.019 degrees at 60N is over 1 km and should not connect, got %v", sizes(got))
	}
}

// A grid answer must equal a brute force answer on random data.
func TestCluster_MatchesBruteForce(t *testing.T) {
	t.Parallel()

	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test data
		in := randomDetections(rng, 400, 0.15, 3)

		got := Cluster(in)
		want := bruteForce(in)

		if !reflect.DeepEqual(canonical(got), want) {
			t.Fatalf("seed %d: grid result differs from brute force", seed)
		}
	}
}

// Shuffled input gives the same output.
func TestCluster_ShuffleInvariant(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test data
	in := randomDetections(rng, 600, 0.2, 3)
	// Add duplicates, including ones that disagree, and low confidence copies.
	in = append(in, in[:50]...)
	dis := in[10]
	dis.FRP += 5
	dis.Confidence = "h"
	in = append(in, dis)

	want := Cluster(in)

	for i := range 25 {
		shuffled := slicesClone(in)
		rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })

		if got := Cluster(shuffled); !reflect.DeepEqual(got, want) {
			t.Fatalf("shuffle %d changed the result", i)
		}
	}
}

// Removing a bridge splits a fire and adding it merges the fires again.
func TestCluster_SplitAndMerge(t *testing.T) {
	t.Parallel()

	left := []Detection{at("N", 0, 60, "n"), at("N", 0.5, 60, "n")}
	right := []Detection{at("N", 2.0, 60, "n"), at("N", 2.5, 60, "n")}
	bridge := at("N", 1.25, 60, "h")

	split := Cluster(append(slicesClone(left), right...))
	if got := sizes(split); !reflect.DeepEqual(got, []int{2, 2}) {
		t.Fatalf("without the bridge want two fires of 2, got %v", got)
	}

	merged := Cluster(append(append(slicesClone(left), bridge), right...))
	if got := sizes(merged); !reflect.DeepEqual(got, []int{5}) {
		t.Fatalf("with the bridge want one fire of 5, got %v", got)
	}

	// The bridge turning low splits the fire again (an update to confidence "l").
	bridge.Confidence = "l"

	again := Cluster(append(append(slicesClone(left), bridge), right...))
	if got := sizes(again); !reflect.DeepEqual(got, []int{2, 2}) {
		t.Fatalf("with a low bridge want two fires of 2, got %v", got)
	}
}

func TestCluster_OutputOrderAndKeyAsReceived(t *testing.T) {
	t.Parallel()

	// "0243" and "243" are different keys as received, so both are kept.
	a := Detection{Satellite: "N", AcqDate: "2026-11-27", AcqTime: "243", Latitude: "-6.0e-5", Longitude: "100", Confidence: "n"}
	b := a
	b.AcqTime = "0243"

	got := Cluster([]Detection{b, a})
	if got.Duplicates != 0 || len(got.Fires) != 1 || len(got.Fires[0].Detections) != 2 {
		t.Fatalf("different key text should not dedupe, got %+v", got)
	}

	if got.Fires[0].Detections[0].Key() != b.Key() {
		t.Errorf("detections with equal time must be ordered by key")
	}

	if a.Key() != "N|2026-11-27|243|-6.0e-5|100" {
		t.Errorf("key = %q", a.Key())
	}
}

func TestCluster_DoesNotModifyInput(t *testing.T) {
	t.Parallel()

	in := []Detection{at("N", 0.5, 60, "n"), at("N", 0, 60, "l"), at("N", 0, 60, "n")}
	before := slicesClone(in)

	Cluster(in)

	if !reflect.DeepEqual(in, before) {
		t.Error("Cluster changed its input")
	}
}

// 100,000 detections must cluster in a few seconds.
func TestCluster_Scale100k(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic test data
	in := randomDetections(rng, 100_000, 8, 30)

	start := time.Now()
	res := Cluster(in)
	elapsed := time.Since(start)

	t.Logf("100000 detections: %d fires, %d singles, %v", len(res.Fires), res.Singles, elapsed)

	if len(res.Fires) == 0 {
		t.Error("expected some fires")
	}

	if elapsed > 5*time.Second {
		t.Errorf("took %v, want a few seconds at most", elapsed)
	}
}

func BenchmarkCluster100k(b *testing.B) {
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic test data
	in := randomDetections(rng, 100_000, 8, 30)

	b.ResetTimer()

	for range b.N {
		Cluster(in)
	}
}

// randomDetections spreads n detections over a box of boxDeg degrees and
// spanDays days, with mixed confidence and satellites.
func randomDetections(rng *rand.Rand, n int, boxDeg float64, spanDays int) []Detection {
	sats := []string{"N", "N20", "N21"}
	confs := []string{"l", "n", "n", "h"}
	out := make([]Detection, 0, n)
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

	for range n {
		day := base.AddDate(0, 0, rng.Intn(spanDays))
		hhmm := rng.Intn(24)*100 + rng.Intn(60)
		lat := -2.5 + rng.Float64()*boxDeg
		lon := 115 + rng.Float64()*boxDeg

		out = append(out, Detection{
			Satellite: sats[rng.Intn(len(sats))], AcqDate: day.Format(time.DateOnly),
			AcqTime: strconv.Itoa(hhmm), Latitude: fmt.Sprintf("%.5f", lat), Longitude: fmt.Sprintf("%.5f", lon),
			Confidence: confs[rng.Intn(len(confs))], FRP: rng.Float64() * 100,
		})
	}

	return out
}

// canonical turns a Result into sorted lists of keys for comparison.
func canonical(r Result) [][]string {
	out := make([][]string, 0, len(r.Fires))

	for _, f := range r.Fires {
		keys := make([]string, len(f.Detections))
		for i, d := range f.Detections {
			keys[i] = d.Key()
		}

		out = append(out, keys)
	}

	return out
}

// bruteForce applies the rules with an all pairs scan and a naive flood fill.
func bruteForce(in []Detection) [][]string {
	byKey, _ := dedupe(in)

	var pts []point

	for key, d := range byKey {
		if d.Confidence == LowConfidence {
			continue
		}

		if p, err := parse(key, d); err == nil {
			pts = append(pts, p)
		}
	}

	sortPoints(pts)

	seen := make([]bool, len(pts))
	groups := [][]string{}

	for i := range pts {
		if seen[i] {
			continue
		}

		comp := []int{i}
		seen[i] = true

		for q := 0; q < len(comp); q++ {
			for j := range pts {
				if !seen[j] && connected(pts[comp[q]], pts[j]) {
					seen[j] = true
					comp = append(comp, j)
				}
			}
		}

		if len(comp) < 2 {
			continue
		}

		sortInts(comp)

		keys := make([]string, len(comp))
		for n, idx := range comp {
			keys[n] = pts[idx].key
		}

		groups = append(groups, keys)
	}

	return groups
}
