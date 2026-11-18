package main

import (
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
)

// writeQuality counts kinds, satellites, confidence values, repeated detection keys and bad records (F12).
func (a *analysis) writeQuality(m *md) {
	m.heading("Data quality (F12)")

	kinds, sats, confidence := map[string]int{}, map[string]int{}, map[string]int{}
	keyIDs := map[string][]string{}
	unknownFields, nonJSON, badDetections := map[string]int{}, map[string]int{}, 0
	box := newBBox()

	for _, r := range a.recs {
		for _, f := range r.Unknown {
			unknownFields[f]++
		}
	}

	for _, r := range a.unique() {
		kinds[r.kind]++

		if !r.jsonOK {
			nonJSON[r.kind]++

			continue
		}

		if r.kind != kindDetection {
			continue
		}

		sats[r.sat]++
		confidence[r.flat.text("confidence")]++

		if r.key == "" || !r.acqOK {
			badDetections++

			continue
		}

		keyIDs[r.key] = append(keyIDs[r.key], idText(r))
		box.add(r.flat.text("latitude"), r.flat.text("longitude"))
	}

	m.printf("Unique events per kind:\n\n")
	m.counts("kind", kinds)
	m.printf("Detections per satellite:\n\n")
	m.counts(fieldSatellite, sats)
	m.printf("Detection confidence values (raw text):\n\n")
	m.counts("confidence", confidence)

	repeated, extra := [][]string{}, 0

	for _, key := range slices.Sorted(maps.Keys(keyIDs)) {
		if ids := keyIDs[key]; len(ids) > 1 {
			extra += len(ids) - 1

			if len(repeated) < 10 {
				repeated = append(repeated, []string{"`" + key + "`", strings.Join(ids, ", ")})
			}
		}
	}

	// keyIDs is built from unique ids, so resume repeats are not counted here: these are re-sends at a new offset.
	m.printf("Detection keys re-sent under a new offset (resume repeats excluded): %d extra copies (first 10 keys below).\n\n", extra)
	m.table([]string{"detection key", "offsets"}, repeated)
	m.table(measureValue, [][]string{
		{"latitude range", box.latRange()},
		{"longitude range", box.lonRange()},
		{"detections missing a key field or with an unreadable acq time", strconv.Itoa(badDetections)},
		{"events whose data is not JSON, per kind", formatCounts(nonJSON)},
		{"SSE fields outside the spec", formatCounts(unknownFields)},
		{"unreadable capture lines", strconv.Itoa(a.badLines)},
	})
}

func formatCounts(c map[string]int) string {
	if len(c) == 0 {
		return "none"
	}

	parts := make([]string, 0, len(c))
	for _, row := range countRows(c) {
		parts = append(parts, row[0]+": "+row[1])
	}

	return strings.Join(parts, ", ")
}

type bbox struct{ minLat, maxLat, minLon, maxLon float64 }

func newBBox() *bbox {
	return &bbox{minLat: math.Inf(1), maxLat: math.Inf(-1), minLon: math.Inf(1), maxLon: math.Inf(-1)}
}

func (b *bbox) add(lat, lon string) {
	la, err1 := strconv.ParseFloat(lat, 64)
	lo, err2 := strconv.ParseFloat(lon, 64)

	if err1 != nil || err2 != nil {
		return
	}

	b.minLat, b.maxLat = min(b.minLat, la), max(b.maxLat, la)
	b.minLon, b.maxLon = min(b.minLon, lo), max(b.maxLon, lo)
}

func (b *bbox) latRange() string { return rangeText(b.minLat, b.maxLat) }
func (b *bbox) lonRange() string { return rangeText(b.minLon, b.maxLon) }

func rangeText(lo, hi float64) string {
	if math.IsInf(lo, 0) {
		return "-"
	}

	return strconv.FormatFloat(lo, 'f', -1, 64) + " … " + strconv.FormatFloat(hi, 'f', -1, 64)
}
