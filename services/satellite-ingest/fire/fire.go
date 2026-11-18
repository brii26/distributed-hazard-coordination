// Package fire groups satellite hotspot detections into fires.
//
// Cluster is a pure function: no I/O, no clock, no randomness. The same set of
// detections gives the same Result in any input order.
//
// Rules (issue #310, spec "Definisi Kebakaran" points 1 to 5):
//
//  1. A detection key is "satellite|acq_date|acq_time|latitude|longitude" built
//     from the text exactly as received. The same key received again counts once.
//  2. Detection time is acq_date plus acq_time (HHMM, padded) in UTC.
//  3. Detections with confidence "l" are dropped.
//  4. Two detections are connected when the haversine distance is at most 1.0 km
//     and the time difference is at most 360 minutes. Connection is transitive.
//  5. A connected group of 2 or more detections is a fire (satellites may
//     differ). A single detection is not a fire, but singles are counted.
package fire

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	// MaxDistanceKm is the largest distance at which two detections connect.
	MaxDistanceKm = 1.0
	// MaxGapMinutes is the largest time difference at which two detections connect.
	MaxGapMinutes = 360
	// LowConfidence is the confidence value that is dropped.
	LowConfidence = "l"

	earthRadiusKm = 6371.0088
	// distanceSlackKm absorbs floating point error so that a pair exactly at
	// 1.0 km stays connected. It is one micrometre, far below any real input
	// (coordinates have 5 decimals, about 1 m).
	distanceSlackKm = 1e-9
)

// Detection is one hotspot as received from the feed. The key fields are kept
// as raw text so the key is exactly what the feed sent, with no rounding.
type Detection struct {
	Satellite  string  // for example "N20"
	AcqDate    string  // "2006-01-02"
	AcqTime    string  // HHMM without padding, for example "243" means 02:43
	Latitude   string  // raw number text, for example "-9.43923"
	Longitude  string  // raw number text
	Confidence string  // "l", "n" or "h"
	FRP        float64 // fire radiative power
}

// Key returns the detection key exactly as received.
func (d Detection) Key() string {
	return strings.Join([]string{d.Satellite, d.AcqDate, d.AcqTime, d.Latitude, d.Longitude}, "|")
}

// Fire is a connected group of 2 or more detections.
type Fire struct {
	// Detections are sorted by time, then key.
	Detections []Detection
}

// Result is the output of Cluster.
type Result struct {
	// Fires are sorted by their earliest detection (time, then key).
	Fires []Fire
	// Singles counts kept detections that connected to nothing.
	Singles int
	// Duplicates counts extra copies of a key that were ignored.
	Duplicates int
	// LowConfidence counts distinct detections dropped for low confidence.
	LowConfidence int
	// Invalid counts distinct detections whose date, time or coordinates
	// cannot be read. They take no part in clustering.
	Invalid int
}

// point is a parsed detection ready for clustering.
type point struct {
	det      Detection
	key      string
	minutes  int64 // minutes since the Unix epoch, UTC
	lat, lon float64
	cosLat   float64
}

// Cluster groups detections into fires. See the package comment for the rules.
func Cluster(in []Detection) Result {
	var res Result

	byKey, dups := dedupe(in)
	res.Duplicates = dups

	pts := make([]point, 0, len(byKey))

	for key, d := range byKey {
		if d.Confidence == LowConfidence {
			res.LowConfidence++

			continue
		}

		p, err := parse(key, d)
		if err != nil {
			res.Invalid++

			continue
		}

		pts = append(pts, p)
	}

	// A total order makes the rest independent of map and input order.
	slices.SortFunc(pts, comparePoints)

	parent := link(pts)

	groups := make(map[int][]int)

	for i := range pts {
		root := find(parent, i)
		groups[root] = append(groups[root], i)
	}

	// pts is sorted and union keeps the smallest index as root, so walking the
	// roots in index order lists fires by their earliest detection.
	for root := range pts {
		idx := groups[root]
		if len(idx) == 0 {
			continue
		}

		if len(idx) < 2 {
			res.Singles++

			continue
		}

		f := Fire{Detections: make([]Detection, len(idx))}
		for n, i := range idx {
			f.Detections[n] = pts[i].det
		}

		res.Fires = append(res.Fires, f)
	}

	return res
}

// dedupe keeps one detection per key. When copies of a key disagree, the copy
// with the highest FRP wins, then the highest confidence text, so the choice
// does not depend on input order. This tie break is a provisional choice, the
// final rule is to be settled in #314.
func dedupe(in []Detection) (map[string]Detection, int) {
	byKey := make(map[string]Detection, len(in))
	dups := 0

	for _, d := range in {
		key := d.Key()

		old, seen := byKey[key]
		if !seen {
			byKey[key] = d

			continue
		}

		dups++

		if d.FRP > old.FRP || (d.FRP == old.FRP && d.Confidence > old.Confidence) {
			byKey[key] = d
		}
	}

	return byKey, dups
}

func comparePoints(a, b point) int {
	if c := cmp.Compare(a.minutes, b.minutes); c != 0 {
		return c
	}

	return cmp.Compare(a.key, b.key)
}

var errInvalid = errors.New("invalid detection")

func parse(key string, d Detection) (point, error) {
	date, err := time.Parse(time.DateOnly, d.AcqDate)
	if err != nil {
		return point{}, fmt.Errorf("%w: date: %w", errInvalid, err)
	}

	hhmm, err := parseHHMM(d.AcqTime)
	if err != nil {
		return point{}, err
	}

	lat, err := parseCoord("latitude", d.Latitude, 90)
	if err != nil {
		return point{}, err
	}

	lon, err := parseCoord("longitude", d.Longitude, 180)
	if err != nil {
		return point{}, err
	}

	rad := lat * math.Pi / 180

	return point{
		det:     d,
		key:     key,
		minutes: date.Unix()/60 + int64(hhmm/100*60+hhmm%100),
		lat:     lat,
		lon:     lon,
		cosLat:  math.Cos(rad),
	}, nil
}

// parseHHMM reads an HHMM acquisition time such as "0930".
func parseHHMM(s string) (int, error) {
	hhmm, err := strconv.Atoi(s)
	if err != nil || hhmm < 0 || hhmm > 2359 || hhmm%100 > 59 {
		return 0, fmt.Errorf("%w: acq_time %q", errInvalid, s)
	}

	return hhmm, nil
}

// parseCoord reads a coordinate in degrees and checks it lies within +-limit.
func parseCoord(name, s string, limit float64) (float64, error) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || v < -limit || v > limit {
		return 0, fmt.Errorf("%w: %s %q", errInvalid, name, s)
	}

	return v, nil
}

// haversineKm is the great circle distance between two points in kilometres.
func haversineKm(a, b point) float64 {
	const rad = math.Pi / 180

	dLat := (b.lat - a.lat) * rad
	dLon := (b.lon - a.lon) * rad
	h := math.Pow(math.Sin(dLat/2), 2) + a.cosLat*b.cosLat*math.Pow(math.Sin(dLon/2), 2)

	return 2 * earthRadiusKm * math.Asin(math.Min(1, math.Sqrt(h)))
}

func connected(a, b point) bool {
	gap := a.minutes - b.minutes
	if gap < 0 {
		gap = -gap
	}

	return gap <= MaxGapMinutes && haversineKm(a, b) <= MaxDistanceKm+distanceSlackKm
}
