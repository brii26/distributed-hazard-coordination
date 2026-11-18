package fire

import "math"

// The grid avoids an all pairs scan. A cell is a latitude band of bandDeg
// degrees, a longitude slice of that band, and a time bucket of MaxGapMinutes.
// Any two connected detections lie in the same or neighbouring cells, so only
// those cells are compared.

const (
	// bandDeg is 0.01 degrees of latitude, about 1.11 km, wider than 1 km.
	bandDeg = 0.01
	// maxGridLat keeps cos(lat) away from zero near the poles.
	maxGridLat = 89.99
)

type cellKey struct {
	band, slice int
	bucket      int64
}

// bandWidth is the longitude width in degrees of a slice in a band. It is at
// least 1 km wide in the whole band, and it is capped at 360 degrees.
func bandWidth(band int) float64 {
	top := math.Max(math.Abs(float64(band)), math.Abs(float64(band+1))) * bandDeg
	top = math.Min(top, maxGridLat)

	return math.Min(360, bandDeg/math.Cos(top*math.Pi/180))
}

func bandOf(lat float64) int { return int(math.Floor(lat / bandDeg)) }

// lonSpan is the longitude difference in degrees that 1 km can cover at lat,
// with a small margin. Beyond maxGridLat it covers the whole circle.
func lonSpan(lat float64) float64 {
	l := math.Min(math.Abs(lat)+2*bandDeg, maxGridLat)

	return math.Min(360, 1.001*(MaxDistanceKm/earthRadiusKm)*180/math.Pi/math.Cos(l*math.Pi/180))
}

// sliceCount is the number of slices around the full circle of a band.
func sliceCount(w float64) int { return int(math.Ceil(360 / w)) }

func sliceOf(lon, w float64) int {
	n := sliceCount(w)
	s := int(math.Floor((lon+180)/w)) % n

	if s < 0 {
		s += n
	}

	return s
}

// link unions every connected pair and returns the union-find parent slice.
// pts must be sorted, so the result does not depend on the input order.
func link(pts []point) []int {
	parent := make([]int, len(pts))
	for i := range parent {
		parent[i] = i
	}

	grid := make(map[cellKey][]int, len(pts))

	for i, p := range pts {
		band := bandOf(p.lat)
		k := cellKey{band, sliceOf(p.lon, bandWidth(band)), floorDiv(p.minutes, MaxGapMinutes)}
		grid[k] = append(grid[k], i)
	}

	for i, p := range pts {
		bucket := floorDiv(p.minutes, MaxGapMinutes)
		span := lonSpan(p.lat)

		for band := bandOf(p.lat) - 1; band <= bandOf(p.lat)+1; band++ {
			w := bandWidth(band)
			n := sliceCount(w)
			lo := int(math.Floor((p.lon + 180 - span) / w))
			hi := int(math.Floor((p.lon + 180 + span) / w))

			// A span that reaches around the circle visits each slice once.
			if hi-lo+1 > n {
				lo, hi = 0, n-1
			}

			for s := lo; s <= hi; s++ {
				slice := ((s % n) + n) % n

				for b := bucket - 1; b <= bucket+1; b++ {
					for _, j := range grid[cellKey{band, slice, b}] {
						if j > i && connected(p, pts[j]) {
							union(parent, i, j)
						}
					}
				}
			}
		}
	}

	return parent
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}

	return q
}

func find(parent []int, i int) int {
	for parent[i] != i {
		parent[i] = parent[parent[i]]
		i = parent[i]
	}

	return i
}

// union joins the sets of i and j, keeping the smaller index as the root.
func union(parent []int, i, j int) {
	ri, rj := find(parent, i), find(parent, j)

	switch {
	case ri < rj:
		parent[rj] = ri
	case rj < ri:
		parent[ri] = rj
	}
}
