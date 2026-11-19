package main

import (
	"math"
	"slices"
)

// Link rule between two detections (Definisi Kebakaran rule 4, S8): at most 1.0 km by haversine on a sphere of R = 6371.0 km, and at most 360 minutes apart. Both bounds are inclusive.
const (
	earthRadiusKm   = 6371.0
	fireLinkKm      = 1.0
	fireLinkSeconds = 360 * 60
)

// A grid cell is 0.01° on each side. That is at least 1.11 km of latitude, so a linked dot is never more than one cell away in latitude. In longitude a cell shrinks with cos(latitude), so the search widens away from the equator.
const gridCellDeg = 0.01

type gridCell struct{ lat, lon int32 }

// fireGrid holds the dots that can be in a fire (not retracted, not low confidence), by cell, so finding a dot's links only looks at nearby cells.
type fireGrid map[gridCell][]*fireDot

func cellOf(lat, lon float64) gridCell {
	return gridCell{int32(math.Floor(lat / gridCellDeg)), int32(math.Floor(lon / gridCellDeg))}
}

func (g fireGrid) add(d *fireDot) {
	c := cellOf(d.lat, d.lon)
	g[c] = append(g[c], d)
}

func (g fireGrid) remove(d *fireDot) {
	c := cellOf(d.lat, d.lon)

	if i := slices.Index(g[c], d); i >= 0 {
		g[c] = slices.Delete(g[c], i, i+1)
	}

	if len(g[c]) == 0 {
		delete(g, c)
	}
}

// neighbours returns the dots in the grid that link to d, in a fixed order. d itself is left out.
func (g fireGrid) neighbours(d *fireDot) []*fireDot {
	var out []*fireDot

	c := cellOf(d.lat, d.lon)
	r := lonCellRadius(d.lat)

	for dy := int32(-1); dy <= 1; dy++ {
		for dx := -r; dx <= r; dx++ {
			for _, o := range g[gridCell{c.lat + dy, c.lon + dx}] {
				if o != d && linked(d, o) {
					out = append(out, o)
				}
			}
		}
	}

	return out
}

func linked(a, b *fireDot) bool {
	gap := a.acq - b.acq

	return gap <= fireLinkSeconds && gap >= -fireLinkSeconds && haversineKm(a.lat, a.lon, b.lat, b.lon) <= fireLinkKm
}

// lonCellRadius is how many cells east and west can hold a dot within 1 km of a dot at this latitude. It is 1 in Indonesia. The search does not wrap at ±180°.
func lonCellRadius(lat float64) int32 {
	const kmPerDeg = earthRadiusKm * math.Pi / 180

	farthest := math.Min(math.Abs(lat)+fireLinkKm/kmPerDeg, 89.9)
	deg := fireLinkKm / (kmPerDeg * math.Cos(farthest*math.Pi/180))

	return int32(math.Ceil(deg / gridCellDeg))
}

func haversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const rad = math.Pi / 180

	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)

	return 2 * earthRadiusKm * math.Asin(math.Sqrt(a))
}
