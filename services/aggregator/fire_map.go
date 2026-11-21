package main

import (
	"maps"
	"math"
	"slices"
	"time"
)

// Severity thresholds from "Aturan Pemetaan Tambahan": AWAS at ≥ 20 detections or ≥ 100 MW total FRP, SIAGA at ≥ 6 or ≥ 30 MW, otherwise WASPADA.
const (
	fireAwasCount  = 20
	fireAwasFRP    = 100.0
	fireSiagaCount = 6
	fireSiagaFRP   = 30.0
)

var confidenceRank = map[string]int{confLow: 1, confNominal: 2, confHigh: 3}

// mapFire turns an engine snapshot into the WILDFIRE HazardEvent to save (#311). The id and every field except ingestedAt come from the snapshot only, so the same feed always maps to the same event. ingestedAt only counts on the first save; the upsert never updates it.
func mapFire(s fireSnapshot, ingestedAt time.Time) HazardEvent {
	h := HazardEvent{
		HazardID:    hazardID(sourceSatelit, s.Name),
		Source:      sourceSatelit,
		SourceRefID: s.Name,
		HazardType:  typeWildfire,
		IngestedAt:  ingestedAt,
		Version:     s.Version,
	}

	if s.Withdrawn {
		at := time.Unix(s.WithdrawnAt, 0).UTC()
		h.WithdrawnAt = &at
	}

	if len(s.Dots) == 0 { // the engine never hands out an empty fire; this only avoids dividing by zero
		h.Attributes = map[string]any{"detection_count": 0}

		return h
	}

	var (
		sumLat, sumLon, sumFRP float64
		earliest               = s.Dots[0].Acq
		maxConf                string
		satellites             = map[string]struct{}{}
		keys                   = make([]string, 0, len(s.Dots))
	)

	// Dots come sorted by key, so the float sums add up in the same order on every run.
	for _, d := range s.Dots {
		sumLat, sumLon, sumFRP = sumLat+d.Lat, sumLon+d.Lon, sumFRP+d.FRP
		earliest = min(earliest, d.Acq)
		satellites[d.Satellite] = struct{}{}
		keys = append(keys, d.Key)

		if confidenceRank[d.Confidence] > confidenceRank[maxConf] {
			maxConf = d.Confidence
		}
	}

	n := len(s.Dots)
	lat, lon := roundTo(sumLat/float64(n), 5), roundTo(sumLon/float64(n), 5)
	totalFRP := roundTo(sumFRP, 2)
	severity := fireSeverity(n, totalFRP)
	area := nearestProvince(lat, lon)

	h.Severity, h.AreaName, h.Latitude, h.Longitude = &severity, &area, &lat, &lon
	h.OccurredAt = time.Unix(earliest, 0).UTC()
	h.Attributes = map[string]any{
		"detection_count": n,
		"satellites":      slices.Sorted(maps.Keys(satellites)),
		"max_confidence":  maxConf,
		"total_frp_mw":    totalFRP,
		"detection_keys":  keys,
	}

	if s.MergedInto != "" {
		h.Attributes["merged_into"] = s.MergedInto
	}

	return h
}

// fireSeverity uses the total FRP as reported (rounded to 2 decimals), so the severity always agrees with the attribute.
func fireSeverity(count int, totalFRP float64) string {
	switch {
	case count >= fireAwasCount || totalFRP >= fireAwasFRP:
		return severityAwas
	case count >= fireSiagaCount || totalFRP >= fireSiagaFRP:
		return severitySiaga
	default:
		return severityWaspada
	}
}

func roundTo(x float64, decimals int) float64 {
	p := math.Pow10(decimals)

	return math.Round(x*p) / p
}
