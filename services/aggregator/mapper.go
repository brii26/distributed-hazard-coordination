package main

import (
	"errors"
	"fmt"
	"time"
)

// mapper turns raw source records into HazardEvents (doc/TRD_M1.md 3.2). It
// reads records as generic maps, the shape the pollers produce, so the
// unknown/changed-field policy (#19) can be applied on top without a rewrite.
// The clock and ID generator are fields so tests can pin them.
type mapper struct {
	now   func() time.Time
	newID func() string
}

func newMapper() *mapper {
	return &mapper{now: time.Now, newID: newHazardID}
}

// volcano is one row of the static reference table: PVMBG sends only a
// volcano_id, so the name and coordinates live here.
type volcano struct {
	name     string
	lat, lon float64
}

// volcanoRef covers the volcano_id values the PVMBG mock draws from.
var volcanoRef = map[string]volcano{
	"MERAPI":   {"Gunung Merapi", -7.5407, 110.4457},
	"SEMERU":   {"Gunung Semeru", -8.1080, 112.9220},
	"BROMO":    {"Gunung Bromo", -7.9425, 112.9530},
	"SINABUNG": {"Gunung Sinabung", 3.1700, 98.3920},
	"KRAKATAU": {"Gunung Anak Krakatau", -6.1020, 105.4230},
	"AGUNG":    {"Gunung Agung", -8.3430, 115.5080},
	"RINJANI":  {"Gunung Rinjani", -8.4110, 116.4570},
	"KELUD":    {"Gunung Kelud", -7.9300, 112.3080},
}

var (
	alertSeverity  = map[string]string{"Normal": severityNormal, "Waspada": severityWaspada, "Siaga": severitySiaga, "Awas": severityAwas}
	threatSeverity = map[string]string{"Waspada": severityWaspada, "Siaga": severitySiaga, "Awas": severityAwas}
	severityRank   = map[string]int{severityNormal: 0, severityWaspada: 1, severitySiaga: 2, severityAwas: 3}
)

func str(r map[string]any, k string) (string, bool) {
	s, ok := r[k].(string)

	return s, ok && s != ""
}

func num(r map[string]any, k string) (float64, bool) {
	f, ok := r[k].(float64)

	return f, ok
}

// base fills what every source shares. A record with no id or no usable time
// cannot be stored (both are NOT NULL), so it is rejected.
func (m *mapper) base(source, hazardType string, r map[string]any, idKey, timeKey string) (HazardEvent, error) {
	id, ok := str(r, idKey)
	if !ok {
		return HazardEvent{}, fmt.Errorf("%s record without %s", source, idKey)
	}

	ts, _ := str(r, timeKey)

	at, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return HazardEvent{}, fmt.Errorf("%s record %s: bad %s: %w", source, id, timeKey, err)
	}

	return HazardEvent{
		HazardID:    m.newID(),
		Source:      source,
		SourceRefID: id,
		HazardType:  hazardType,
		OccurredAt:  at,
		IngestedAt:  m.now(),
		Attributes:  map[string]any{},
	}, nil
}

// mapAll maps every record, skipping the ones fn rejects and reporting them
// together, so one bad record never costs the rest of the poll.
func mapAll(records []map[string]any, fn func(map[string]any) (HazardEvent, error)) ([]HazardEvent, error) {
	var (
		out  []HazardEvent
		errs []error
	)

	for _, r := range records {
		h, err := fn(r)
		if err != nil {
			errs = append(errs, err)

			continue
		}

		out = append(out, h)
	}

	return out, errors.Join(errs...)
}

// mapSeismic maps one BMKG poll: the seismic events plus the tsunami warnings
// polled with them, correlated here through related_event_id.
func (m *mapper) mapSeismic(events, warnings []map[string]any) ([]HazardEvent, error) {
	byEvent := map[string][]map[string]any{}

	for _, w := range warnings {
		if id, ok := str(w, "related_event_id"); ok {
			byEvent[id] = append(byEvent[id], w)
		}
	}

	return mapAll(events, func(r map[string]any) (HazardEvent, error) {
		return m.seismic(r, byEvent)
	})
}

func (m *mapper) seismic(r map[string]any, warnings map[string][]map[string]any) (HazardEvent, error) {
	h, err := m.base(sourceBMKG, typeSeismic, r, "event_id", "occurred_at")
	if err != nil {
		return HazardEvent{}, err
	}

	if s, ok := str(r, "region_name"); ok {
		h.AreaName = &s
	}

	if v, ok := num(r, "epicenter_lat"); ok {
		h.Latitude = &v
	}

	if v, ok := num(r, "epicenter_lon"); ok {
		h.Longitude = &v
	}

	for _, k := range []string{"magnitude", "depth_km", "potential_tsunami"} {
		if v, ok := r[k]; ok {
			h.Attributes[k] = v
		}
	}

	top, sev := topWarning(warnings[h.SourceRefID])
	if top != nil {
		h.Attributes["tsunami_warning"] = top
	}

	if sev != "" {
		h.Severity = &sev
	} else {
		h.Severity = magnitudeSeverity(r)
	}

	return h, nil
}

// topWarning returns the most severe related warning and the severity its
// threat_level maps to. A warning is never dropped: if none has a known level,
// the first is returned with an empty severity.
func topWarning(related []map[string]any) (map[string]any, string) {
	var (
		best map[string]any
		sev  string
	)

	for _, w := range related {
		level, _ := str(w, "threat_level")

		if s, ok := threatSeverity[level]; ok && (best == nil || severityRank[s] > severityRank[sev]) {
			best, sev = w, s
		}
	}

	if best == nil && len(related) > 0 {
		best = related[0]
	}

	return best, sev
}

// magnitudeSeverity is the fallback when no warning decides the severity.
func magnitudeSeverity(r map[string]any) *string {
	mag, ok := num(r, "magnitude")
	if !ok {
		return nil
	}

	s := severitySiaga

	switch {
	case mag < 5.0:
		s = severityNormal
	case mag < 6.5:
		s = severityWaspada
	}

	return &s
}

// mapVolcanic maps one PVMBG poll.
func (m *mapper) mapVolcanic(reports []map[string]any) ([]HazardEvent, error) {
	return mapAll(reports, m.volcanic)
}

func (m *mapper) volcanic(r map[string]any) (HazardEvent, error) {
	h, err := m.base(sourcePVMBG, typeVolcanic, r, "report_id", "reported_at")
	if err != nil {
		return HazardEvent{}, err
	}

	// A field with no canonical counterpart goes to attributes instead of being lost.
	id, _ := str(r, "volcano_id")
	if v, ok := volcanoRef[id]; ok {
		h.AreaName, h.Latitude, h.Longitude = &v.name, &v.lat, &v.lon
	} else if raw, ok := r["volcano_id"]; ok {
		h.Attributes["volcano_id"] = raw
	}

	level, _ := str(r, "alert_level")
	if s, ok := alertSeverity[level]; ok {
		h.Severity = &s
	} else if raw, ok := r["alert_level"]; ok {
		h.Attributes["alert_level"] = raw
	}

	for _, k := range []string{"eruption_count_24h", "ash_column_height_m", "confidence_level"} {
		if v, ok := r[k]; ok {
			h.Attributes[k] = v
		}
	}

	return h, nil
}
