package main

import (
	"errors"
	"fmt"
	"time"
)

// Turns raw source records into HazardEvents, working on generic maps. Clock and ID generator are fields so tests can pin them.
type mapper struct {
	now   func() time.Time
	newID func() string
}

func newMapper() *mapper {
	return &mapper{now: time.Now, newID: newHazardID}
}

// One row of the volcano reference table. PVMBG only sends volcano_id.
type volcano struct {
	name     string
	lat, lon float64
}

// The volcano_id values the PVMBG mock draws from.
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

const (
	maxFutureSkew = 24 * time.Hour
	maxRefIDLen   = 255 // hazard_events.source_ref_id is VARCHAR(255)
)

func num(r map[string]any, k string) (float64, bool) {
	f, ok := r[k].(float64)

	return f, ok
}

// Field policy: a source field that feeds a canonical field is consumed, every other field goes to attributes under its own key, and a missing canonical field stays null.

// Fills what every source shares. A record without an id or a usable time is rejected.
func (m *mapper) base(source, hazardType string, r map[string]any, idKey, timeKey string, used map[string]bool) (HazardEvent, error) {
	id, ok := str(r, idKey)
	if !ok {
		return HazardEvent{}, fmt.Errorf("%s record without %s", source, idKey)
	}

	ts, _ := str(r, timeKey)

	at, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return HazardEvent{}, fmt.Errorf("%s record %s: bad %s: %w", source, id, timeKey, err)
	}

	// A report from the far future would sort to the top for good, and an over-long id is a database error.
	if at.After(m.now().Add(maxFutureSkew)) || len(id) > maxRefIDLen {
		return HazardEvent{}, fmt.Errorf("%s record %.40s: implausible id or %s", source, id, timeKey)
	}

	used[idKey], used[timeKey] = true, true

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

// Maps every record and reports rejected ones together, so one bad record never fails the whole poll.
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

// Maps one BMKG poll: seismic events plus tsunami warnings, correlated through related_event_id.
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
	used := map[string]bool{}

	h, err := m.base(sourceBMKG, typeSeismic, r, "event_id", "occurred_at", used)
	if err != nil {
		return HazardEvent{}, err
	}

	if s, ok := str(r, "region_name"); ok {
		h.AreaName, used["region_name"] = &s, true
	}

	if v, ok := num(r, "epicenter_lat"); ok && v >= -90 && v <= 90 {
		h.Latitude, used["epicenter_lat"] = &v, true
	}

	if v, ok := num(r, "epicenter_lon"); ok && v >= -180 && v <= 180 {
		h.Longitude, used["epicenter_lon"] = &v, true
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

	h.addLeftovers(r, used)

	return h, nil
}

// The most severe related warning and its severity. If no warning has a known level, the first one comes back with an empty severity.
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

// Severity from magnitude when no warning decides it.
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

// Maps one PVMBG poll.
func (m *mapper) mapVolcanic(reports []map[string]any) ([]HazardEvent, error) {
	return mapAll(reports, m.volcanic)
}

func (m *mapper) volcanic(r map[string]any) (HazardEvent, error) {
	used := map[string]bool{}

	h, err := m.base(sourcePVMBG, typeVolcanic, r, "report_id", "reported_at", used)
	if err != nil {
		return HazardEvent{}, err
	}

	id, _ := str(r, "volcano_id")
	if v, ok := volcanoRef[id]; ok {
		h.AreaName, h.Latitude, h.Longitude = &v.name, &v.lat, &v.lon
		used["volcano_id"] = true
	}

	level, _ := str(r, "alert_level")
	if s, ok := alertSeverity[level]; ok {
		h.Severity = &s
		used["alert_level"] = true
	}

	h.addLeftovers(r, used)

	return h, nil
}

// Puts every unconsumed field into attributes.
func (h *HazardEvent) addLeftovers(r map[string]any, used map[string]bool) {
	for k, v := range r {
		if !used[k] {
			h.Attributes[k] = v
		}
	}
}
