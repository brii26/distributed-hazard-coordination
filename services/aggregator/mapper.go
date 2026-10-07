package main

import (
	"errors"
	"fmt"
	"time"
)

// ubah record mentah source jadi HazardEvent dari map generik. Clock dan ID generator berupa field biar test bisa pin.
type mapper struct {
	now   func() time.Time
	newID func() string
}

func newMapper() *mapper {
	return &mapper{now: time.Now, newID: newHazardID}
}

// satu baris tabel referensi gunung, PVMBG cuma kirim volcano_id.
type volcano struct {
	name     string
	lat, lon float64
}

// volcano_id yang dipakai mock PVMBG.
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

// field policy: field source yang dipakai canonical nggak diulang di attributes, sisanya masuk attributes pake key sendiri, canonical yang hilang jadi null.

// isi field yang dipakai semua source, record tanpa id atau waktu valid ditolak.
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

// mapping semua record, yang ditolak dilaporkan bareng biar satu record rusak nggak bikin gagal poll.
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

// mapping satu poll BMKG: event seismik plus tsunami warning, dikorelasi lewat related_event_id.
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

	if v, ok := num(r, "epicenter_lat"); ok {
		h.Latitude, used["epicenter_lat"] = &v, true
	}

	if v, ok := num(r, "epicenter_lon"); ok {
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

// warning terkait paling parah beserta severity-nya, kalau nggak ada level yang dikenal warning pertama di-return pake severity kosong.
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

// severity dari magnitude kalau nggak ada warning yang nentuin.
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

// mapping satu poll PVMBG.
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

// masukin field yang belum dipakai ke attributes.
func (h *HazardEvent) addLeftovers(r map[string]any, used map[string]bool) {
	for k, v := range r {
		if !used[k] {
			h.Attributes[k] = v
		}
	}
}
