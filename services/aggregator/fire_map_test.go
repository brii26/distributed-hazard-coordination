package main

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func acqAt(hhmm string) int64 {
	t, _ := acqUnix("2026-11-12", hhmm)

	return t
}

// A fire shaped like the Lampiran A example: centre -2.51234, 113.80450 in Kalimantan Tengah, earliest detection 02:43.
func lampiranFire() fireSnapshot {
	return fireSnapshot{
		Name:    "fire-0a41c7d2e3f4",
		Version: 7,
		Dots: []dotView{ // sorted by key, as the engine hands them out
			{Key: "N20|2026-11-12|243|-2.51230|113.80440", Satellite: "N20", Confidence: confNominal, Acq: acqAt("243"), Lat: -2.51230, Lon: 113.80440, FRP: 1.115},
			{Key: "N20|2026-11-12|300|-2.51300|113.80500", Satellite: "N20", Confidence: confHigh, Acq: acqAt("300"), Lat: -2.51300, Lon: 113.80500, FRP: 2.2},
			{Key: "N|2026-11-12|250|-2.51172|113.80410", Satellite: "N", Confidence: confNominal, Acq: acqAt("250"), Lat: -2.51172, Lon: 113.80410, FRP: 3.333},
		},
	}
}

func TestMapFireMatchesTheAuditShape(t *testing.T) {
	t.Parallel()

	ingested := time.Date(2026, 11, 12, 2, 51, 8, 0, time.UTC)

	got, err := json.Marshal(mapFire(lampiranFire(), ingested))
	if err != nil {
		t.Fatal(err)
	}

	want := `{"hazard_id":"` + hazardID(sourceSatelit, "fire-0a41c7d2e3f4") + `","source":"SATELIT","source_ref_id":"fire-0a41c7d2e3f4",` +
		`"hazard_type":"WILDFIRE","severity":"WASPADA","area_name":"Kalimantan Tengah","latitude":-2.51234,"longitude":113.8045,` +
		`"occurred_at":"2026-11-12T02:43:00Z","ingested_at":"2026-11-12T02:51:08Z",` +
		`"attributes":{"detection_count":3,"detection_keys":["N20|2026-11-12|243|-2.51230|113.80440","N20|2026-11-12|300|-2.51300|113.80500","N|2026-11-12|250|-2.51172|113.80410"],` +
		`"max_confidence":"h","satellites":["N","N20"],"total_frp_mw":6.65},"version":7}`

	if string(got) != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
}

// #311: mapping the same fire again gives the same event; only ingested_at follows the caller.
func TestMapFireIsStable(t *testing.T) {
	t.Parallel()

	a := mapFire(lampiranFire(), time.Unix(1, 0))
	b := mapFire(lampiranFire(), time.Unix(2, 0))
	b.IngestedAt = a.IngestedAt

	if !reflect.DeepEqual(a, b) {
		t.Fatalf("two mappings differ:\n%+v\n%+v", a, b)
	}
}

func TestMapWithdrawnFire(t *testing.T) {
	t.Parallel()

	s := lampiranFire()
	s.Withdrawn, s.WithdrawnAt, s.MergedInto = true, acqAt("900"), "fire-111111111111"

	h := mapFire(s, time.Unix(1, 0))

	if h.WithdrawnAt == nil || !h.WithdrawnAt.Equal(time.Unix(acqAt("900"), 0)) || h.Attributes["merged_into"] != "fire-111111111111" {
		t.Fatalf("withdrawn fire: withdrawn_at %v, attributes %v", h.WithdrawnAt, h.Attributes)
	}
}

func TestFireSeverityBoundaries(t *testing.T) {
	t.Parallel()

	cases := []struct {
		count int
		frp   float64
		want  string
	}{
		{20, 0, severityAwas},
		{2, 100, severityAwas},
		{19, 99.99, severitySiaga},
		{6, 0, severitySiaga},
		{2, 30, severitySiaga},
		{5, 29.99, severityWaspada},
		{2, 0, severityWaspada},
	}

	for _, c := range cases {
		if got := fireSeverity(c.count, c.frp); got != c.want {
			t.Errorf("%d detections, %.2f MW: %s, want %s", c.count, c.frp, got, c.want)
		}
	}
}

func TestNearestProvince(t *testing.T) {
	t.Parallel()

	cases := map[string][2]float64{
		"DKI Jakarta":       {-6.20, 106.80},
		"Kalimantan Tengah": {-2.21, 113.92}, // Palangka Raya
		"Bali":              {-8.65, 115.22}, // Denpasar
		"Riau":              {0.51, 101.45},  // Pekanbaru
	}

	for want, at := range cases {
		if got := nearestProvince(at[0], at[1]); got != want {
			t.Errorf("%v: %s, want %s", at, got, want)
		}
	}

	names := map[string]bool{}
	for _, p := range provinces {
		names[p.name] = true
	}

	if len(provinces) != 38 || len(names) != 38 {
		t.Fatalf("%d provinces, %d distinct names; want 38", len(provinces), len(names))
	}
}
