package main

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

var fixedNow = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

func testMapper() *mapper {
	n := 0

	return &mapper{
		now: func() time.Time { return fixedNow },
		newID: func() string {
			n++

			return "id-" + strconv.Itoa(n)
		},
	}
}

func quake(id string, mag float64) map[string]any {
	return map[string]any{
		"event_id": id, "magnitude": mag, "depth_km": 12.5,
		"epicenter_lat": -8.5, "epicenter_lon": 112.5, "region_name": "Jawa Timur",
		"occurred_at": "2026-10-04T10:00:00Z", "potential_tsunami": true,
	}
}

func warning(eventID, level string) map[string]any {
	return map[string]any{
		"warning_id": "TSU-" + eventID, "related_event_id": eventID, "threat_level": level,
		"affected_zones": []any{"Jawa Timur"}, "estimated_arrival": "2026-10-04T10:30:00Z",
	}
}

func severityOf(t *testing.T, events, warnings []map[string]any) string {
	t.Helper()

	got, err := testMapper().mapSeismic(events, warnings)
	if err != nil || len(got) != 1 || got[0].Severity == nil {
		t.Fatalf("want one event with a severity, got %+v err=%v", got, err)
	}

	return *got[0].Severity
}

func TestSeismicSeverity(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name     string
		mag      float64
		warnings []map[string]any
		want     string
	}{
		{"Awas warning wins over low magnitude", 4.0, []map[string]any{warning("E1", "Awas")}, severityAwas},
		{"warning level is followed", 4.0, []map[string]any{warning("E1", "Siaga")}, severitySiaga},
		{"most severe of several warnings", 4.0, []map[string]any{warning("E1", "Waspada"), warning("E1", "Siaga")}, severitySiaga},
		{"warning for another event is ignored", 6.8, []map[string]any{warning("OTHER", "Awas")}, severitySiaga},
		{"no warning, 6.8 is SIAGA", 6.8, nil, severitySiaga},
		{"4.9 is NORMAL", 4.9, nil, severityNormal},
		{"5.0 is WASPADA", 5.0, nil, severityWaspada},
		{"6.4 is WASPADA", 6.4, nil, severityWaspada},
		{"6.5 is SIAGA", 6.5, nil, severitySiaga},
	} {
		if got := severityOf(t, []map[string]any{quake("E1", c.mag)}, c.warnings); got != c.want {
			t.Fatalf("%s: severity = %s, want %s", c.name, got, c.want)
		}
	}
}

func mapOneQuake(t *testing.T) HazardEvent {
	t.Helper()

	got, err := testMapper().mapSeismic([]map[string]any{quake("E1", 7.2)}, []map[string]any{warning("E1", "Siaga")})
	if err != nil || len(got) != 1 {
		t.Fatalf("got %+v err=%v", got, err)
	}

	return got[0]
}

func TestSeismicFieldsMapped(t *testing.T) {
	t.Parallel()

	h := mapOneQuake(t)
	if h.Source != "BMKG" || h.SourceRefID != "E1" || h.HazardType != "SEISMIC" || h.HazardID == "" {
		t.Fatalf("identity fields wrong: %+v", h)
	}

	if h.AreaName == nil || *h.AreaName != "Jawa Timur" || *h.Latitude != -8.5 || *h.Longitude != 112.5 {
		t.Fatalf("area or coordinates wrong: %+v", h)
	}

	if !h.OccurredAt.Equal(time.Date(2026, 10, 4, 10, 0, 0, 0, time.UTC)) || !h.IngestedAt.Equal(fixedNow) {
		t.Fatalf("times wrong: occurred %v ingested %v", h.OccurredAt, h.IngestedAt)
	}
}

func TestSeismicAttributesKeepRawFields(t *testing.T) {
	t.Parallel()

	h := mapOneQuake(t)

	for _, k := range []string{"magnitude", "depth_km", "potential_tsunami", "tsunami_warning"} {
		if _, ok := h.Attributes[k]; !ok {
			t.Fatalf("attribute %q missing: %v", k, h.Attributes)
		}
	}
}

func TestSeismicRejectsUnstorableRecordsOnly(t *testing.T) {
	t.Parallel()

	noID := quake("", 5)
	delete(noID, "event_id")

	badTime := quake("E2", 5)
	badTime["occurred_at"] = "yesterday"

	got, err := testMapper().mapSeismic([]map[string]any{noID, quake("E1", 5), badTime}, nil)
	if err == nil || len(got) != 1 || got[0].SourceRefID != "E1" {
		t.Fatalf("want only E1 mapped and an error for the other two, got %+v err=%v", got, err)
	}
}

func report(id, volcanoID, level string) map[string]any {
	return map[string]any{
		"report_id": id, "volcano_id": volcanoID, "alert_level": level,
		"eruption_count_24h": 4.0, "ash_column_height_m": 1200.0, "reported_at": "2026-10-04T09:00:00Z",
	}
}

func TestVolcanicKnownVolcanoAndSeverity(t *testing.T) {
	t.Parallel()

	got, err := testMapper().mapVolcanic([]map[string]any{report("R1", "MERAPI", "Siaga")})
	if err != nil || len(got) != 1 {
		t.Fatalf("got %+v err=%v", got, err)
	}

	h := got[0]
	if h.Source != "PVMBG" || h.SourceRefID != "R1" || h.HazardType != "VOLCANIC" || *h.Severity != severitySiaga {
		t.Fatalf("fields wrong: %+v", h)
	}

	if *h.AreaName != "Gunung Merapi" || *h.Latitude != -7.5407 || *h.Longitude != 110.4457 {
		t.Fatalf("reference table not applied: %+v", h)
	}

	if _, ok := h.Attributes["volcano_id"]; ok {
		t.Fatal("a resolved volcano_id belongs in area_name, not attributes")
	}

	if _, ok := h.Attributes["confidence_level"]; ok {
		t.Fatal("confidence_level must be absent when the source did not send it")
	}
}

func TestVolcanicConfidenceLevelOnlyWhenPresent(t *testing.T) {
	t.Parallel()

	legacy := report("R1", "BROMO", "Normal")
	extended := report("R2", "BROMO", "Normal")
	extended["confidence_level"] = 0.8

	got, err := testMapper().mapVolcanic([]map[string]any{legacy, extended})
	if err != nil || len(got) != 2 {
		t.Fatalf("got %+v err=%v", got, err)
	}

	if _, ok := got[0].Attributes["confidence_level"]; ok {
		t.Fatal("legacy report should not carry confidence_level")
	}

	if got[1].Attributes["confidence_level"] != 0.8 {
		t.Fatalf("extended report lost confidence_level: %v", got[1].Attributes)
	}
}

func TestVolcanicUnknownValuesGoToAttributes(t *testing.T) {
	t.Parallel()

	got, err := testMapper().mapVolcanic([]map[string]any{report("R1", "NOWHERE", "Dormant")})
	if err != nil || len(got) != 1 {
		t.Fatalf("got %+v err=%v", got, err)
	}

	h := got[0]
	if h.AreaName != nil || h.Latitude != nil || h.Severity != nil {
		t.Fatalf("unmappable values must stay null: %+v", h)
	}

	if h.Attributes["volcano_id"] != "NOWHERE" || h.Attributes["alert_level"] != "Dormant" {
		t.Fatalf("unmappable values must be kept in attributes: %v", h.Attributes)
	}
}

func TestHazardEventJSONShape(t *testing.T) {
	t.Parallel()

	got, _ := testMapper().mapVolcanic([]map[string]any{report("R1", "NOWHERE", "Siaga")})

	b, err := json.Marshal(got[0])
	if err != nil {
		t.Fatal(err)
	}

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"hazard_id", "source", "source_ref_id", "hazard_type", "severity", "area_name", "latitude", "longitude", "occurred_at", "ingested_at", "attributes"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("canonical field %q missing from %s", k, b)
		}
	}

	if m["area_name"] != nil || m["latitude"] != nil {
		t.Fatalf("missing values must serialize as null: %s", b)
	}
}

func TestNewHazardIDIsUUIDv4AndUnique(t *testing.T) {
	t.Parallel()

	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := map[string]bool{}

	for range 100 {
		id := newHazardID()
		if !re.MatchString(id) || seen[id] {
			t.Fatalf("bad or repeated id %q", id)
		}

		seen[id] = true
	}

	if strings.Count(newHazardID(), "-") != 4 {
		t.Fatal("not UUID shaped")
	}
}

func TestDefaultMapperUsesRealClockAndUUIDs(t *testing.T) {
	t.Parallel()

	before := time.Now()

	got, err := newMapper().mapVolcanic([]map[string]any{report("R1", "KELUD", "Awas")})
	if err != nil || len(got) != 1 {
		t.Fatalf("got %+v err=%v", got, err)
	}

	if got[0].IngestedAt.Before(before) || strings.Count(got[0].HazardID, "-") != 4 {
		t.Fatalf("default clock or id generator not used: %+v", got[0])
	}
}
