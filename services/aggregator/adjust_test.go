package main

import (
	"fmt"
	"maps"
	"slices"
	"testing"
)

const (
	adjCount       = "eruption_count_24h"
	adjVolcano     = "volcano_id"
	adjMarkRename  = "rename:eruption_count"
	adjMarkDefault = "default:volcano_id"
	adjMarkType    = "type:ash_column_height_m"
	adjSetting     = "rename:eruption_count=eruption_count_24h,type:ash_column_height_m,default:volcano_id=UNKNOWN"
)

func TestParseSchemaAdjust(t *testing.T) {
	t.Parallel()

	a := parseSchemaAdjust("rename:eruption_count=eruption_count_24h, type:ash_column_height_m ,default:volcano_id=UNKNOWN,default:eruption_count_24h=0")

	if a.rename["eruption_count"] != adjCount || !a.toNumber["ash_column_height_m"] || a.defaults[adjVolcano] != "UNKNOWN" || a.defaults[adjCount] != 0.0 {
		t.Fatalf("parsed %+v", a)
	}

	for _, raw := range []string{
		"",
		"  ",
		"rename:eruption_count",
		"rename:eruption_count=nothing",
		"rename:volcano_id=alert_level",
		"type:volcano_id",
		"type:ash_column_height_m=number",
		"default:report_id=R1",
		"default:reported_at=2026-10-04T09:00:00Z",
		"default:eruption_count_24h=many",
		"default:eruption_count_24h=NaN",
		"default:volcano_id=",
		"shrug:volcano_id",
		adjVolcano,
	} {
		if got := parseSchemaAdjust(raw); !got.empty() {
			t.Errorf("%q: want nothing applied, got %+v", raw, got)
		}
	}

	got := parseSchemaAdjust("shrug:x,type:ash_column_height_m,default:report_id=R1")
	if len(got.toNumber) != 1 || len(got.rename)+len(got.defaults) != 0 {
		t.Fatalf("a bad term must not drop the good ones: %+v", got)
	}
}

func TestAdjustApply(t *testing.T) {
	t.Parallel()

	a := parseSchemaAdjust(adjSetting)
	good := report("R1", "MERAPI", "Waspada")
	noCount := without(good, adjCount)

	cases := []struct {
		name  string
		rec   map[string]any
		want  map[string]any
		marks []string
	}{
		{"good record untouched", good, good, nil},
		{"renamed", with(noCount, "eruption_count", 3.0), with(good, adjCount, 3.0), []string{adjMarkRename}},
		{"renamed to null is left alone", with(noCount, "eruption_count", nil), with(noCount, "eruption_count", nil), nil},
		{"both names keep the contract value", with(good, "eruption_count", 9.0), with(good, "eruption_count", 9.0), nil},
		{"text number", with(good, "ash_column_height_m", "1200.5"), with(good, "ash_column_height_m", 1200.5), []string{adjMarkType}},
		{"text that is not a number", with(good, "ash_column_height_m", "high"), with(good, "ash_column_height_m", "high"), nil},
		{"NaN text", with(good, "ash_column_height_m", "NaN"), with(good, "ash_column_height_m", "NaN"), nil},
		{"Inf text", with(good, "ash_column_height_m", "Inf"), with(good, "ash_column_height_m", "Inf"), nil},
		{"missing default", without(good, adjVolcano), with(good, adjVolcano, "UNKNOWN"), []string{adjMarkDefault}},
		{"null default", with(good, adjVolcano, nil), with(good, adjVolcano, "UNKNOWN"), []string{adjMarkDefault}},
	}

	for _, c := range cases {
		before := maps.Clone(c.rec)
		got, marks := a.apply(c.rec)

		if !maps.Equal(got, c.want) || !slices.Equal(marks, c.marks) {
			t.Errorf("%s: got %v %v, want %v %v", c.name, got, marks, c.want, c.marks)
		}

		if !maps.Equal(c.rec, before) {
			t.Errorf("%s: the source record was changed", c.name)
		}
	}

	all := without(good, adjVolcano, adjCount)
	all["eruption_count"], all["ash_column_height_m"] = 3.0, "1"

	got, marks := a.apply(all)
	if got[adjCount] != 3.0 || got["ash_column_height_m"] != 1.0 || got[adjVolcano] != "UNKNOWN" || got["eruption_count"] != nil {
		t.Errorf("all three: got %v", got)
	}

	if want := []string{adjMarkDefault, adjMarkRename, adjMarkType}; !slices.Equal(marks, want) {
		t.Errorf("all three marks = %v, want %v", marks, want)
	}

	if got, marks := (schemaAdjust{}).apply(good); !maps.Equal(got, good) || marks != nil {
		t.Errorf("empty setting must return the record as it is: %v %v", got, marks)
	}
}

func TestVolcanicAdjustedRecordIsMarkedAndTheRestIsNot(t *testing.T) {
	t.Parallel()

	good := report("R1", "MERAPI", "Waspada")
	broken := without(good, adjVolcano, adjCount)
	broken["eruption_count"], broken["ash_column_height_m"] = 3.0, "1200.5"

	m := testMapper()
	if _, err := m.volcanic(broken); err == nil {
		t.Fatal("without the setting the three changes must still be parked")
	}

	m.adjust = parseSchemaAdjust(adjSetting)

	h, err := m.volcanic(broken)
	if err != nil {
		t.Fatalf("adjusted record: %v", err)
	}

	marks, ok := h.Attributes["schema_adjusted"].([]string)
	if want := []string{adjMarkDefault, adjMarkRename, adjMarkType}; !ok || !slices.Equal(marks, want) {
		t.Errorf("schema_adjusted = %v, want %v", h.Attributes["schema_adjusted"], want)
	}

	if h.Attributes[adjVolcano] != "UNKNOWN" || h.AreaName != nil {
		t.Errorf("a default volcano is raw data, not a reference row: %+v", h)
	}

	h, err = m.volcanic(good)
	if err != nil {
		t.Fatal(err)
	}

	if _, marked := h.Attributes["schema_adjusted"]; marked {
		t.Error("a correct record must not be marked")
	}

	_, err = m.volcanic(with(good, "ash_column_height_m", "high"))
	if err == nil || err.Error() != "schema_incompatible: field 'ash_column_height_m' changed from number to string" {
		t.Errorf("text that does not fit keeps today's reason, got %v", err)
	}
}

func parkedAllThree() []failedMessage {
	payloads := []string{
		`{"report_id":"R1","volcano_id":"MERAPI","alert_level":"Siaga","reported_at":"2026-10-01T09:00:00Z","ash_column_height_m":1200,"eruption_count":3}`,
		`{"report_id":"R2","volcano_id":"MERAPI","alert_level":"Siaga","reported_at":"2026-10-01T09:00:00Z","ash_column_height_m":"1200.5","eruption_count_24h":3}`,
		`{"report_id":"R3","alert_level":"Siaga","reported_at":"2026-10-01T09:00:00Z","ash_column_height_m":1200,"eruption_count_24h":3}`,
	}

	rows := make([]failedMessage, len(payloads))

	for i, p := range payloads {
		rows[i] = newFailedMessage(sourcePVMBG, []byte(p), "schema_incompatible: x", false, replayBase)
		rows[i].MessageID = fmt.Sprintf("m%d", i)
	}

	return rows
}

func (g *replayRig) replay(t *testing.T) replayReport {
	t.Helper()

	rep, err := g.r.run(t.Context(), sourcePVMBG, 100)
	if err != nil {
		t.Fatal(err)
	}

	return rep
}

func TestReplayRecoversAllThreeChangesOnceTheSettingIsApplied(t *testing.T) {
	t.Parallel()

	g := newReplayRig(parkedAllThree()...)

	if rep := g.replay(t); rep.Recovered != 0 || rep.StillParked != 3 || len(g.writes) != 0 {
		t.Fatalf("without the setting nothing may recover: %+v", rep)
	}

	g.r.m.adjust = parseSchemaAdjust(adjSetting)

	if rep := g.replay(t); rep.Recovered != 3 || rep.StillParked != 0 || len(g.writes) != 3 || len(g.pubs) != 3 {
		t.Fatalf("with the setting all three recover: %+v writes=%d", rep, len(g.writes))
	}

	if rep := g.replay(t); rep.Scanned != 0 || len(g.writes) != 3 {
		t.Fatalf("a second run must do nothing: %+v", rep)
	}

	for _, h := range g.writes {
		if _, marked := h.Attributes["schema_adjusted"]; !marked {
			t.Errorf("%s recovered without a mark", h.SourceRefID)
		}
	}
}
