package main

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

var testGenerated = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func validObs() map[string]any {
	return map[string]any{
		"observation_id":      "o-1",
		"station_id":          "s-1",
		"observed_at":         "2026-01-01T11:00:00Z",
		"sea_level_anomaly_m": 0.4,
		"wave_height_m":       1.2,
		"quality":             "ok",
		"critical":            false,
	}
}

func TestSensorValidate(t *testing.T) {
	v := newSensorValidator([]string{"s-1"})

	tests := []struct {
		name   string
		mutate func(map[string]any)
		reason string
		field  string
	}{
		{"valid", func(map[string]any) {}, "", ""},
		{"suspect quality is valid", func(m map[string]any) { m["quality"] = "suspect" }, "", ""},
		{"observed_at equal to generated_at", func(m map[string]any) { m["observed_at"] = "2026-01-01T12:00:00Z" }, "", ""},
		{"missing id", func(m map[string]any) { delete(m, "observation_id") }, reasonMissingField, "observation_id"},
		{"null value", func(m map[string]any) { m["wave_height_m"] = nil }, reasonMissingField, "wave_height_m"},
		{"empty station", func(m map[string]any) { m["station_id"] = "" }, reasonMissingField, "station_id"},
		{"empty quality", func(m map[string]any) { m["quality"] = "" }, reasonMissingField, "quality"},
		{"observed_at with +00:00 is UTC", func(m map[string]any) { m["observed_at"] = "2026-01-01T11:00:00+00:00" }, "", ""},
		{"infinite wave height", func(m map[string]any) { m["wave_height_m"] = math.Inf(1) }, reasonOutOfRange, "wave_height_m"},
		{"NaN anomaly", func(m map[string]any) { m["sea_level_anomaly_m"] = math.NaN() }, reasonOutOfRange, "sea_level_anomaly_m"},
		{"overflowing JSON number", func(m map[string]any) { m["wave_height_m"] = json.Number("1e999") }, reasonOutOfRange, "wave_height_m"},
		{"number as string", func(m map[string]any) { m["wave_height_m"] = "1.2" }, reasonBadType, "wave_height_m"},
		{"critical as string", func(m map[string]any) { m["critical"] = "no" }, reasonBadType, "critical"},
		{"observed_at not a time", func(m map[string]any) { m["observed_at"] = "yesterday" }, reasonBadType, "observed_at"},
		{"observed_at not UTC", func(m map[string]any) { m["observed_at"] = "2026-01-01T18:00:00+07:00" }, reasonBadType, "observed_at"},
		{"unknown quality", func(m map[string]any) { m["quality"] = "great" }, reasonBadQuality, "quality"},
		{"unknown station", func(m map[string]any) { m["station_id"] = "s-9" }, reasonUnknownStation, "station_id"},
		{"future observed_at", func(m map[string]any) { m["observed_at"] = "2026-01-01T12:05:00Z" }, reasonFutureObservedAt, "observed_at"},
		{"wave too high", func(m map[string]any) { m["wave_height_m"] = 99.0 }, reasonOutOfRange, "wave_height_m"},
		{"negative wave", func(m map[string]any) { m["wave_height_m"] = -0.5 }, reasonOutOfRange, "wave_height_m"},
		{"anomaly too low", func(m map[string]any) { m["sea_level_anomaly_m"] = -50.0 }, reasonOutOfRange, "sea_level_anomaly_m"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := validObs()
			tc.mutate(rec)

			got := v.validate(rec, testGenerated)
			if tc.reason == "" {
				if got != nil {
					t.Fatalf("want valid, got %v", got)
				}

				return
			}

			if got == nil || got.Reason != tc.reason || got.Field != tc.field {
				t.Fatalf("want %s on %s, got %+v", tc.reason, tc.field, got)
			}
		})
	}
}

func TestSensorValidateZeroGeneratedAtSkipsFutureCheck(t *testing.T) {
	v := newSensorValidator([]string{"s-1"})
	rec := validObs()
	rec["observed_at"] = "2099-01-01T00:00:00Z"

	if got := v.validate(rec, time.Time{}); got != nil {
		t.Fatalf("want valid, got %v", got)
	}
}

func TestSensorValidateNoStationsRejectsAll(t *testing.T) {
	v := newSensorValidator(nil)

	if got := v.validate(validObs(), testGenerated); got == nil || got.Reason != reasonUnknownStation {
		t.Fatalf("want unknown_station, got %+v", got)
	}
}

func TestQuarantineObservationIsNotRetryable(t *testing.T) {
	rec := validObs()
	rec["quality"] = "bad"
	viol := newSensorValidator([]string{"s-1"}).validate(rec, testGenerated)

	m := quarantineObservation([]byte(`{"observation_id":"o-1"}`), rec, viol, testGenerated)
	if m.Retryable || m.Source != sourceSensorPantai || m.SourceRefID != "o-1" || !strings.HasPrefix(m.Reason, reasonBadQuality+":") {
		t.Fatalf("unexpected row: %+v", m)
	}
}

func TestRejectCounterMetrics(t *testing.T) {
	c := newRejectCounter()
	c.add(reasonOutOfRange)
	c.add(reasonOutOfRange)

	var b bytes.Buffer

	c.writeMetrics(&b)

	out := b.String()
	if !strings.Contains(out, `aggregator_sensor_rejected_total{reason="out_of_range"} 2`) ||
		!strings.Contains(out, `aggregator_sensor_rejected_total{reason="missing_field"} 0`) {
		t.Fatalf("unexpected metrics:\n%s", out)
	}
}

func TestValidatorHasNoPracticeConstants(t *testing.T) {
	for _, forbidden := range []string{"stn-", "obs-0", "PANTAI_SEED"} {
		if strings.Contains(sensorValidateSource(t), forbidden) {
			t.Fatalf("validator source contains %q", forbidden)
		}
	}
}

func sensorValidateSource(t *testing.T) string {
	t.Helper()

	b, err := os.ReadFile("sensor_validate.go")
	if err != nil {
		t.Fatal(err)
	}

	return string(b)
}
