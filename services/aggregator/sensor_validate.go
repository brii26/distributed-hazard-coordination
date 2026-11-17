package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

const sourceSensorPantai = "SENSOR_PANTAI"

// Why an observation is set aside. The code is the start of the reason stored with the quarantined record.
const (
	reasonMissingField     = "missing_field"
	reasonBadType          = "bad_type"
	reasonBadQuality       = "bad_quality"
	reasonUnknownStation   = "unknown_station"
	reasonFutureObservedAt = "future_observed_at"
	reasonOutOfRange       = "out_of_range"
)

var allReasons = []string{
	reasonMissingField, reasonBadType, reasonBadQuality,
	reasonUnknownStation, reasonFutureObservedAt, reasonOutOfRange,
}

// Limits of the rules below. The ranges sit far outside real readings, so they only catch impossible values.
const (
	seaLevelAnomalyLimitM = 20.0
	waveHeightMaxM        = 30.0
	futureTolerance       = time.Second
)

// Quality values the contract allows.
var knownQualities = map[string]bool{"ok": true, "suspect": true}

type valueKind int

const (
	kindString valueKind = iota
	kindNumber
	kindBool
)

// One violation: the reason code, the field and a short detail.
type violation struct {
	Reason, Field, Detail string
}

func (v *violation) Error() string { return v.Reason + ": " + v.Detail }

type obsRule struct {
	field string
	kind  valueKind
	// Runs after the field is present with the right type. Nil means presence and type are enough.
	check func(s *sensorValidator, v any, generatedAt time.Time) *violation
}

// All rules of the observation contract in one place, checked in this order. Presence and type of every
// field are checked first, then the value checks.
var observationRules = []obsRule{
	{field: "observation_id", kind: kindString},
	{field: "station_id", kind: kindString, check: (*sensorValidator).knownStation},
	{field: "observed_at", kind: kindString, check: checkObservedAt},
	{field: "sea_level_anomaly_m", kind: kindNumber, check: inRange(-seaLevelAnomalyLimitM, seaLevelAnomalyLimitM)},
	{field: "wave_height_m", kind: kindNumber, check: inRange(0, waveHeightMaxM)},
	{field: "quality", kind: kindString, check: knownQuality},
	{field: "critical", kind: kindBool},
}

// Checks observations against the base contract. The station list comes from the sensor at run time.
type sensorValidator struct {
	mu       sync.RWMutex
	stations map[string]bool
}

func newSensorValidator(stationIDs []string) *sensorValidator {
	s := &sensorValidator{}
	s.setStations(stationIDs)

	return s
}

// Replaces the known stations. With no stations every observation fails, so nothing unchecked is stored.
func (s *sensorValidator) setStations(ids []string) {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.stations = set
}

// Returns the first violation of rec, or nil when it is valid. generatedAt is the sensor's own clock for
// the response; a zero value skips the future check. An absent field, null and an empty string all count as missing.
func (s *sensorValidator) validate(rec map[string]any, generatedAt time.Time) *violation {
	for _, r := range observationRules {
		v, ok := rec[r.field]
		if !ok || v == nil || v == "" {
			return &violation{reasonMissingField, r.field, r.field + " is missing"}
		}

		if !hasKind(v, r.kind) {
			return &violation{reasonBadType, r.field, fmt.Sprintf("%s is %s, want %s", r.field, jsonKind(v), kindName(r.kind))}
		}
	}

	for _, r := range observationRules {
		if r.check == nil {
			continue
		}

		if viol := r.check(s, rec[r.field], generatedAt); viol != nil {
			viol.Field = r.field

			return viol
		}
	}

	return nil
}

func (s *sensorValidator) knownStation(v any, _ time.Time) *violation {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if id, _ := v.(string); !s.stations[id] {
		return &violation{Reason: reasonUnknownStation, Detail: "station_id is not in the station list"}
	}

	return nil
}

func knownQuality(_ *sensorValidator, v any, _ time.Time) *violation {
	if q, _ := v.(string); !knownQualities[q] {
		return &violation{Reason: reasonBadQuality, Detail: "quality is not a known value"}
	}

	return nil
}

func checkObservedAt(_ *sensorValidator, v any, generatedAt time.Time) *violation {
	t, err := time.Parse(time.RFC3339Nano, v.(string)) //nolint:forcetypeassert // type checked before
	if err != nil {
		return &violation{Reason: reasonBadType, Detail: "observed_at is not an RFC 3339 time"}
	}

	if _, offset := t.Zone(); offset != 0 {
		return &violation{Reason: reasonBadType, Detail: "observed_at is not UTC"}
	}

	if !generatedAt.IsZero() && t.After(generatedAt.Add(futureTolerance)) {
		return &violation{Reason: reasonFutureObservedAt, Detail: "observed_at is after generated_at"}
	}

	return nil
}

func inRange(lo, hi float64) func(*sensorValidator, any, time.Time) *violation {
	return func(_ *sensorValidator, v any, _ time.Time) *violation {
		f, _ := asFloat(v)
		if math.IsNaN(f) || f < lo || f > hi {
			return &violation{Reason: reasonOutOfRange, Detail: fmt.Sprintf("value %v is outside %v to %v", f, lo, hi)}
		}

		return nil
	}
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		// A literal like 1e999 overflows to Inf with ErrRange. It is still a number, so the range check rejects it.
		f, err := n.Float64()

		return f, err == nil || errors.Is(err, strconv.ErrRange)
	}

	return 0, false
}

func hasKind(v any, k valueKind) bool {
	switch k {
	case kindString:
		_, ok := v.(string)

		return ok
	case kindNumber:
		_, ok := asFloat(v)

		return ok
	case kindBool:
		_, ok := v.(bool)

		return ok
	}

	return false
}

func kindName(k valueKind) string {
	return [...]string{"string", "number", "boolean"}[k]
}

// Builds the failed message row for a rejected observation. It is never retried, the data itself is wrong.
// raw is the observation as received and the reason is "<code>: <detail>".
func quarantineObservation(raw []byte, rec map[string]any, v *violation, receivedAt time.Time) failedMessage {
	m := newFailedMessage(sourceSensorPantai, raw, v.Error(), false, receivedAt)
	if id, ok := rec["observation_id"].(string); ok {
		m.SourceRefID = id
	}

	return m
}

// Counts rejected observations by reason, for /metrics.
type rejectCounter struct {
	mu sync.Mutex
	n  map[string]uint64
}

func newRejectCounter() *rejectCounter {
	c := &rejectCounter{n: make(map[string]uint64, len(allReasons))}
	for _, r := range allReasons {
		c.n[r] = 0
	}

	return c
}

func (c *rejectCounter) add(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.n[reason]++
}

func (c *rejectCounter) snapshot() map[string]uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make(map[string]uint64, len(c.n))
	maps.Copy(out, c.n)

	return out
}

// Writes the counter in the Prometheus text format, one series per reason.
func (c *rejectCounter) writeMetrics(w io.Writer) {
	snap := c.snapshot()
	reasons := make([]string, 0, len(snap))

	for r := range snap {
		reasons = append(reasons, r)
	}

	sort.Strings(reasons)

	_, _ = fmt.Fprint(w, "# HELP aggregator_sensor_rejected_total Sensor Pantai observations set aside, by reason.\n# TYPE aggregator_sensor_rejected_total counter\n")

	for _, r := range reasons {
		_, _ = fmt.Fprintf(w, "aggregator_sensor_rejected_total{reason=%q} %d\n", r, snap[r])
	}
}
