package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"time"
)

// The longest id the Aggregator accepts is 255 characters, so longer ones are hashed.
const maxIDLen = 255

// event is one earthquake in the shape the Aggregator's BMKG mapper reads.
type event struct {
	EventID          string  `json:"event_id"`
	Magnitude        float64 `json:"magnitude"`
	DepthKm          float64 `json:"depth_km"`
	EpicenterLat     float64 `json:"epicenter_lat"`
	EpicenterLon     float64 `json:"epicenter_lon"`
	RegionName       string  `json:"region_name"`
	OccurredAt       string  `json:"occurred_at"`
	PotentialTsunami bool    `json:"potential_tsunami"`
}

// columns names the catalog column behind each field. "time" may be two columns joined with a plus, such as Date+Time.
type columns map[string]string

func defaultColumns() columns {
	return columns{"id": "event_id", "time": "occurred_at", "mag": "magnitude", "depth": "depth_km", "lat": "epicenter_lat", "lon": "epicenter_lon", "region": "region_name"}
}

// parseColumns applies "time=Origin,lat=Latitude" over the defaults.
func parseColumns(spec string) (columns, error) {
	cols := defaultColumns()

	if spec == "" {
		return cols, nil
	}

	for pair := range strings.SplitSeq(spec, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if _, known := cols[k]; !ok || !known || v == "" {
			return nil, fmt.Errorf("bad --map entry %q, want field=Column with field one of id, time, mag, depth, lat, lon, region", pair)
		}

		cols[k] = v
	}

	return cols, nil
}

// readRows reads a CSV file with a header row, or a JSON array of objects, into rows of text.
func readRows(path string) ([]map[string]string, error) {
	data, err := os.ReadFile(path) //nolint:gosec // the path is the operator's own --file flag
	if err != nil {
		return nil, err
	}

	if t := bytes.TrimSpace(data); len(t) > 0 && t[0] == '[' {
		dec := json.NewDecoder(bytes.NewReader(t))
		dec.UseNumber()

		var objs []map[string]any
		if err := dec.Decode(&objs); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}

		rows := make([]map[string]string, len(objs))

		for i, o := range objs {
			rows[i] = map[string]string{}
			for k, v := range o {
				rows[i][k] = fmt.Sprint(v)
			}
		}

		return rows, nil
	}

	r := csv.NewReader(bytes.NewReader(data))
	r.LazyQuotes, r.FieldsPerRecord, r.TrimLeadingSpace = true, -1, true

	recs, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	if len(recs) == 0 {
		return nil, fmt.Errorf("%s is empty", path)
	}

	head := recs[0]
	head[0] = strings.TrimPrefix(head[0], "\xef\xbb\xbf")
	rows := make([]map[string]string, 0, len(recs)-1)

	for _, rec := range recs[1:] {
		row := make(map[string]string, len(head))
		for i, h := range head {
			if i < len(rec) {
				row[strings.TrimSpace(h)] = strings.TrimSpace(rec[i])
			}
		}

		rows = append(rows, row)
	}

	return rows, nil
}

// checkColumns names the first required column the catalog does not have, so a wrong --map stops before anything is sent.
func checkColumns(row map[string]string, cols columns) error {
	for _, f := range []string{"time", "mag", "lat", "lon"} {
		for name := range strings.SplitSeq(cols[f], "+") {
			if _, ok := row[name]; !ok {
				return fmt.Errorf("the catalog has no column %q (needed for %s), use --map %s=<column>", name, f, f)
			}
		}
	}

	return nil
}

var timeLayouts = []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05.999", "2006-01-02 15:04:05", "2006/01/02 15:04:05", "2006-01-02"}

// parseTime reads the usual catalog time formats as UTC.
func parseTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)

	for _, l := range timeLayouts {
		if t, err := time.ParseInLocation(l, s, time.UTC); err == nil {
			return t.UTC(), true
		}
	}

	return time.Time{}, false
}

func floatField(row map[string]string, col string) (float64, bool) {
	f, err := strconv.ParseFloat(strings.TrimSpace(row[col]), 64)

	return f, err == nil
}

// normalise turns one row into an event, or says why it cannot. Rows the Aggregator would drop or skew are stopped here: a bad time, a position out of range, a time in the future.
func normalise(row map[string]string, cols columns, now time.Time) (event, string) {
	at, why := rowTime(row, cols, now)
	if why != "" {
		return event{}, why
	}

	lat, ok1 := floatField(row, cols["lat"])
	lon, ok2 := floatField(row, cols["lon"])

	if !ok1 || !ok2 || lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		return event{}, "position missing or out of range"
	}

	mag, ok := floatField(row, cols["mag"])
	if !ok {
		return event{}, "magnitude not a number"
	}

	depth, _ := floatField(row, cols["depth"])
	region := row[cols["region"]]

	if region == "" {
		region = "unknown"
	}

	ev := event{Magnitude: mag, DepthKm: depth, EpicenterLat: lat, EpicenterLon: lon, RegionName: region, OccurredAt: at.Format(time.RFC3339)}

	if id := row[cols["id"]]; id != "" && len(id) <= maxIDLen {
		ev.EventID = id
	} else {
		ev.EventID = derivedID(ev)
	}

	return ev, ""
}

// rowTime reads the time of a row, which may sit in two columns, and refuses one that is not readable or not in the past.
func rowTime(row map[string]string, cols columns, now time.Time) (time.Time, string) {
	var parts []string
	for name := range strings.SplitSeq(cols["time"], "+") {
		parts = append(parts, row[name])
	}

	at, ok := parseTime(strings.Join(parts, " "))

	switch {
	case !ok:
		return time.Time{}, "time not readable"
	case at.After(now.Add(-time.Minute)):
		return time.Time{}, "time in the future"
	}

	return at, ""
}

// derivedID is the same short hash for the same quake, for catalogs that have no id column.
func derivedID(ev event) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%.4f|%.4f|%.1f", ev.OccurredAt, ev.EpicenterLat, ev.EpicenterLon, ev.Magnitude))

	return "q-" + hex.EncodeToString(sum[:8])
}

// report counts what happened to the catalog rows.
type report struct {
	Read, Kept, Duplicates int
	Rejected               map[string]int
}

// load reads and normalises a catalog and drops repeated ids. Kept is the number of unique events, which is what the Audit count must equal.
func load(path string, cols columns, now time.Time) ([]event, report, error) {
	rows, err := readRows(path)
	if err != nil {
		return nil, report{}, err
	}

	rep := report{Read: len(rows), Rejected: map[string]int{}}

	if len(rows) == 0 {
		return nil, rep, fmt.Errorf("%s has no rows", path)
	}

	if err := checkColumns(rows[0], cols); err != nil {
		return nil, rep, err
	}

	seen := map[string]bool{}

	var out []event

	for _, row := range rows {
		ev, why := normalise(row, cols, now)

		switch {
		case why != "":
			rep.Rejected[why]++
		case seen[ev.EventID]:
			rep.Duplicates++
		default:
			seen[ev.EventID] = true
			out = append(out, ev)
		}
	}

	rep.Kept = len(out)

	return out, rep, nil
}

// burst makes n events inside one box (latMin, latMax, lonMin, lonMax) with times in the 24 hours before now, so the Aggregator's cursor never moves past what the live feed produces next.
func burst(n int, box [4]float64, now time.Time, rng *rand.Rand) []event {
	run := strconv.FormatInt(now.UnixNano(), 36)
	out := make([]event, n)

	for i := range out {
		at := now.Add(-time.Minute - time.Duration(rng.Int64N(int64(24*time.Hour-time.Minute))))
		out[i] = event{
			EventID:      fmt.Sprintf("burst-%s-%06d", run, i),
			Magnitude:    3 + rng.Float64()*3,
			DepthKm:      10 + rng.Float64()*40,
			EpicenterLat: box[0] + rng.Float64()*(box[1]-box[0]),
			EpicenterLon: box[2] + rng.Float64()*(box[3]-box[2]),
			RegionName:   "burst",
			OccurredAt:   at.UTC().Format(time.RFC3339),
		}
	}

	return out
}
