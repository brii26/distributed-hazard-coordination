package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// JSON kinds of a leaf.
const (
	jsonString = "string"
	jsonNumber = "number"
)

// leaf is one scalar of a JSON document. text is the exact source text for
// numbers (json.Number keeps it) and the decoded value for strings.
type leaf struct {
	kind string // string, number, bool, null, object, array
	text string
}

// raw is the leaf as it appeared in JSON, quotes included for strings.
func (l leaf) raw() string {
	if l.kind == jsonString {
		return strconv.Quote(l.text)
	}

	return l.text
}

// flatJSON maps dotted paths ("a.b", "list[]") to leaves. Array elements share one path.
type flatJSON map[string]leaf

func flattenJSON(data string) (flatJSON, bool) {
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		return nil, false
	}

	out := flatJSON{}
	flattenInto("", v, out)

	return out, true
}

func flattenInto(path string, v any, out flatJSON) {
	switch x := v.(type) {
	case map[string]any:
		if len(x) == 0 {
			out[rooted(path)] = leaf{"object", "{}"}
		}

		for k, child := range x {
			flattenInto(joinPath(path, k), child, out)
		}
	case []any:
		if len(x) == 0 {
			out[rooted(path)] = leaf{"array", "[]"}
		}

		for _, child := range x {
			flattenInto(path+"[]", child, out)
		}
	case json.Number:
		out[rooted(path)] = leaf{jsonNumber, x.String()}
	case string:
		out[rooted(path)] = leaf{jsonString, x}
	case bool:
		out[rooted(path)] = leaf{"bool", strconv.FormatBool(x)}
	default:
		out[rooted(path)] = leaf{"null", "null"}
	}
}

func joinPath(path, key string) string {
	if path == "" {
		return key
	}

	return path + "." + key
}

func rooted(path string) string {
	if path == "" {
		return "(root)"
	}

	return path
}

// lastSegment is the field name at the end of a path, without array markers.
func lastSegment(path string) string {
	if i := strings.LastIndexByte(path, '.'); i >= 0 {
		path = path[i+1:]
	}

	return strings.TrimRight(path, "[]")
}

// find returns the shallowest leaf whose field name is name, so the probe works
// whether the feed nests the record ("detection.latitude") or not.
func (f flatJSON) find(name string) (leaf, bool) {
	best := ""

	for path := range f {
		if lastSegment(path) != name {
			continue
		}

		if best == "" || strings.Count(path, ".") < strings.Count(best, ".") || (strings.Count(path, ".") == strings.Count(best, ".") && path < best) {
			best = path
		}
	}

	if best == "" {
		return leaf{}, false
	}

	return f[best], true
}

func (f flatJSON) text(name string) string {
	l, _ := f.find(name)

	return l.text
}

// parseFeedTime reads an RFC 3339 string or a Unix time in seconds or milliseconds.
func parseFeedTime(l leaf) (time.Time, bool) {
	switch l.kind {
	case jsonString:
		t, err := time.Parse(time.RFC3339Nano, l.text)

		return t.UTC(), err == nil
	case jsonNumber:
		f, err := strconv.ParseFloat(l.text, 64)
		if err != nil {
			return time.Time{}, false
		}

		if math.Abs(f) > 1e11 { // milliseconds
			return time.UnixMilli(int64(f)).UTC(), true
		}

		sec, frac := math.Modf(f)

		return time.Unix(int64(sec), int64(frac*1e9)).UTC(), true
	default:
		return time.Time{}, false
	}
}

// acqDateTime joins acq_date (YYYY-MM-DD) and acq_time (HHMM, H:MM or HH:MM[:SS]) in UTC.
func acqDateTime(date, clock string) (time.Time, error) {
	day, err := time.Parse(time.DateOnly, strings.TrimSpace(date))
	if err != nil {
		return time.Time{}, fmt.Errorf("acq_date %q: %w", date, err)
	}

	clock = strings.TrimSpace(clock)

	if strings.Contains(clock, ":") {
		layout := "15:04"
		if strings.Count(clock, ":") == 2 {
			layout = time.TimeOnly
		}

		t, err := time.Parse(layout, clock)
		if err != nil {
			return time.Time{}, fmt.Errorf("acq_time %q: %w", clock, err)
		}

		return day.Add(time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second), nil
	}

	if !allDigits(clock) || len(clock) > 4 {
		return time.Time{}, fmt.Errorf("acq_time %q is not HHMM", clock)
	}

	hhmm, _ := strconv.Atoi(clock)
	hours, minutes := hhmm/100, hhmm%100

	if hours > 23 || minutes > 59 {
		return time.Time{}, fmt.Errorf("acq_time %q is out of range", clock)
	}

	return day.Add(time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute), nil
}

// snapshot is the part of /v1/session the analysis needs.
type snapshot struct {
	wall         time.Time
	connSeq      int
	profile      string
	speedText    string
	speed        float64
	latestOffset string
	virtualNow   time.Time
	hasVirtual   bool
}

func parseSnapshot(body []byte) (snapshot, bool) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return snapshot{}, false
	}

	flat, ok := flattenJSON(string(body))
	if !ok {
		return snapshot{}, false
	}

	s := snapshot{profile: flat.text("profile"), speedText: flat.text("speed"), latestOffset: flat.text("latest_offset")}
	s.speed, _ = strconv.ParseFloat(strings.TrimSuffix(s.speedText, "x"), 64)

	if l, ok := flat.find("virtual_now"); ok {
		s.virtualNow, s.hasVirtual = parseFeedTime(l)
	}

	return s, true
}
