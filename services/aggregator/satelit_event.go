package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"time"
)

// Event kinds on the satellite feed (P4).
const (
	feedDetection = "detection"
	feedUpdate    = "update"
	feedRetract   = "retract"
	feedHeartbeat = "heartbeat"
)

// Detection confidence. Low is left out of fires (Definisi Kebakaran rule 3, S10).
const (
	confLow     = "l"
	confNominal = "n"
	confHigh    = "h"
)

var errBadFeedEvent = errors.New("bad satellite feed event")

// One feed event, cut down to what the fire rules need.
type feedEvent struct {
	Offset  int64
	Kind    string
	Key     string          // detection, update, retract: satellite|acq_date|acq_time|latitude|longitude
	Det     detectionData   // detection only
	Changes detectionChange // update only
	Now     int64           // heartbeat only: virtual_now in Unix seconds
}

type detectionData struct {
	Satellite  string
	Acq        int64 // acq_date + acq_time in UTC, Unix seconds (rule 2)
	Lat, Lon   float64
	Confidence string
	FRP        float64
	Body       uint64 // FNV-1a of the raw detection object: an identical copy is a re-send, a different one replaces it (S2)
}

// Only confidence and FRP change a fire; brightness changes are read and ignored.
type detectionChange struct {
	Confidence *string
	FRP        *float64
}

type wireFeedEvent struct {
	Offset     *int64          `json:"offset"`
	Key        string          `json:"key"`
	Detection  json.RawMessage `json:"detection"`
	Changes    *wireChange     `json:"changes"`
	VirtualNow string          `json:"virtual_now"`
}

type wireDetection struct {
	Satellite  string          `json:"satellite"`
	AcqDate    string          `json:"acq_date"`
	AcqTime    json.RawMessage `json:"acq_time"`
	Latitude   json.Number     `json:"latitude"`
	Longitude  json.Number     `json:"longitude"`
	Confidence string          `json:"confidence"`
	FRP        float64         `json:"frp"`
}

type wireChange struct {
	Confidence *string  `json:"confidence"`
	FRP        *float64 `json:"frp"`
}

// parseFeedEvent reads one SSE event (its event name and data). Any error wraps errBadFeedEvent; the caller parks the raw event.
func parseFeedEvent(kind string, data []byte) (feedEvent, error) {
	var w wireFeedEvent
	if err := json.Unmarshal(data, &w); err != nil {
		return feedEvent{}, fmt.Errorf("%w: %s: %w", errBadFeedEvent, kind, err)
	}

	if w.Offset == nil || *w.Offset < 0 {
		return feedEvent{}, fmt.Errorf("%w: %s: missing or negative offset", errBadFeedEvent, kind)
	}

	ev := feedEvent{Offset: *w.Offset, Kind: kind, Key: w.Key}

	var err error

	switch kind {
	case feedDetection:
		err = ev.readDetection(w)
	case feedUpdate:
		err = ev.readUpdate(w)
	case feedRetract:
		err = checkKey(w.Key)
	case feedHeartbeat:
		ev.Now, err = parseVirtualNow(w.VirtualNow)
	default:
		err = errors.New("unknown event kind")
	}

	if err != nil {
		return feedEvent{}, fmt.Errorf("%w: %s at offset %d: %w", errBadFeedEvent, kind, ev.Offset, err)
	}

	return ev, nil
}

func (ev *feedEvent) readDetection(w wireFeedEvent) error {
	var d wireDetection
	if err := json.Unmarshal(w.Detection, &d); err != nil {
		return fmt.Errorf("detection: %w", err)
	}

	det, key, err := d.parse()
	if err != nil {
		return err
	}

	h := fnv.New64a()
	_, _ = h.Write(w.Detection)
	det.Body = h.Sum64()

	ev.Det = det

	// The feed sends no key with a detection; one that does keeps its own (S1).
	if ev.Key == "" {
		ev.Key = key
	}

	return nil
}

func (ev *feedEvent) readUpdate(w wireFeedEvent) error {
	if err := checkKey(w.Key); err != nil {
		return err
	}

	if w.Changes == nil {
		return errors.New("missing changes")
	}

	if c := w.Changes.Confidence; c != nil && !validConfidence(*c) {
		return fmt.Errorf("confidence %q", *c)
	}

	if f := w.Changes.FRP; f != nil && *f < 0 {
		return fmt.Errorf("frp %v", *f)
	}

	ev.Changes = detectionChange{Confidence: w.Changes.Confidence, FRP: w.Changes.FRP}

	return nil
}

// parse checks the fields and builds the detection key from their text as sent (S1, F6).
func (d wireDetection) parse() (detectionData, string, error) {
	if d.Satellite == "" || strings.Contains(d.Satellite, "|") {
		return detectionData{}, "", fmt.Errorf("satellite %q", d.Satellite)
	}

	if !validConfidence(d.Confidence) {
		return detectionData{}, "", fmt.Errorf("confidence %q", d.Confidence)
	}

	if d.FRP < 0 {
		return detectionData{}, "", fmt.Errorf("frp %v", d.FRP)
	}

	acqTime := strings.Trim(string(d.AcqTime), `"`)

	acq, err := acqUnix(d.AcqDate, acqTime)
	if err != nil {
		return detectionData{}, "", err
	}

	lat, latText, err := coordinate(d.Latitude, 90)
	if err != nil {
		return detectionData{}, "", fmt.Errorf("latitude: %w", err)
	}

	lon, lonText, err := coordinate(d.Longitude, 180)
	if err != nil {
		return detectionData{}, "", fmt.Errorf("longitude: %w", err)
	}

	det := detectionData{Satellite: d.Satellite, Acq: acq, Lat: lat, Lon: lon, Confidence: d.Confidence, FRP: d.FRP}

	return det, strings.Join([]string{d.Satellite, d.AcqDate, acqTime, latText, lonText}, "|"), nil
}

// acqUnix joins acq_date (YYYY-MM-DD) and acq_time (unpadded HHMM, e.g. 25 = 00:25) in UTC.
func acqUnix(date, hhmm string) (int64, error) {
	day, err := time.Parse(time.DateOnly, date)
	if err != nil {
		return 0, fmt.Errorf("acq_date %q: %w", date, err)
	}

	if hhmm == "" || len(hhmm) > 4 || strings.Trim(hhmm, "0123456789") != "" {
		return 0, fmt.Errorf("acq_time %q is not HHMM", hhmm)
	}

	n, _ := strconv.Atoi(hhmm)
	if n/100 > 23 || n%100 > 59 {
		return 0, fmt.Errorf("acq_time %q is out of range", hhmm)
	}

	return day.Unix() + int64(n/100*3600+n%100*60), nil
}

// coordinate returns the value and its key text: the number as sent, except that exponent form
// (-6.0e-5) is written as a plain decimal (-0.00006), the way the feed's own keys write it (F6).
func coordinate(n json.Number, limit float64) (float64, string, error) {
	text := n.String()

	v, err := strconv.ParseFloat(text, 64)
	if err != nil || math.Abs(v) > limit {
		return 0, "", fmt.Errorf("%q is not a coordinate", text)
	}

	if strings.ContainsAny(text, "eE") {
		text = strconv.FormatFloat(v, 'f', -1, 64)
	}

	return v, text, nil
}

func validConfidence(c string) bool {
	return c == confLow || c == confNominal || c == confHigh
}

func checkKey(key string) error {
	if strings.Count(key, "|") != 4 {
		return fmt.Errorf("key %q does not have 5 fields", key)
	}

	return nil
}

func parseVirtualNow(s string) (int64, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, fmt.Errorf("virtual_now: %w", err)
	}

	return t.Unix(), nil
}
