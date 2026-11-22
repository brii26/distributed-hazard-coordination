package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
)

type malformedKind uint32

const (
	malformedOff malformedKind = iota
	malformedTruncated
	malformedDeep
	malformedUTF8
	malformedHuge
)

const (
	deepLevels = 100           // the Aggregator accepts 64
	hugeBytes  = 1<<20 + 1<<10 // the Aggregator accepts 1 MiB per record
)

var malformedNames = [...]string{"off", "truncated", "deep", "utf8", "huge"}

func (k malformedKind) name() string { return malformedNames[k] }

// What /volcanic-reports does to its body. One atomic value, so a request never sees half a switch.
var malformedState atomic.Uint32

func currentMalformed() malformedKind { return malformedKind(malformedState.Load()) }

// An empty name is off. An unknown name is off too, and the second result is false.
func parseMalformed(s string) (malformedKind, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return malformedOff, true
	}

	for _, k := range [...]malformedKind{malformedTruncated, malformedDeep, malformedUTF8, malformedHuge} {
		if k.name() == s {
			return k, true
		}
	}

	return malformedOff, s == malformedOff.name()
}

// Sets the kind from {"kind": ...}. An empty body, bad JSON or an unknown kind means off: the mock never answers 4xx for an odd body.
func malformedHandler(w http.ResponseWriter, r *http.Request) {
	raw, l := adminCall(r)

	var req struct {
		Kind string `json:"kind"`
	}

	kind, known := malformedOff, true
	if json.Unmarshal(raw, &req) == nil {
		kind, known = parseMalformed(req.Kind)
	}

	if !known {
		l.Warn("malformed kind unknown, using off", "kind", req.Kind)
	}

	was := currentMalformed()

	malformedState.Store(uint32(kind))

	l.Info("malformed changed", "from", was.name(), "to", kind.name())

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(map[string]string{"malformed": kind.name()}); err != nil {
		log.Printf("failed to write malformed response: %v", err)
	}
}

// Shaped by the malformed kind when one is set.
func writeReports(w io.Writer, page []VolcanicReport) error {
	kind := currentMalformed()
	if kind == malformedOff {
		return json.NewEncoder(w).Encode(page)
	}

	body, err := malformedBody(page, kind)
	if err != nil {
		return err
	}

	_, err = w.Write(body)

	return err
}

// The normal records plus one bad record after the first half, so records before and after it prove the others keep flowing.
// truncated serves the whole page cut inside its last record instead.
func malformedBody(page []VolcanicReport, kind malformedKind) ([]byte, error) {
	items := make([][]byte, 0, len(page)+1)

	for _, r := range page {
		b, err := json.Marshal(r)
		if err != nil {
			return nil, err
		}

		items = append(items, b)
	}

	comma := []byte(",")

	if kind == malformedTruncated {
		if len(items) == 0 {
			return []byte(`[{"report_id":"MALFORMED-truncated","volca`), nil
		}

		last := items[len(items)-1]
		body := append([]byte("["), bytes.Join(items[:len(items)-1], comma)...)

		if len(items) > 1 {
			body = append(body, ',')
		}

		return append(body, last[:len(last)/2]...), nil
	}

	items = slices.Insert(items, len(items)/2, badRecord(kind))

	return append(append([]byte("["), bytes.Join(items, comma)...), ']'), nil
}

func badRecord(kind malformedKind) []byte {
	switch kind {
	case malformedDeep:
		return []byte(`{"report_id":"MALFORMED-deep","nested":` + strings.Repeat("[", deepLevels) + strings.Repeat("]", deepLevels) + `}`)
	case malformedUTF8:
		return []byte(`{"report_id":"MALFORMED-utf8","note":"` + "\xff\xfe" + `"}`)
	case malformedHuge:
		return []byte(`{"report_id":"MALFORMED-huge","note":"` + strings.Repeat("x", hugeBytes) + `"}`)
	case malformedOff, malformedTruncated:
		return nil
	}

	return nil
}
