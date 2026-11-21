package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

type sampleLine struct {
	ID    string          `json:"id"`
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

func readSamples(t *testing.T, kind string) []sampleLine {
	t.Helper()

	f, err := os.Open("../../doc/p4_feed_samples/" + kind + ".ndjson") //nolint:gosec // test reads the project's own sample files
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = f.Close() }()

	var out []sampleLine

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var l sampleLine
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			t.Fatal(err)
		}

		out = append(out, l)
	}

	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}

	return out
}

// Every captured sample of every kind parses, and its offset matches the SSE id.
func TestParseFeedEventReadsEverySample(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{feedDetection, feedUpdate, feedRetract, feedHeartbeat} {
		for _, l := range readSamples(t, kind) {
			ev, err := parseFeedEvent(l.Event, l.Data)
			if err != nil {
				t.Fatalf("%s %s: %v", kind, l.ID, err)
			}

			if strconv.FormatInt(ev.Offset, 10) != l.ID || ev.Kind != kind {
				t.Fatalf("offset %d kind %s, want id %s kind %s", ev.Offset, ev.Kind, l.ID, kind)
			}
		}
	}
}

// Keys keep the number text as sent, except that exponent coordinates become plain decimals like the feed's own keys (F6).
func TestDetectionKeyText(t *testing.T) {
	t.Parallel()

	want := map[string]string{
		"0":    "N21|2026-11-27|2350|-9.43923|113.81356",
		"4325": "N21|2026-11-28|1014|-0.00006|120.86746",
		"4330": "N20|2026-11-28|1047|0.00076|120.86769",
		"4372": "N20|2026-11-28|1100|0.00054|120.86836",
		"4408": "N|2026-11-28|1023|-0.00083|120.86714",
		"4426": "N|2026-11-28|1102|0.00062|120.86737",
	}

	for _, l := range readSamples(t, feedDetection) {
		ev, err := parseFeedEvent(l.Event, l.Data)
		if err != nil {
			t.Fatal(err)
		}

		if k, ok := want[l.ID]; ok && ev.Key != k {
			t.Fatalf("offset %s key %q, want %q", l.ID, ev.Key, k)
		}
	}
}

func TestDetectionTimeIsAcqDateAndUnpaddedTime(t *testing.T) {
	t.Parallel()

	data := `{"offset":7,"detection":{"satellite":"N","acq_date":"2026-11-28","acq_time":25,"latitude":-1.5,"longitude":110.25,"confidence":"h","frp":3.5}}`

	ev, err := parseFeedEvent(feedDetection, []byte(data))
	if err != nil {
		t.Fatal(err)
	}

	if want := time.Date(2026, 11, 28, 0, 25, 0, 0, time.UTC).Unix(); ev.Det.Acq != want {
		t.Fatalf("acq %d, want %d", ev.Det.Acq, want)
	}

	if ev.Key != "N|2026-11-28|25|-1.5|110.25" {
		t.Fatalf("key %q", ev.Key)
	}
}

func TestParseFeedEventRejectsBadEvents(t *testing.T) {
	t.Parallel()

	const det = `{"offset":1,"detection":{"satellite":"N","acq_date":"2026-11-28","acq_time":%s,"latitude":%s,"longitude":110,"confidence":"%s","frp":1}}`

	cases := map[string]struct{ kind, data string }{
		"not json":           {feedDetection, `{`},
		"no offset":          {feedRetract, `{"key":"N|2026-11-28|25|1|2"}`},
		"no detection":       {feedDetection, `{"offset":1}`},
		"minute 60":          {feedDetection, fmt.Sprintf(det, "1060", "1", "n")},
		"hour 24":            {feedDetection, fmt.Sprintf(det, "2400", "1", "n")},
		"time not a number":  {feedDetection, fmt.Sprintf(det, `"10:00"`, "1", "n")},
		"latitude 91":        {feedDetection, fmt.Sprintf(det, "1000", "91", "n")},
		"unknown confidence": {feedDetection, fmt.Sprintf(det, "1000", "1", "x")},
		"update no changes":  {feedUpdate, `{"offset":1,"key":"N|2026-11-28|25|1|2"}`},
		"update bad conf":    {feedUpdate, `{"offset":1,"key":"N|2026-11-28|25|1|2","changes":{"confidence":"x"}}`},
		"retract short key":  {feedRetract, `{"offset":1,"key":"N|2026-11-28"}`},
		"heartbeat bad time": {feedHeartbeat, `{"offset":1,"virtual_now":"yesterday"}`},
		"unknown kind":       {"message", `{"offset":1}`},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := parseFeedEvent(c.kind, []byte(c.data)); !errors.Is(err, errBadFeedEvent) {
				t.Fatalf("err %v, want errBadFeedEvent", err)
			}
		})
	}
}
