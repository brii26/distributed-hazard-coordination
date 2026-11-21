package main

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"
)

// All test detection times count in minutes from fireT0. Dots sit on longitude 110; 0.001° of latitude is about 0.111 km.
var fireT0 = time.Date(2026, 11, 28, 10, 0, 0, 0, time.UTC).Unix()

type fireFeed struct {
	t   *testing.T
	e   *fireEngine
	off int64
}

func newFireFeed(t *testing.T) *fireFeed {
	t.Helper()

	return &fireFeed{t: t, e: newFireEngine(12 * time.Hour)}
}

func (f *fireFeed) next() int64 {
	f.off++

	return f.off - 1
}

func detectionEvent(off int64, sat string, minute int64, lat float64, conf string, frp float64) feedEvent {
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%s|%g", conf, frp)

	return feedEvent{
		Offset: off, Kind: feedDetection, Key: fmt.Sprintf("%s|%d|%.5f|110", sat, minute, lat),
		Det: detectionData{Satellite: sat, Acq: fireT0 + minute*60, Lat: lat, Lon: 110, Confidence: conf, FRP: frp, Body: h.Sum64()},
	}
}

// detection sends a detection and returns its key and park reason.
func (f *fireFeed) detection(sat string, minute int64, lat float64, conf string, frp float64) (key, park string) {
	ev := detectionEvent(f.next(), sat, minute, lat, conf, frp)

	return ev.Key, f.e.apply(ev)
}

// dot sends an ordinary detection that must be accepted.
func (f *fireFeed) dot(minute int64, lat float64) string {
	f.t.Helper()

	key, park := f.detection("N", minute, lat, confNominal, 1)
	if park != "" {
		f.t.Fatalf("detection %s parked: %s", key, park)
	}

	return key
}

func (f *fireFeed) update(key, conf string, frp float64) string {
	ev := feedEvent{Offset: f.next(), Kind: feedUpdate, Key: key}
	if conf != "" {
		ev.Changes.Confidence = &conf
	}

	if frp >= 0 {
		ev.Changes.FRP = &frp
	}

	return f.e.apply(ev)
}

func (f *fireFeed) retract(key string) string {
	return f.e.apply(feedEvent{Offset: f.next(), Kind: feedRetract, Key: key})
}

func (f *fireFeed) heartbeat(minute int64) {
	f.e.apply(feedEvent{Offset: f.next(), Kind: feedHeartbeat, Now: fireT0 + minute*60})
}

// drained returns the changed fires by name.
func (f *fireFeed) drained() map[string]fireSnapshot {
	out := map[string]fireSnapshot{}
	for _, s := range f.e.drain() {
		out[s.Name] = s
	}

	return out
}

func dotKeys(s fireSnapshot) []string {
	keys := make([]string, 0, len(s.Dots))
	for _, d := range s.Dots {
		keys = append(keys, d.Key)
	}

	return keys
}

// wantLive checks that a fire is live and holds exactly these keys.
func wantLive(t *testing.T, got map[string]fireSnapshot, name string, keys ...string) fireSnapshot {
	t.Helper()

	s, ok := got[name]
	if !ok || s.Withdrawn {
		t.Fatalf("fire %s missing or withdrawn: %+v", name, got)
	}

	if !sameKeys(dotKeys(s), keys) {
		t.Fatalf("fire %s dots %v, want %v", name, dotKeys(s), keys)
	}

	return s
}

func sameKeys(a, b []string) bool {
	set := map[string]int{}
	for _, k := range a {
		set[k]++
	}

	for _, k := range b {
		set[k]--
	}

	for _, n := range set {
		if n != 0 {
			return false
		}
	}

	return true
}

// Rule 4: A and C are 1.78 km apart but both link to B, so all three are one fire, named after A (smallest offset).
func TestFireChainLinksThroughAMiddleDot(t *testing.T) {
	t.Parallel()

	f := newFireFeed(t)
	a := f.dot(0, 0)
	c := f.dot(0, 0.016)

	if got := f.drained(); len(got) != 0 {
		t.Fatalf("two unlinked dots formed a fire: %+v", got)
	}

	b := f.dot(0, 0.008)

	got := f.drained()
	if s := wantLive(t, got, fireName(a), a, b, c); s.Version != 2 || len(got) != 1 {
		t.Fatalf("version %d, fires %d; want 2 and 1", s.Version, len(got))
	}
}

func TestFireLinkBoundaries(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		minute int64
		lat    float64
		linked bool
	}{
		{"exactly 360 min apart", 360, 0.001, true},
		{"361 min apart", 361, 0.001, false},
		{"0.990 km apart", 0, 0.0089, true},
		{"1.012 km apart", 0, 0.0091, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			f := newFireFeed(t)
			f.dot(0, 0)
			f.dot(c.minute, c.lat)

			if got := len(f.drained()) == 1; got != c.linked {
				t.Fatalf("linked %v, want %v", got, c.linked)
			}
		})
	}
}

// #312: lateness is judged by detection time against the feed clock. Within L (inclusive) a late dot still joins its fire; beyond L it is parked and counted, never dropped silently.
func TestFireLatenessWindow(t *testing.T) {
	t.Parallel()

	f := newFireFeed(t)
	a := f.dot(0, 0)

	f.heartbeat(12 * 60)
	b := f.dot(0, 0.005) // exactly L behind the clock

	wantLive(t, f.drained(), fireName(a), a, b)

	f.heartbeat(12*60 + 1)

	if _, park := f.detection("N", 0, 0.003, confNominal, 1); park != parkTooLate {
		t.Fatalf("detection 12 h 1 min late: park %q, want %q", park, parkTooLate)
	}

	if park := f.update(a, "", 9); park != parkTooLate {
		t.Fatalf("update whose target is 12 h 1 min late: park %q, want %q", park, parkTooLate)
	}

	if got := f.drained(); len(got) != 0 || f.e.counts.TooLate != 2 {
		t.Fatalf("late events changed fires %+v or count %d != 2", got, f.e.counts.TooLate)
	}
}

// A dot that links two fires merges them into the one with the older anchor; the other is withdrawn and says where it went.
func TestFireMergeKeepsTheOlderAnchor(t *testing.T) {
	t.Parallel()

	f := newFireFeed(t)
	a, b := f.dot(0, 0), f.dot(0, 0.005)
	c, d := f.dot(0, 0.020), f.dot(0, 0.025)
	f.drained()

	e := f.dot(0, 0.0125)
	got := f.drained()

	wantLive(t, got, fireName(a), a, b, c, d, e)

	if s := got[fireName(c)]; !s.Withdrawn || s.MergedInto != fireName(a) || s.Version != 4 {
		t.Fatalf("merged fire: %+v", s)
	}
}

// #314: a retract that breaks the chain splits the fire. The part with the anchor keeps the name; the other part is a new fire.
func TestFireSplitsOnRetract(t *testing.T) {
	t.Parallel()

	f := newFireFeed(t)
	a, b, c, d, e := f.dot(0, 0), f.dot(0, 0.008), f.dot(0, 0.016), f.dot(0, 0.024), f.dot(0, 0.032)
	f.drained()

	f.retract(c)
	got := f.drained()

	wantLive(t, got, fireName(a), a, b)
	wantLive(t, got, fireName(d), d, e)
}

func TestFireKeepsItsNameWhenTheAnchorIsRetracted(t *testing.T) {
	t.Parallel()

	f := newFireFeed(t)
	a, b, c := f.dot(0, 0), f.dot(0, 0.003), f.dot(0, 0.006)
	f.drained()

	f.retract(a)
	wantLive(t, f.drained(), fireName(a), b, c)
}

// Rule 3: low confidence stays out of fires.
func TestFireLeavesLowConfidenceOut(t *testing.T) {
	t.Parallel()

	f := newFireFeed(t)
	a := f.dot(0, 0)
	f.detection("N", 0, 0.008, confLow, 1)
	c := f.dot(0, 0.016)

	if got := f.drained(); len(got) != 0 {
		t.Fatalf("a low-confidence dot linked a fire: %+v", got)
	}

	d, park := f.detection("N20", 0, 0.008, confNominal, 1) // where the low dot is, seen by another satellite
	if park != "" {
		t.Fatal(park)
	}

	wantLive(t, f.drained(), fireName(a), a, c, d)
}

// An update to low withdraws a fire that falls below 2 dots; an update back to nominal forms it again under the same name with a higher version.
func TestFireConfidenceFlipWithdrawsAndRevives(t *testing.T) {
	t.Parallel()

	f := newFireFeed(t)
	a, b, c := f.dot(0, 0), f.dot(0, 0.008), f.dot(0, 0.016)
	f.drained()

	f.update(b, confLow, -1)

	gone := f.drained()[fireName(a)]
	if !gone.Withdrawn || gone.MergedInto != "" || gone.Version != 3 {
		t.Fatalf("after n→l: %+v", gone)
	}

	f.update(b, confHigh, -1)

	if s := wantLive(t, f.drained(), fireName(a), a, b, c); s.Version != 4 {
		t.Fatalf("revived version %d, want 4", s.Version)
	}
}

// S3: a retract is final. Later copies, updates and retracts of the key are counted and ignored, not parked.
func TestFireRetractIsFinal(t *testing.T) {
	t.Parallel()

	f := newFireFeed(t)
	a := f.dot(0, 0)
	f.dot(0, 0.003)
	f.retract(a)

	if s := f.drained()[fireName(a)]; !s.Withdrawn {
		t.Fatalf("fire of 1 dot left: %+v", s)
	}

	if _, park := f.detection("N", 0, 0, confNominal, 1); park != "" {
		t.Fatalf("re-sent retracted detection parked: %s", park)
	}

	if park := f.update(a, confHigh, 5); park != "" {
		t.Fatalf("update of retracted detection parked: %s", park)
	}

	if park := f.retract(a); park != "" {
		t.Fatalf("second retract parked: %s", park)
	}

	if got := f.drained(); len(got) != 0 || f.e.counts.TombstoneRejects != 3 {
		t.Fatalf("changes %+v, tombstone rejects %d; want none and 3", got, f.e.counts.TombstoneRejects)
	}
}

// Rule 1 and S2: an identical copy counts once; a copy with different values replaces the detection.
func TestFireResendIsCountedOnceAndAChangedCopyReplaces(t *testing.T) {
	t.Parallel()

	f := newFireFeed(t)
	a, b := f.dot(0, 0), f.dot(0, 0.003)
	f.drained()

	f.dot(0, 0)

	if got := f.drained(); len(got) != 0 || f.e.counts.Resent != 1 {
		t.Fatalf("identical copy changed %+v, resent %d", got, f.e.counts.Resent)
	}

	f.detection("N", 0, 0, confNominal, 5)

	s := wantLive(t, f.drained(), fireName(a), a, b)
	if s.Dots[0].FRP+s.Dots[1].FRP != 6 || s.Version != 3 {
		t.Fatalf("after replace: %+v", s)
	}
}

// Closing: once C > newest acq + 360 min + L the fire leaves memory, loose dots count as singles, and later events for them are parked.
func TestFireClosesAndLaterEventsCannotChangeIt(t *testing.T) {
	t.Parallel()

	f := newFireFeed(t)
	a := f.dot(0, 0)
	f.dot(0, 0.003)
	f.dot(0, 1) // a single, far away
	f.drained()

	f.heartbeat(18 * 60) // exactly at the deadline: still open

	if len(f.e.fires) != 1 {
		t.Fatal("fire closed at its deadline, want after it")
	}

	f.heartbeat(18*60 + 1)

	if len(f.e.fires) != 0 || len(f.e.dots) != 0 || f.e.counts.Singles != 1 {
		t.Fatalf("after closing: %d fires, %d dots, %d singles; want 0, 0, 1", len(f.e.fires), len(f.e.dots), f.e.counts.Singles)
	}

	if park := f.update(a, "", 9); park != parkUnknownTarget {
		t.Fatalf("update to a closed fire: park %q, want %q", park, parkUnknownTarget)
	}

	if _, park := f.detection("N", 0, 0, confNominal, 1); park != parkTooLate {
		t.Fatalf("copy of a closed detection: park %q, want %q", park, parkTooLate)
	}

	if got := f.drained(); len(got) != 0 {
		t.Fatalf("closed fire changed: %+v", got)
	}
}

// A fire that closes before its last change was drained is still handed out once.
func TestFireClosedBeforeDrainIsStillSaved(t *testing.T) {
	t.Parallel()

	f := newFireFeed(t)
	a, b := f.dot(0, 0), f.dot(0, 0.003)
	f.heartbeat(18*60 + 1)

	if len(f.e.fires) != 0 {
		t.Fatal("fire did not close")
	}

	wantLive(t, f.drained(), fireName(a), a, b)
}

func TestFireUnknownTargetIsParked(t *testing.T) {
	t.Parallel()

	f := newFireFeed(t)

	if park := f.update("N|2026-11-28|25|1.00000|2.00000", "", 1); park != parkUnknownTarget {
		t.Fatalf("update park %q", park)
	}

	if park := f.retract("N|2026-11-28|25|1.00000|2.00000"); park != parkUnknownTarget {
		t.Fatalf("retract park %q", park)
	}

	if f.e.counts.UnknownTarget != 2 {
		t.Fatalf("unknown target count %d, want 2", f.e.counts.UnknownTarget)
	}
}

// randomFeed builds a fixed pseudo-random stream: out-of-order and late detections, re-sends, confidence flips, retracts and heartbeats, packed into a small area so fires merge and split.
func randomFeed(n int) []feedEvent {
	r := rand.New(rand.NewPCG(7, 11)) //nolint:gosec // test data, not security
	out := make([]feedEvent, 0, n)

	var dets []feedEvent

	confs := []string{confNominal, confNominal, confNominal, confHigh, confLow}

	for off := range int64(n) {
		minute := off/2 - r.Int64N(900)

		switch p := r.IntN(100); {
		case p < 55 || len(dets) == 0:
			sat := []string{"N", "N20", "N21"}[r.IntN(3)]
			ev := detectionEvent(off, sat, minute, float64(r.IntN(4000))*0.0005, confs[r.IntN(len(confs))], float64(r.IntN(50)))
			dets = append(dets, ev)
			out = append(out, ev)
		case p < 65:
			ev := dets[r.IntN(len(dets))]
			ev.Offset = off
			out = append(out, ev)
		case p < 85:
			conf, frp := confs[r.IntN(len(confs))], float64(r.IntN(50))
			out = append(out, feedEvent{Offset: off, Kind: feedUpdate, Key: dets[r.IntN(len(dets))].Key, Changes: detectionChange{Confidence: &conf, FRP: &frp}})
		case p < 92:
			out = append(out, feedEvent{Offset: off, Kind: feedRetract, Key: dets[r.IntN(len(dets))].Key})
		default:
			out = append(out, feedEvent{Offset: off, Kind: feedHeartbeat, Now: fireT0 + (off/2)*60})
		}
	}

	return out
}

// runFeed applies a stream, draining every `every` events, and returns the last saved state of every fire, the park reasons and the counts.
func runFeed(events []feedEvent, every int) (map[string]fireSnapshot, []string, fireCounts) {
	e := newFireEngine(12 * time.Hour)
	saved := map[string]fireSnapshot{}
	parks := make([]string, 0, len(events))

	save := func() {
		for _, s := range e.drain() {
			saved[s.Name] = s
		}
	}

	for i, ev := range events {
		parks = append(parks, e.apply(ev))

		if (i+1)%every == 0 {
			save()
		}
	}

	save()

	return saved, parks, e.counts
}

// #313 relies on this: the same stream gives the same fires, park decisions and counts, wherever the batches are cut.
func TestFireEngineIsDeterministic(t *testing.T) {
	t.Parallel()

	events := randomFeed(6000)
	want, wantParks, wantCounts := runFeed(events, 1)

	if len(want) < 20 || wantCounts.TooLate == 0 || wantCounts.Singles == 0 {
		t.Fatalf("stream too tame to test anything: %d fires, counts %+v", len(want), wantCounts)
	}

	for _, every := range []int{1, 7, 500, len(events)} {
		got, parks, counts := runFeed(events, every)

		if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(parks, wantParks) || counts != wantCounts {
			t.Fatalf("draining every %d events changed the result", every)
		}
	}
}
