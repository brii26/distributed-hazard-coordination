package main

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"slices"
	"strconv"
	"time"
)

// The fire engine (P4: #310, #312, #314) turns satellite feed events into fires, one event at a time.
// It does no I/O and never reads the wall clock. The same events in the same order always give the same fires, names, versions and counts; crash recovery by replay from offset 0 relies on that (#313).
//
// The rules, with the reasons in doc/P4_FEED_NOTES.md:
//   - The feed clock C is the largest heartbeat virtual_now or detection acq seen so far.
//   - Lateness: a detection is judged by its own acq, an update or retract by its target's acq. It is accepted only if C − acq ≤ L; otherwise it is parked as too late.
//   - Closing: a fire is final once C > its newest acq + 360 min + L. Nothing accepted later can link to it or change one of its dots, so it is dropped from memory.
//   - Naming: a fire is named after its anchor, the dot with the smallest offset when the fire formed. The name stays when the anchor leaves.

// Park reasons, stored in failed_messages.
const (
	parkTooLate       = "too_late"
	parkUnknownTarget = "unknown_or_closed_target"
)

// Running totals. A replay rebuilds them like the rest of the state.
type fireCounts struct {
	TooLate          int64 // events parked because their detection time is more than L behind the feed clock
	UnknownTarget    int64 // updates and retracts parked because their target is not in memory
	Resent           int64 // identical copies of a detection already held, skipped (rule 1)
	TombstoneRejects int64 // detections, updates and retracts for a retracted key, ignored (S3)
	Singles          int64 // detections that closed outside any fire (rule 5)
}

type fireDot struct {
	key       string
	off       int64 // offset of the first copy; orders anchors
	sat       string
	acq       int64
	lat, lon  float64
	conf      string
	frp       float64
	body      uint64
	retracted bool
	fire      *fire
}

func (d *fireDot) eligible() bool { return !d.retracted && d.conf != confLow }

type fire struct {
	name    string
	anchor  *fireDot // the dot the name comes from, kept after it leaves the fire
	dots    map[string]*fireDot
	newest  int64 // newest acq ever in the fire; closing counts from it
	queued  int64 // newest value already in the expiry queue
	version int64 // offset of the last event that changed the fire
}

func (f *fire) join(d *fireDot) {
	d.fire = f
	f.dots[d.key] = d
	f.newest = max(f.newest, d.acq)
}

// One detection as it is now, for mapping to a HazardEvent.
type dotView struct {
	Key, Satellite, Confidence string
	Acq                        int64
	Lat, Lon, FRP              float64
}

// The state of one fire to save. A withdrawn fire (shrank below 2 dots, or merged into another) keeps the dots it had when it was withdrawn.
type fireSnapshot struct {
	Name        string
	Version     int64
	Withdrawn   bool
	WithdrawnAt int64  // feed clock, Unix seconds
	MergedInto  string // set when withdrawn by a merge
	Dots        []dotView
}

type fireEngine struct {
	lateness int64 // L in seconds
	clock    int64 // C in Unix seconds
	dots     map[string]*fireDot
	grid     fireGrid
	fires    map[string]*fire        // fires in memory, by name
	left     map[string]fireSnapshot // fires withdrawn, or closed while dirty, since the last drain
	dirty    map[string]struct{}     // names changed since the last drain
	expiries expiryQueue
	counts   fireCounts
}

func newFireEngine(lateness time.Duration) *fireEngine {
	return &fireEngine{
		lateness: int64(lateness / time.Second),
		dots:     map[string]*fireDot{},
		grid:     fireGrid{},
		fires:    map[string]*fire{},
		left:     map[string]fireSnapshot{},
		dirty:    map[string]struct{}{},
	}
}

// apply processes one event and returns a park reason, or "" when the event was taken or deliberately ignored.
func (e *fireEngine) apply(ev feedEvent) string {
	switch ev.Kind {
	case feedHeartbeat:
		e.advance(ev.Now)
	case feedDetection:
		return e.detect(ev)
	case feedUpdate, feedRetract:
		return e.change(ev)
	}

	return ""
}

// drain returns the fires changed since the last call, sorted by name, and forgets them.
func (e *fireEngine) drain() []fireSnapshot {
	out := make([]fireSnapshot, 0, len(e.dirty))

	for _, name := range slices.Sorted(maps.Keys(e.dirty)) {
		if f := e.fires[name]; f != nil {
			out = append(out, e.view(f))
		} else if s, ok := e.left[name]; ok {
			out = append(out, s)
		}
	}

	clear(e.dirty)
	clear(e.left)

	return out
}

func (e *fireEngine) late(acq int64) bool { return e.clock-acq > e.lateness }

// horizon is how long after a dot's acq it can still gain a link: a new dot within 360 min of it must also be within L of the clock.
func (e *fireEngine) horizon() int64 { return fireLinkSeconds + e.lateness }

func (e *fireEngine) advance(t int64) {
	if t > e.clock {
		e.clock = t
		e.expire()
	}
}

func (e *fireEngine) detect(ev feedEvent) string {
	e.advance(ev.Det.Acq)

	if e.late(ev.Det.Acq) {
		e.counts.TooLate++

		return parkTooLate
	}

	switch d := e.dots[ev.Key]; {
	case d == nil:
		e.addDot(ev)
	case d.retracted:
		e.counts.TombstoneRejects++
	case d.body == ev.Det.Body:
		e.counts.Resent++
	default:
		d.body = ev.Det.Body
		e.setValues(d, ev.Det.Confidence, ev.Det.FRP, ev.Offset)
	}

	return ""
}

func (e *fireEngine) change(ev feedEvent) string {
	d := e.dots[ev.Key]

	switch {
	case d == nil:
		e.counts.UnknownTarget++

		return parkUnknownTarget
	case e.late(d.acq):
		e.counts.TooLate++

		return parkTooLate
	case d.retracted:
		e.counts.TombstoneRejects++
	case ev.Kind == feedRetract:
		wasEligible := d.eligible()
		d.retracted = true

		if wasEligible {
			e.removeEligible(d, ev.Offset)
		}
	default:
		conf, frp := d.conf, d.frp

		if c := ev.Changes.Confidence; c != nil {
			conf = *c
		}

		if f := ev.Changes.FRP; f != nil {
			frp = *f
		}

		e.setValues(d, conf, frp, ev.Offset)
	}

	return ""
}

func (e *fireEngine) addDot(ev feedEvent) {
	det := ev.Det
	d := &fireDot{
		key: ev.Key, off: ev.Offset, sat: det.Satellite, acq: det.Acq, lat: det.Lat, lon: det.Lon,
		conf: det.Confidence, frp: det.FRP, body: det.Body,
	}
	e.dots[d.key] = d
	e.expiries.push(d.acq+e.horizon(), nil, d)

	if d.eligible() {
		e.addEligible(d, ev.Offset)
	}
}

func (e *fireEngine) setValues(d *fireDot, conf string, frp float64, off int64) {
	if conf == d.conf && frp == d.frp {
		return
	}

	was := d.eligible()
	d.conf, d.frp = conf, frp

	switch now := d.eligible(); {
	case !was && now:
		e.addEligible(d, off)
	case was && !now:
		e.removeEligible(d, off)
	case now && d.fire != nil:
		e.touch(d.fire, off)
	}
}

// addEligible links a dot that can now be in a fire. Fires it links to merge into the one with the oldest anchor; loose dots it links to join that fire.
func (e *fireEngine) addEligible(d *fireDot, off int64) {
	near := e.grid.neighbours(d)
	e.grid.add(d)

	if len(near) == 0 {
		return
	}

	loose := []*fireDot{d}

	var fires []*fire

	for _, n := range near {
		switch {
		case n.fire == nil:
			loose = append(loose, n)
		case !slices.Contains(fires, n.fire):
			fires = append(fires, n.fire)
		}
	}

	if len(fires) == 0 {
		e.touch(e.newFire(loose), off)

		return
	}

	winner := slices.MinFunc(fires, func(a, b *fire) int { return cmp.Compare(a.anchor.off, b.anchor.off) })

	for _, f := range fires {
		if f != winner {
			e.withdraw(f, off, winner.name)

			for _, x := range f.dots {
				winner.join(x)
			}
		}
	}

	for _, x := range loose {
		winner.join(x)
	}

	e.touch(winner, off)
}

// removeEligible takes out a dot that was retracted or dropped to low confidence, and splits its fire if that broke a link.
func (e *fireEngine) removeEligible(d *fireDot, off int64) {
	e.grid.remove(d)

	f := d.fire
	if f == nil {
		return
	}

	d.fire = nil
	delete(f.dots, d.key)
	e.expiries.push(d.acq+e.horizon(), nil, d)
	e.split(f, off)
}

// split re-walks a fire that lost a dot. The part holding the anchor keeps the name; if that part is a single dot or the anchor is gone, the first part with 2 or more dots keeps it. Other parts with 2 or more dots become new fires; single dots go loose. With no part of 2 or more, the fire is withdrawn.
func (e *fireEngine) split(f *fire, off int64) {
	parts := e.components(f)
	keep := keeperPart(f, parts)

	if keep < 0 {
		e.withdraw(f, off, "")
	}

	for i, p := range parts {
		switch {
		case len(p) < 2:
			for _, x := range p {
				x.fire = nil
				e.expiries.push(x.acq+e.horizon(), nil, x)
			}
		case i == keep:
			f.dots = make(map[string]*fireDot, len(p))

			for _, x := range p {
				f.join(x)
			}

			e.touch(f, off)
		default:
			e.touch(e.newFire(p), off)
		}
	}
}

// components returns the connected parts of a fire, each sorted by offset, the parts sorted by their first offset.
func (e *fireEngine) components(f *fire) [][]*fireDot {
	seen := make(map[*fireDot]bool, len(f.dots))

	var parts [][]*fireDot

	for _, key := range slices.Sorted(maps.Keys(f.dots)) {
		start := f.dots[key]
		if seen[start] {
			continue
		}

		seen[start] = true
		part := []*fireDot{start}

		for i := 0; i < len(part); i++ {
			for _, n := range e.grid.neighbours(part[i]) {
				if n.fire == f && !seen[n] {
					seen[n] = true
					part = append(part, n)
				}
			}
		}

		slices.SortFunc(part, byOffset)
		parts = append(parts, part)
	}

	slices.SortFunc(parts, func(a, b []*fireDot) int { return byOffset(a[0], b[0]) })

	return parts
}

func keeperPart(f *fire, parts [][]*fireDot) int {
	first := -1

	for i, p := range parts {
		if len(p) < 2 {
			continue
		}

		if slices.Contains(p, f.anchor) {
			return i
		}

		if first < 0 {
			first = i
		}
	}

	return first
}

// newFire forms a fire from linked dots. It is named after the dot with the smallest offset whose name no fire in memory uses.
func (e *fireEngine) newFire(members []*fireDot) *fire {
	sorted := slices.SortedFunc(slices.Values(members), byOffset)
	anchor, name := sorted[0], ""

	for _, x := range sorted {
		if n := fireName(x.key); e.fires[n] == nil {
			anchor, name = x, n

			break
		}
	}

	if name == "" { // every candidate name is taken by another fire; practically unreachable, but still deterministic
		name = fireName(anchor.key + "|" + strconv.FormatInt(anchor.off, 10))
	}

	f := &fire{name: name, anchor: anchor, dots: make(map[string]*fireDot, len(sorted))}
	for _, x := range sorted {
		f.join(x)
	}

	e.fires[name] = f
	delete(e.left, name) // a withdrawn fire that forms again is the same HazardEvent

	return f
}

// fireName is "fire-" and the first 12 hex digits of SHA-256 of the anchor's key, so a replay gives the same name.
func fireName(key string) string {
	sum := sha256.Sum256([]byte(key))

	return "fire-" + hex.EncodeToString(sum[:6])
}

func (e *fireEngine) touch(f *fire, off int64) {
	f.version = off
	e.dirty[f.name] = struct{}{}

	if f.newest != f.queued {
		f.queued = f.newest
		e.expiries.push(f.newest+e.horizon(), f, nil)
	}
}

func (e *fireEngine) withdraw(f *fire, off int64, mergedInto string) {
	s := e.view(f)
	s.Version, s.Withdrawn, s.WithdrawnAt, s.MergedInto = off, true, e.clock, mergedInto
	e.left[f.name] = s
	e.dirty[f.name] = struct{}{}
	delete(e.fires, f.name)
}

// expire drops the fires and loose dots whose deadline the clock has passed.
func (e *fireEngine) expire() {
	for {
		x, ok := e.expiries.popBefore(e.clock)
		if !ok {
			return
		}

		if f := x.fire; f != nil {
			if e.fires[f.name] == f && x.at == f.newest+e.horizon() {
				e.closeFire(f)
			}

			continue
		}

		if d := x.dot; e.dots[d.key] == d && d.fire == nil {
			if d.eligible() {
				e.counts.Singles++
				e.grid.remove(d)
			}

			delete(e.dots, d.key)
		}
	}
}

func (e *fireEngine) closeFire(f *fire) {
	if _, ok := e.dirty[f.name]; ok {
		e.left[f.name] = e.view(f)
	}

	for _, d := range f.dots {
		e.grid.remove(d)
		delete(e.dots, d.key)
	}

	delete(e.fires, f.name)
}

func (e *fireEngine) view(f *fire) fireSnapshot {
	s := fireSnapshot{Name: f.name, Version: f.version, Dots: make([]dotView, 0, len(f.dots))}

	for _, key := range slices.Sorted(maps.Keys(f.dots)) {
		d := f.dots[key]
		s.Dots = append(s.Dots, dotView{
			Key: d.key, Satellite: d.sat, Confidence: d.conf, Acq: d.acq, Lat: d.lat, Lon: d.lon, FRP: d.frp,
		})
	}

	return s
}

func byOffset(a, b *fireDot) int { return cmp.Compare(a.off, b.off) }

// expiryQueue is a min-heap of deadlines, ties broken by push order so the order is fixed. Entries go stale when their fire or dot changes; expire skips those.
type expiryQueue struct {
	items []expiry
	seq   int64
}

type expiry struct {
	at   int64
	seq  int64
	fire *fire
	dot  *fireDot
}

func (q *expiryQueue) push(at int64, f *fire, d *fireDot) {
	q.seq++
	q.items = append(q.items, expiry{at: at, seq: q.seq, fire: f, dot: d})

	for i := len(q.items) - 1; i > 0; {
		parent := (i - 1) / 2
		if !q.less(i, parent) {
			break
		}

		q.items[i], q.items[parent] = q.items[parent], q.items[i]
		i = parent
	}
}

// popBefore removes and returns the earliest entry if its deadline is before t.
func (q *expiryQueue) popBefore(t int64) (expiry, bool) {
	if len(q.items) == 0 || q.items[0].at >= t {
		return expiry{}, false
	}

	top := q.items[0]
	last := len(q.items) - 1
	q.items[0] = q.items[last]
	q.items = q.items[:last]

	for i := 0; ; {
		small := i

		for _, c := range []int{2*i + 1, 2*i + 2} {
			if c < len(q.items) && q.less(c, small) {
				small = c
			}
		}

		if small == i {
			break
		}

		q.items[i], q.items[small] = q.items[small], q.items[i]
		i = small
	}

	return top, true
}

func (q *expiryQueue) less(i, j int) bool {
	a, b := q.items[i], q.items[j]

	return a.at < b.at || (a.at == b.at && a.seq < b.seq)
}
