package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Event kinds named by the spec.
const (
	kindDetection = "detection"
	kindUpdate    = "update"
	kindRetract   = "retract"
	kindHeartbeat = "heartbeat"

	fieldSatellite = "satellite"
	// ownVirtual marks a heartbeat whose feed time is its own virtual_now, not an estimate.
	ownVirtual = "own virtual_now"
)

type analyzeOpts struct {
	in       string
	session  string
	speed    float64
	passGap  time.Duration
	burstGap time.Duration
}

// rec is one captured event with everything the report derives from it.
type rec struct {
	eventRecord

	wall    time.Time
	kind    string
	flat    flatJSON
	jsonOK  bool
	off     int64
	offOK   bool
	dup     bool // this id was already received earlier in the capture
	resume  bool // a dup that is the expected repeat at a resume: ?from=<last id> is inclusive (F8)
	replay  bool // the offset already existed when the connection opened
	virt    time.Time
	virtOK  bool
	virtSrc string
	acq     time.Time
	acqOK   bool
	sat     string
	key     string // satellite|acq_date|acq_time|latitude|longitude, text as received
}

type connEntry struct {
	connRecord

	wall time.Time
}

type analysis struct {
	opts      analyzeOpts
	recs      []*rec
	conns     []connEntry
	snaps     []snapshot
	speed     float64
	speedFrom string
	badLines  int
}

func runAnalyze(args []string, stdout io.Writer) error {
	var o analyzeOpts

	fset := flag.NewFlagSet("analyze", flag.ContinueOnError)
	fset.StringVar(&o.in, "in", "", "capture file written by feedprobe capture")
	fset.StringVar(&o.session, "session", "", "optional output of feedprobe session")
	fset.Float64Var(&o.speed, "speed", 0, "feed speed override (virtual seconds per wall second)")
	fset.DurationVar(&o.passGap, "pass-gap", 20*time.Minute, "acq-time gap that starts a new satellite pass")
	fset.DurationVar(&o.burstGap, "burst-gap", 5*time.Second, "wall-time silence that ends an arrival burst")

	if err := fset.Parse(args); err != nil {
		return err
	}

	if o.in == "" {
		return errors.New("--in is required")
	}

	a, err := loadAnalysis(o)
	if err != nil {
		return err
	}

	return a.report(stdout)
}

func loadAnalysis(o analyzeOpts) (*analysis, error) {
	a := &analysis{opts: o}

	if err := readNDJSON(o.in, func(line []byte) error { return a.addEvent(line) }); err != nil {
		return nil, err
	}

	err := readNDJSON(connPath(o.in), func(line []byte) error { return a.addConn(line) })
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	if o.session != "" {
		if err := readNDJSON(o.session, func(line []byte) error { return a.addSessionLine(line) }); err != nil {
			return nil, err
		}
	}

	slices.SortStableFunc(a.snaps, func(x, y snapshot) int { return x.wall.Compare(y.wall) })
	a.pickSpeed()
	a.markOffsets()
	a.estimateVirtual()

	return a, nil
}

func readNDJSON(path string, add func([]byte) error) error {
	f, err := os.Open(path) //nolint:gosec // the path is the operator's own capture file
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}

	defer func() { _ = f.Close() }() // read-only: a close error loses nothing

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 8<<20)

	for sc.Scan() {
		if len(strings.TrimSpace(sc.Text())) == 0 {
			continue
		}

		if err := add(sc.Bytes()); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}

	if err := sc.Err(); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	return nil
}

func (a *analysis) addEvent(line []byte) error {
	r := &rec{}
	if err := json.Unmarshal(line, &r.eventRecord); err != nil {
		a.badLines++

		return nil //nolint:nilerr // a broken capture line is counted, not fatal
	}

	r.wall, _ = time.Parse(time.RFC3339Nano, r.Wall)
	r.flat, r.jsonOK = flattenJSON(r.DataRaw)
	r.kind = eventKind(r)

	if r.SSEID != nil {
		r.off, r.offOK = parseOffset(*r.SSEID)
	}

	if r.jsonOK {
		r.sat = r.flat.text(fieldSatellite)
		r.key = detectionKey(r.flat)

		if t, err := acqDateTime(r.flat.text("acq_date"), r.flat.text("acq_time")); err == nil {
			r.acq, r.acqOK = t, true
		}
	}

	a.recs = append(a.recs, r)

	return nil
}

func parseOffset(id string) (int64, bool) {
	n, err := strconv.ParseInt(strings.TrimSpace(id), 10, 64)

	return n, err == nil
}

// eventKind is the SSE event name, or a "type" field when the feed only sends "message".
func eventKind(r *rec) string {
	if r.Event != defaultEvent || !r.jsonOK {
		return r.Event
	}

	for _, name := range []string{"type", "event", "kind"} {
		if l, ok := r.flat[name]; ok && l.kind == jsonString {
			return l.text
		}
	}

	return r.Event
}

// detectionKey is the event's own top-level "key" when it carries one (update, retract),
// otherwise built as "Definisi Kebakaran" says: the five fields, text taken as is.
func detectionKey(f flatJSON) string {
	if l, ok := f["key"]; ok && l.kind == jsonString {
		return l.text
	}

	parts := make([]string, 0, 5)

	for _, name := range []string{fieldSatellite, "acq_date", "acq_time", "latitude", "longitude"} {
		l, ok := f.find(name)
		if !ok {
			return ""
		}

		parts = append(parts, l.text)
	}

	return strings.Join(parts, "|")
}

func (a *analysis) addConn(line []byte) error {
	var c connEntry
	if err := json.Unmarshal(line, &c.connRecord); err != nil {
		return fmt.Errorf("connection log line: %w", err)
	}

	c.wall, _ = time.Parse(time.RFC3339Nano, c.Wall)
	a.conns = append(a.conns, c)

	if c.Kind == kindSession {
		if s, ok := parseSnapshot(c.Session); ok {
			s.wall, s.connSeq = c.wall, c.ConnSeq
			a.snaps = append(a.snaps, s)
		}
	}

	return nil
}

func (a *analysis) addSessionLine(line []byte) error {
	var l sessionLine
	if err := json.Unmarshal(line, &l); err != nil {
		return fmt.Errorf("session line: %w", err)
	}

	if s, ok := parseSnapshot(l.Session); ok {
		s.wall, _ = time.Parse(time.RFC3339Nano, l.Wall)
		s.connSeq = -1
		a.snaps = append(a.snaps, s)
	}

	return nil
}

func (a *analysis) pickSpeed() {
	a.speed, a.speedFrom = 1, "assumed 1 (no session speed seen)"

	if a.opts.speed > 0 {
		a.speed, a.speedFrom = a.opts.speed, "--speed"

		return
	}

	for _, s := range a.snaps {
		if s.speed > 0 {
			a.speed, a.speedFrom = s.speed, "/v1/session speed"

			return
		}
	}
}

// markOffsets flags repeated ids and events whose offset already existed when their connection opened.
// A repeated id that opens a connection and equals the previous connection's last id is the
// resume repeat; any other repeated id would be the server sending an offset twice.
func (a *analysis) markOffsets() {
	latestAtConnect := map[int]int64{}

	for _, s := range a.snaps {
		if n, ok := parseOffset(s.latestOffset); ok && s.connSeq > 0 {
			latestAtConnect[s.connSeq] = n
		}
	}

	seen := map[int64]bool{}
	prevConn, prevOff := -1, int64(0)

	for _, r := range a.recs {
		if !r.offOK {
			continue
		}

		r.dup = seen[r.off]
		r.resume = r.dup && prevConn >= 0 && r.ConnSeq != prevConn && r.off == prevOff
		seen[r.off] = true
		prevConn, prevOff = r.ConnSeq, r.off

		if latest, ok := latestAtConnect[r.ConnSeq]; ok {
			r.replay = r.off <= latest
		}
	}
}

// estimateVirtual sets the feed time at which each live event arrived: the last
// heartbeat's virtual_now plus speed × elapsed wall time, else the nearest session poll.
// Replayed events get no estimate: replay runs faster than speed, and heartbeats are
// too far apart in feed time to stand in for an event's emission time.
func (a *analysis) estimateVirtual() {
	lastHB := map[int]*rec{}

	for _, r := range a.recs {
		if r.kind == kindHeartbeat && r.jsonOK {
			if l, ok := r.flat.find("virtual_now"); ok {
				if t, ok := parseFeedTime(l); ok {
					r.virt, r.virtOK, r.virtSrc = t, true, ownVirtual
					lastHB[r.ConnSeq] = r

					continue
				}
			}
		}

		hb := lastHB[r.ConnSeq]

		switch {
		case r.replay:
			continue
		case hb != nil:
			r.virt, r.virtOK, r.virtSrc = hb.virt.Add(a.scale(r.wall.Sub(hb.wall))), true, "heartbeat + speed"
		case !r.replay:
			if s, ok := a.nearestSnapshot(r.wall); ok {
				r.virt, r.virtOK, r.virtSrc = s.virtualNow.Add(a.scale(r.wall.Sub(s.wall))), true, "session + speed"
			}
		}
	}
}

func (a *analysis) scale(wall time.Duration) time.Duration {
	return time.Duration(float64(wall) * a.speed)
}

func (a *analysis) nearestSnapshot(wall time.Time) (snapshot, bool) {
	var (
		best  snapshot
		found bool
	)

	for _, s := range a.snaps {
		if s.hasVirtual && (!found || s.wall.Sub(wall).Abs() < best.wall.Sub(wall).Abs()) {
			best, found = s, true
		}
	}

	return best, found
}

// unique is every event except repeats of an id, in arrival order.
func (a *analysis) unique() []*rec {
	out := make([]*rec, 0, len(a.recs))

	for _, r := range a.recs {
		if !r.dup && !r.Comment {
			out = append(out, r)
		}
	}

	return out
}

func (a *analysis) ofKind(kind string) []*rec {
	var out []*rec

	for _, r := range a.unique() {
		if r.kind == kind {
			out = append(out, r)
		}
	}

	return out
}
