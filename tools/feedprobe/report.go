package main

import (
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Shared table headers.
var (
	measureValue = []string{"measure", "value"}
	floatHeader  = []string{"measure", "min", "median", "max", "n"}
)

func (a *analysis) report(w io.Writer) error {
	m := &md{w: w}

	m.printf("# feedprobe analysis of `%s`\n", a.opts.in)
	a.writeOverview(m)
	a.writeConnections(m)
	a.writeSession(m)
	a.writeSchema(m)
	a.writeTargets(m)
	a.writeOffsets(m)
	a.writeHeartbeats(m)
	a.writeDelay(m)
	a.writeOutOfOrder(m)
	a.writeBursts(m)
	a.writeSlowReader(m)
	a.writeQuality(m)

	return m.err
}

func (a *analysis) writeOverview(m *md) {
	m.heading("Overview")

	var resumeRepeats, otherRepeats, comments, replay int

	for _, r := range a.recs {
		switch {
		case r.Comment:
			comments++
		case r.resume:
			resumeRepeats++
		case r.dup:
			otherRepeats++
		case r.replay:
			replay++
		}
	}

	rows := [][]string{
		{"records", strconv.Itoa(len(a.recs))},
		{"unique events (by id, comments excluded)", strconv.Itoa(len(a.unique()))},
		{"repeated ids at a resume (`?from=<last id>` is inclusive)", strconv.Itoa(resumeRepeats)},
		{"other repeated ids (server sent an offset twice)", strconv.Itoa(otherRepeats)},
		{"comment lines", strconv.Itoa(comments)},
		{"unique events already in the feed when the connection opened (replay)", strconv.Itoa(replay)},
		{"unreadable capture lines", strconv.Itoa(a.badLines)},
		{"speed used", strconv.FormatFloat(a.speed, 'f', -1, 64) + " (" + a.speedFrom + ")"},
	}

	if len(a.recs) > 0 {
		rows = append(rows, []string{"wall span of events", fmtDur(a.recs[len(a.recs)-1].wall.Sub(a.recs[0].wall))})
	}

	m.table(measureValue, rows)
}

// writeConnections lists every connection event: status, headers, error bodies, reasons (F8, F9).
func (a *analysis) writeConnections(m *md) {
	m.heading("Connections (F8, F9)")

	rows := make([][]string, 0, len(a.conns))

	for _, c := range a.conns {
		rows = append(rows, []string{secs(c.MonoNs), strconv.Itoa(c.ConnSeq), c.Kind, connDetail(c.connRecord)})
	}

	m.table([]string{"t", "conn", "kind", "detail"}, rows)
}

func connDetail(c connRecord) string {
	var parts []string

	add := func(label, v string) {
		if v != "" {
			parts = append(parts, label+"="+v)
		}
	}

	add("url", c.URL)
	add("Last-Event-ID", c.LastEventID)

	if c.Status != 0 {
		add("status", strconv.Itoa(c.Status))
	}

	add("proto", c.Proto)

	for _, k := range slices.Sorted(maps.Keys(c.Headers)) {
		if k != "Date" {
			add(k, c.Headers[k])
		}
	}

	if c.Kind == kindSession && len(c.Session) > 0 {
		add("session", truncate(string(c.Session), 200))
	}

	add("body", truncate(strings.TrimSpace(c.Body), 300))
	add("reason", c.Reason)

	if c.WaitMs != 0 {
		add("wait_ms", strconv.FormatInt(c.WaitMs, 10))
	}

	if c.Events != 0 {
		add("events", strconv.Itoa(c.Events))
	}

	return strings.Join(parts, "; ")
}

// writeSession reports profile, speed and how virtual_now and latest_offset advance (F11).
func (a *analysis) writeSession(m *md) {
	m.heading("Session clock (F11)")

	profiles, speeds := map[string]int{}, map[string]int{}

	for _, s := range a.snaps {
		profiles[s.profile]++
		speeds[s.speedText]++
	}

	m.printf("Snapshots: %d. Profiles: %v. Speed values: %v.\n\n", len(a.snaps), profiles, speeds)

	var rates, offRates []float64

	for i := 1; i < len(a.snaps); i++ {
		prev, cur := a.snaps[i-1], a.snaps[i]
		wall := cur.wall.Sub(prev.wall).Seconds()

		if wall <= 0 {
			continue
		}

		if prev.hasVirtual && cur.hasVirtual {
			rates = append(rates, cur.virtualNow.Sub(prev.virtualNow).Seconds()/wall)
		}

		p, ok1 := parseOffset(prev.latestOffset)
		c, ok2 := parseOffset(cur.latestOffset)

		if ok1 && ok2 {
			offRates = append(offRates, float64(c-p)/wall)
		}
	}

	m.table(floatHeader, [][]string{
		floatRow("virtual seconds per wall second", rates),
		floatRow("latest_offset growth per wall second", offRates),
	})
}

func floatRow(label string, v []float64) []string {
	if len(v) == 0 {
		return []string{label, "-", "-", "-", "0"}
	}

	s := slices.Clone(v)
	slices.Sort(s)

	f := func(x float64) string { return strconv.FormatFloat(x, 'f', 3, 64) }

	return []string{label, f(s[0]), f(s[len(s)/2]), f(s[len(s)-1]), strconv.Itoa(len(s))}
}

// writeSchema lists every field per event kind with its JSON type and an example (F5, F6, F12).
func (a *analysis) writeSchema(m *md) {
	m.heading("Schema per event kind (F5, F6, F12)")

	byKind := map[string][]*rec{}
	for _, r := range a.unique() {
		byKind[r.kind] = append(byKind[r.kind], r)
	}

	for _, kind := range slices.Sorted(maps.Keys(byKind)) {
		recs := byKind[kind]
		present, kinds, example := map[string]int{}, map[string]map[string]bool{}, map[string]string{}
		nonJSON := 0

		for _, r := range recs {
			if !r.jsonOK {
				nonJSON++

				continue
			}

			for p, l := range r.flat {
				present[p]++

				if kinds[p] == nil {
					kinds[p] = map[string]bool{}
					example[p] = l.raw()
				}

				kinds[p][l.kind] = true
			}
		}

		m.printf("### `%s` (%d events, %d not JSON)\n\n", kind, len(recs), nonJSON)

		rows := make([][]string, 0, len(present))
		for _, p := range slices.Sorted(maps.Keys(present)) {
			types := strings.Join(slices.Sorted(maps.Keys(kinds[p])), ",")
			seen := strconv.Itoa(present[p]) + "/" + strconv.Itoa(len(recs))
			rows = append(rows, []string{"`" + p + "`", types, seen, "`" + truncate(example[p], 80) + "`"})
		}

		m.table([]string{"field", "JSON type", "present", "example (raw)"}, rows)
	}
}

// detectionIndex holds the detections seen so far, to find what an update or retract points at.
type detectionIndex struct {
	byKey     map[string]*rec
	byOffset  map[string]*rec
	allKeys   map[string]bool // every detection key in the capture, earlier or later
	retracted map[string]bool
	paths     map[string]bool
}

func newDetectionIndex(recs []*rec) *detectionIndex {
	idx := &detectionIndex{
		byKey: map[string]*rec{}, byOffset: map[string]*rec{}, allKeys: map[string]bool{},
		retracted: map[string]bool{}, paths: map[string]bool{},
	}

	for _, r := range recs {
		if r.kind == kindDetection && r.key != "" {
			idx.allKeys[r.key] = true
		}
	}

	return idx
}

func (idx *detectionIndex) add(r *rec) {
	if r.key != "" {
		idx.byKey[r.key] = r
	}

	if r.SSEID != nil {
		idx.byOffset[*r.SSEID] = r
	}

	for p := range r.flat {
		idx.paths[p] = true
	}
}

// isRefName reports whether a field name looks like it points at another record.
func isRefName(path string) bool {
	name := strings.ToLower(lastSegment(path))

	return slices.ContainsFunc([]string{"id", "ref", "offset", "target"}, func(s string) bool { return strings.Contains(name, s) })
}

// reference finds an id-like field whose value is the offset of an earlier detection.
func (idx *detectionIndex) reference(r *rec) (*rec, string) {
	for _, p := range slices.Sorted(maps.Keys(r.flat)) {
		if !isRefName(p) {
			continue
		}

		if d := idx.byOffset[r.flat[p].text]; d != nil {
			return d, p
		}
	}

	return nil, ""
}

// coverage is the share of detection fields the event also carries (1.0 means a full record).
func (idx *detectionIndex) coverage(r *rec) (float64, bool) {
	if !r.jsonOK || len(idx.paths) == 0 {
		return 0, false
	}

	have := 0

	for p := range idx.paths {
		if _, ok := r.flat[p]; ok {
			have++
		}
	}

	return float64(have) / float64(len(idx.paths)), true
}

// writeTargets shows how update and retract point at what they change (F5, F6).
func (a *analysis) writeTargets(m *md) {
	m.heading("How update and retract identify their target (F5, F6)")

	idx := newDetectionIndex(a.unique())
	stats := map[string]*targetStats{}

	for _, r := range a.unique() {
		switch r.kind {
		case kindDetection:
			idx.add(r)
		case kindUpdate, kindRetract:
			if stats[r.kind] == nil {
				stats[r.kind] = &targetStats{keys: map[string]int{}, refPaths: map[string]int{}, changed: map[string]int{}, reasons: map[string]int{}}
			}

			stats[r.kind].add(r, idx)

			if r.kind == kindRetract {
				idx.retracted[r.key] = true
			}
		}
	}

	writeTargetStats(m, stats)
}

type targetStats struct {
	n, keyEarlier, keyAnywhere, refOnly, afterRetract int
	keys                                              map[string]int
	refPaths, changed, reasons                        map[string]int
	coverage                                          []float64
	examples                                          []string
}

func (st *targetStats) add(r *rec, idx *detectionIndex) {
	st.n++
	st.keys[r.key]++

	target := idx.byKey[r.key]
	if target != nil {
		st.keyEarlier++
	}

	if idx.allKeys[r.key] {
		st.keyAnywhere++
	}

	if idx.retracted[r.key] {
		st.afterRetract++
	}

	if ref, path := idx.reference(r); ref != nil {
		st.refPaths["`"+path+"` = offset of an earlier detection"]++

		if target == nil {
			st.refOnly++
		}

		target = ref
	}

	if c, ok := idx.coverage(r); ok {
		st.coverage = append(st.coverage, c)
	}

	if l, ok := r.flat.find("reason"); ok {
		st.reasons[l.text]++
	}

	st.addChanges(r, target)

	if len(st.examples) < 3 {
		st.examples = append(st.examples, truncate(r.DataRaw, 300))
	}
}

// addChanges counts the fields an event carries that the target detection also has, by field name.
func (st *targetStats) addChanges(r, target *rec) {
	if target == nil {
		return
	}

	for p, l := range r.flat {
		name := lastSegment(p)
		if name == "offset" || name == "key" || name == "reason" {
			continue
		}

		if old, ok := target.flat.find(name); ok {
			label := "`" + name + "`"
			if old.text == l.text {
				label += " (same value as the detection)"
			}

			st.changed[label]++
		}
	}
}

func writeTargetStats(m *md, stats map[string]*targetStats) {
	if len(stats) == 0 {
		m.printf("_no update or retract events in this capture_\n\n")

		return
	}

	for _, kind := range slices.Sorted(maps.Keys(stats)) {
		st := stats[kind]
		of := func(x int) string { return strconv.Itoa(x) + "/" + strconv.Itoa(st.n) }
		maxPerKey := slices.Max(slices.Collect(maps.Values(st.keys)))

		m.printf("### `%s`: %d events\n\n", kind, st.n)
		m.table(measureValue, [][]string{
			{"key equals the key built from an earlier detection's fields", of(st.keyEarlier)},
			{"key equals a detection key anywhere in the capture", of(st.keyAnywhere)},
			{"matched only through an id/offset field", of(st.refOnly)},
			{"key was already retracted earlier", of(st.afterRetract)},
			{"distinct keys / most events for one key", strconv.Itoa(len(st.keys)) + " / " + strconv.Itoa(maxPerKey)},
		})
		m.table(floatHeader, [][]string{floatRow("share of detection fields present (1.0 = full record)", st.coverage)})
		m.printf("Fields compared by name with the target detection:\n\n")
		m.counts("field", st.changed)

		if len(st.refPaths) > 0 {
			m.printf("Reference fields:\n\n")
			m.counts("field", st.refPaths)
		}

		if len(st.reasons) > 0 {
			m.printf("`reason` values:\n\n")
			m.counts("reason", st.reasons)
		}

		m.printf("Examples:\n\n")

		for _, e := range st.examples {
			m.printf("```json\n%s\n```\n\n", e)
		}
	}
}

// connSum is the offset sequence seen on one connection.
type connSum struct {
	from, leid         string
	events, gaps, dups int
	first, last        string
	firstOff, lastOff  int64
	hasFirst           bool
}

// offsetSummary collects the offset facts of a whole capture.
type offsetSummary struct {
	sums                        map[int]*connSum
	gapRows                     [][]string
	retries                     map[string]int
	hbWithID, hbTotal, nonNumID int
}

func (a *analysis) summariseOffsets() *offsetSummary {
	s := &offsetSummary{sums: map[int]*connSum{}, retries: map[string]int{}}

	for _, c := range a.conns {
		if c.Kind == kindConnect {
			s.sums[c.ConnSeq] = &connSum{from: queryFrom(c.URL), leid: c.LastEventID}
		}
	}

	for _, r := range a.recs {
		s.add(r)
	}

	return s
}

func (s *offsetSummary) add(r *rec) {
	if r.Retry != nil {
		s.retries[strconv.FormatInt(*r.Retry, 10)]++
	}

	if r.kind == kindHeartbeat {
		s.hbTotal++

		if r.SSEID != nil {
			s.hbWithID++
		}
	}

	if r.SSEID == nil {
		return
	}

	if !r.offOK {
		s.nonNumID++

		return
	}

	c := s.sums[r.ConnSeq]
	if c == nil {
		c = &connSum{}
		s.sums[r.ConnSeq] = c
	}

	c.events++

	if r.dup && !r.resume {
		c.dups++
	}

	if c.hasFirst && r.off != c.lastOff+1 {
		c.gaps++

		if len(s.gapRows) < 15 {
			s.gapRows = append(s.gapRows, []string{strconv.Itoa(r.ConnSeq), strconv.FormatInt(c.lastOff, 10), strconv.FormatInt(r.off, 10), r.kind})
		}
	}

	if !c.hasFirst {
		c.first, c.firstOff, c.hasFirst = *r.SSEID, r.off, true
	}

	c.last, c.lastOff = *r.SSEID, r.off
}

// writeOffsets checks the offset sequence per connection and at each resume (F2, F8).
func (a *analysis) writeOffsets(m *md) {
	m.heading("Offsets and resume (F2, F8)")

	s := a.summariseOffsets()
	rows := [][]string{}
	prevLast, prevOK := int64(0), false

	for _, seq := range slices.Sorted(maps.Keys(s.sums)) {
		c := s.sums[seq]
		resume := "-"

		if c.hasFirst && prevOK {
			resume = describeResume(prevLast, c.firstOff)
		}

		rows = append(rows, []string{
			strconv.Itoa(seq), c.from, c.leid, strconv.Itoa(c.events), c.first, c.last,
			strconv.Itoa(c.gaps), strconv.Itoa(c.dups), resume,
		})

		if c.hasFirst {
			prevLast, prevOK = c.lastOff, true
		}
	}

	m.table([]string{"conn", "?from", "Last-Event-ID", "events with id", "first id", "last id", "gaps inside", "repeated ids besides the resume repeat", "first id vs previous conn's last id"}, rows)
	m.printf("Heartbeats carrying an id: %d/%d. Non-numeric ids: %d. `retry:` values: %v.\n\n", s.hbWithID, s.hbTotal, s.nonNumID, s.retries)
	m.printf("Gaps (first 15):\n\n")
	m.table([]string{"conn", "previous id", "next id", "next kind"}, s.gapRows)
}

func queryFrom(requestURI string) string {
	_, q, ok := strings.Cut(requestURI, "?")
	if !ok {
		return ""
	}

	for kv := range strings.SplitSeq(q, "&") {
		if v, ok := strings.CutPrefix(kv, "from="); ok {
			return v
		}
	}

	return ""
}

func describeResume(prevLast, first int64) string {
	switch d := first - prevLast; {
	case d == 1:
		return "last+1 (contiguous)"
	case d == 0:
		return "same as last (1 repeat)"
	case d < 0:
		return "rewound " + strconv.FormatInt(-d, 10) + " (repeats)"
	default:
		return "gap of " + strconv.FormatInt(d-1, 10)
	}
}

// writeHeartbeats measures heartbeat spacing in wall and feed time and the fields they carry (F2, F11).
func (a *analysis) writeHeartbeats(m *md) {
	m.heading("Heartbeats (F2, F11)")

	var wallGaps, virtGaps, commentGaps []time.Duration

	var ratios []float64

	fields := map[string]int{}
	last, lastComment := map[int]*rec{}, map[int]*rec{}

	for _, r := range a.recs {
		if r.Comment {
			if p := lastComment[r.ConnSeq]; p != nil {
				commentGaps = append(commentGaps, r.wall.Sub(p.wall))
			}

			lastComment[r.ConnSeq] = r

			continue
		}

		if r.kind != kindHeartbeat {
			continue
		}

		for p := range r.flat {
			fields["`"+p+"`"]++
		}

		if p := last[r.ConnSeq]; p != nil {
			wg := r.wall.Sub(p.wall)
			wallGaps = append(wallGaps, wg)

			if p.virtSrc == ownVirtual && r.virtSrc == ownVirtual {
				vg := r.virt.Sub(p.virt)
				virtGaps = append(virtGaps, vg)

				if wg > 0 {
					ratios = append(ratios, vg.Seconds()/wg.Seconds())
				}
			}
		}

		last[r.ConnSeq] = r
	}

	header := append([]string{"interval"}, distHeader...)
	m.table(header, [][]string{
		append([]string{"heartbeat, wall time"}, distRow(wallGaps)...),
		append([]string{"heartbeat, feed time (virtual_now)"}, distRow(virtGaps)...),
		append([]string{"comment line, wall time"}, distRow(commentGaps)...),
	})
	m.table(floatHeader, [][]string{floatRow("heartbeat feed seconds per wall second", ratios)})
	m.printf("Heartbeat fields:\n\n")
	m.counts("field", fields)
}
