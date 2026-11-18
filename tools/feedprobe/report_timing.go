package main

import (
	"maps"
	"slices"
	"strconv"
	"time"
)

// writeDelay reports feed time at arrival minus acq time, and per-satellite clock skew (F3, F4).
func (a *analysis) writeDelay(m *md) {
	m.heading("Delay: feed time at arrival minus acq time (F3, F4)")

	var all, live, replay []time.Duration

	perSat := map[string][]time.Duration{}
	sources := map[string]int{}
	missing := 0

	for _, r := range a.ofKind(kindDetection) {
		if !r.acqOK || !r.virtOK {
			missing++

			continue
		}

		d := r.virt.Sub(r.acq)
		all = append(all, d)
		perSat[r.sat] = append(perSat[r.sat], d)
		sources[r.virtSrc]++

		if r.replay {
			replay = append(replay, d)
		} else {
			live = append(live, d)
		}
	}

	header := append([]string{"detections"}, distHeader...)
	rows := [][]string{
		append([]string{"all"}, distRow(all)...),
		append([]string{"live"}, distRow(live)...),
		append([]string{"replay"}, distRow(replay)...),
	}

	for _, sat := range slices.Sorted(maps.Keys(perSat)) {
		rows = append(rows, append([]string{"satellite " + sat}, distRow(perSat[sat])...))
	}

	m.table(header, rows)
	m.printf("Feed time at arrival came from: %v. Detections without acq time or feed time: %d.\n\n", sources, missing)

	skew := make([][]string, 0, len(perSat))

	for _, sat := range slices.Sorted(maps.Keys(perSat)) {
		ds := perSat[sat]
		future, mostFuture := 0, time.Duration(0)

		for _, d := range ds {
			if d < 0 {
				future++
				mostFuture = min(mostFuture, d)
			}
		}

		skew = append(skew, []string{sat, fmtDur(slices.Min(ds)), strconv.Itoa(future) + "/" + strconv.Itoa(len(ds)), fmtDur(-mostFuture)})
	}

	m.printf("Clock skew: a negative minimum delay means the satellite stamps acq times ahead of the feed clock.\n\n")
	m.table([]string{fieldSatellite, "min delay", "acq later than feed time at arrival", "furthest ahead"}, skew)
}

// writeOutOfOrder measures, in offset order, how far each acq time is behind the newest one seen (F3).
func (a *analysis) writeOutOfOrder(m *md) {
	m.heading("Out of order, by offset (F3)")

	all, own := outOfOrderLags(a.ofKind(kindDetection))
	positive := func(ds []time.Duration) int {
		n := 0

		for _, d := range ds {
			if d > 0 {
				n++
			}
		}

		return n
	}

	header := append([]string{"behind the newest acq seen so far"}, distHeader...)
	m.table(header, [][]string{
		append([]string{"any satellite"}, distRow(all)...),
		append([]string{"same satellite"}, distRow(own)...),
	})
	m.printf("Detections behind the newest (any satellite): %d/%d. Behind their own satellite's newest: %d/%d.\n\n",
		positive(all), len(all), positive(own), len(own))
}

// outOfOrderLags walks detections in offset order and returns, for each, how far its acq
// time is behind the newest acq seen so far, over all satellites and within its own satellite.
func outOfOrderLags(dets []*rec) (all, own []time.Duration) {
	dets = slices.Clone(dets)
	slices.SortStableFunc(dets, func(x, y *rec) int {
		if x.offOK && y.offOK {
			return int(x.off - y.off)
		}

		return 0
	})

	var newest time.Time

	newestSat := map[string]time.Time{}

	for _, r := range dets {
		if !r.acqOK {
			continue
		}

		if r.acq.After(newest) {
			newest = r.acq
		}

		if r.acq.After(newestSat[r.sat]) {
			newestSat[r.sat] = r.acq
		}

		all, own = append(all, newest.Sub(r.acq)), append(own, newestSat[r.sat].Sub(r.acq))
	}

	return all, own
}

// writeBursts reports satellite passes (by acq time) and arrival bursts (by wall time) (F7).
func (a *analysis) writeBursts(m *md) {
	m.heading("Passes and bursts (F7)")
	a.writePasses(m)
	a.writeArrivalBursts(m)
}

func (a *analysis) writePasses(m *md) {
	perSat := map[string][]time.Time{}

	for _, r := range a.ofKind(kindDetection) {
		if r.acqOK {
			perSat[r.sat] = append(perSat[r.sat], r.acq)
		}
	}

	rows := make([][]string, 0, len(perSat))

	for _, sat := range slices.Sorted(maps.Keys(perSat)) {
		times := perSat[sat]
		slices.SortFunc(times, func(x, y time.Time) int { return x.Compare(y) })

		var sizes []int

		var spans, gaps []time.Duration

		start, count := times[0], 1

		for i := 1; i < len(times); i++ {
			if gap := times[i].Sub(times[i-1]); gap > a.opts.passGap {
				sizes, spans, gaps = append(sizes, count), append(spans, times[i-1].Sub(start)), append(gaps, gap)
				start, count = times[i], 0
			}

			count++
		}

		sizes, spans = append(sizes, count), append(spans, times[len(times)-1].Sub(start))
		spanDist, gapDist := distRow(spans), distRow(gaps)
		rows = append(rows, []string{
			sat, strconv.Itoa(len(sizes)), strconv.Itoa(intPercentile(sizes, 50)), strconv.Itoa(slices.Max(sizes)),
			spanDist[2], spanDist[6], gapDist[1], gapDist[2],
		})
	}

	m.printf("Passes: detections of one satellite whose acq times are at most %s apart.\n\n", a.opts.passGap)
	m.table([]string{
		fieldSatellite, "passes", "detections/pass p50", "detections/pass max",
		"acq span p50", "acq span max", "gap between passes min", "gap between passes p50",
	}, rows)
}

type burst struct {
	startSec, endSec   int64
	events, detections int
	peak               int
	replay             int
	gapBefore          int64
}

func (a *analysis) writeArrivalBursts(m *md) {
	perSec, detPerSec, replayPerSec := map[int64]int{}, map[int64]int{}, map[int64]int{}

	for _, r := range a.unique() {
		sec := r.MonoNs / int64(time.Second)
		perSec[sec]++

		if r.kind == kindDetection {
			detPerSec[sec]++
		}

		if r.replay {
			replayPerSec[sec]++
		}
	}

	if len(perSec) == 0 {
		m.printf("_no events_\n\n")

		return
	}

	gapSecs := int64(a.opts.burstGap / time.Second)

	var bursts []burst

	for _, sec := range slices.Sorted(maps.Keys(perSec)) {
		if len(bursts) == 0 || sec-bursts[len(bursts)-1].endSec > gapSecs {
			b := burst{startSec: sec}
			if len(bursts) > 0 {
				b.gapBefore = sec - bursts[len(bursts)-1].endSec
			}

			bursts = append(bursts, b)
		}

		b := &bursts[len(bursts)-1]
		b.endSec = sec
		b.events += perSec[sec]
		b.detections += detPerSec[sec]
		b.replay += replayPerSec[sec]
		b.peak = max(b.peak, perSec[sec])
	}

	peakAll, peakDet := slices.Max(slices.Collect(maps.Values(perSec))), 0
	if len(detPerSec) > 0 {
		peakDet = slices.Max(slices.Collect(maps.Values(detPerSec)))
	}

	m.printf("Peak in one wall-clock second: %d events, %d detections. Bursts end after %s without events.\n\n", peakAll, peakDet, a.opts.burstGap)

	rows := make([][]string, 0, len(bursts))

	for i, b := range bursts {
		if i == 40 {
			m.printf("(first 40 of %d bursts shown)\n\n", len(bursts))

			break
		}

		wall := time.Duration(b.endSec-b.startSec+1) * time.Second
		rows = append(rows, []string{
			strconv.FormatInt(b.startSec, 10) + " s", fmtDur(wall), "≈" + fmtDur(a.scale(wall)),
			strconv.Itoa(b.events), strconv.Itoa(b.detections), strconv.Itoa(b.peak), strconv.Itoa(b.replay),
			strconv.FormatInt(b.gapBefore, 10) + " s",
		})
	}

	m.table([]string{"start", "wall duration", "feed duration (wall × speed)", "events", "detections", "peak/s", "replayed", "silence before"}, rows)
}

// writeSlowReader reports what arrived after the scripted pause in reading (F10).
func (a *analysis) writeSlowReader(m *md) {
	m.heading("Slow reader (F10)")

	start, end := a.findConn(kindPauseStart), a.findConn(kindPauseEnd)
	if start == nil || end == nil {
		m.printf("_no pause in this capture_\n\n")

		return
	}

	var before, after []*rec

	for _, r := range a.recs {
		switch {
		case r.MonoNs < start.MonoNs:
			before = append(before, r)
		case r.MonoNs >= end.MonoNs:
			after = append(after, r)
		}
	}

	rows := [][]string{
		{"pause", secs(start.MonoNs) + " → " + secs(end.MonoNs) + " (conn " + strconv.Itoa(start.ConnSeq) + ")"},
		{"events in the 60 s before the pause", strconv.Itoa(countWithin(before, start.MonoNs-int64(time.Minute), time.Minute))},
		{"events in the first 1 s after resuming", strconv.Itoa(countWithin(after, end.MonoNs, time.Second))},
		{"events in the first 5 s after resuming", strconv.Itoa(countWithin(after, end.MonoNs, 5*time.Second))},
		{"events in the first 60 s after resuming", strconv.Itoa(countWithin(after, end.MonoNs, time.Minute))},
	}

	if len(before) > 0 && len(after) > 0 {
		lb, fa := before[len(before)-1], after[0]
		conns := " (conn " + strconv.Itoa(lb.ConnSeq) + " / " + strconv.Itoa(fa.ConnSeq) + ")"
		rows = append(rows,
			[]string{"last id before / first id after", idText(lb) + " / " + idText(fa) + conns},
			[]string{"feed time of last event before / first after", virtText(lb) + " / " + virtText(fa)})
	}

	for _, c := range a.conns {
		if c.Kind == kindDisconnect && c.MonoNs >= start.MonoNs && c.MonoNs <= end.MonoNs+int64(30*time.Second) {
			rows = append(rows, []string{"disconnect near the pause", secs(c.MonoNs) + ": " + c.Reason})
		}
	}

	m.table(measureValue, rows)
}

// findConn returns the last connection record of a kind, or nil.
func (a *analysis) findConn(kind string) *connEntry {
	for i := len(a.conns) - 1; i >= 0; i-- {
		if a.conns[i].Kind == kind {
			return &a.conns[i]
		}
	}

	return nil
}

func countWithin(rs []*rec, from int64, d time.Duration) int {
	n := 0

	for _, r := range rs {
		if r.MonoNs >= from && r.MonoNs < from+d.Nanoseconds() {
			n++
		}
	}

	return n
}

func idText(r *rec) string {
	if r.SSEID == nil {
		return "(none)"
	}

	return *r.SSEID
}

func virtText(r *rec) string {
	if !r.virtOK {
		return "?"
	}

	return r.virt.Format(time.RFC3339)
}
