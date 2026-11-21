package main

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Metrics of the satellite pipeline (#315, criterion 4): how far the saved state is behind the feed, how long a record takes from read to saved, and the engine's counts.
//
// Only the lease holder runs the pipeline. The lag, pile and count series are left out while this replica does not run it, so a dashboard that takes the max over replicas shows the leader.

const satelitSessionEvery = 5 * time.Second // how often /v1/session is polled for latest_offset and virtual_now

// Upper bounds in seconds for the read-to-saved histogram. A live record is saved within about one batch wait (1 s); the larger buckets catch bursts and catch-up.
var satelitSavedBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

// satelitStats is written by the pipeline and read by /metrics. Methods accept a nil receiver.
type satelitStats struct {
	mu sync.Mutex

	running     bool
	latest      int64 // latest_offset from /v1/session, -1 when unknown
	feedNow     int64 // virtual_now from /v1/session in Unix seconds, 0 when unknown
	saved       int64 // last saved offset (the bookmark), -1 when nothing is saved
	savedClock  int64 // feed clock C at the bookmark in Unix seconds, 0 when unknown
	pile        int
	rebuild     time.Duration
	counts      fireCounts
	countsKnown bool // false while the replay is still below the bookmark

	parked  int64
	buckets []int64 // one per bound, plus +Inf
	sum     time.Duration
	seen    int64
}

func newSatelitStats() *satelitStats {
	return &satelitStats{latest: -1, saved: -1, buckets: make([]int64, len(satelitSavedBuckets)+1)}
}

// started marks the start of a run from the bookmark mark.
func (s *satelitStats) started(mark int64) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.running, s.latest, s.feedNow, s.saved, s.savedClock = true, -1, 0, mark, 0
	s.pile, s.rebuild, s.counts, s.countsKnown = 0, 0, fireCounts{}, mark < 0
}

func (s *satelitStats) stopped() {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.running = false
}

func (s *satelitStats) sessionSeen(latest int64, now time.Time) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.latest, s.feedNow = latest, now.Unix()
}

// rebuilt records the replay reaching the bookmark: how long it took, and the feed clock there.
func (s *satelitStats) rebuilt(took time.Duration, clock int64) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.rebuild, s.savedClock, s.countsKnown = took, clock, true
}

// progress records the pile depth and, once past the bookmark, the engine's counts.
func (s *satelitStats) progress(pile int, counts fireCounts) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.pile = pile

	if s.countsKnown {
		s.counts = counts
	}
}

// savedBatch records a saved batch: the new bookmark and its feed clock, the records parked, and each record's time from read to saved.
func (s *satelitStats) savedBatch(last, clock int64, counts fireCounts, parked int, reads []time.Time) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.saved, s.savedClock, s.counts = last, clock, counts
	s.parked += int64(parked)

	for _, at := range reads {
		d := time.Since(at)

		i := 0
		for i < len(satelitSavedBuckets) && d.Seconds() > satelitSavedBuckets[i] {
			i++
		}

		s.buckets[i]++
		s.sum += d
		s.seen++
	}
}

// Writes the series in the Prometheus text format.
func (s *satelitStats) writeMetrics(w io.Writer) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, _ = fmt.Fprintf(w, "# HELP aggregator_satelit_parked_total Satellite records parked in failed_messages by this process.\n# TYPE aggregator_satelit_parked_total counter\naggregator_satelit_parked_total %d\n", s.parked)

	_, _ = fmt.Fprint(w, "# HELP aggregator_satelit_read_to_saved_seconds Time from reading a satellite record to its batch being saved, published and bookmarked.\n# TYPE aggregator_satelit_read_to_saved_seconds histogram\n")

	var cum int64

	for i, le := range satelitSavedBuckets {
		cum += s.buckets[i]
		_, _ = fmt.Fprintf(w, "aggregator_satelit_read_to_saved_seconds_bucket{le=\"%g\"} %d\n", le, cum)
	}

	_, _ = fmt.Fprintf(w, "aggregator_satelit_read_to_saved_seconds_bucket{le=\"+Inf\"} %d\naggregator_satelit_read_to_saved_seconds_sum %g\naggregator_satelit_read_to_saved_seconds_count %d\n", s.seen, s.sum.Seconds(), s.seen)

	if !s.running {
		return
	}

	_, _ = fmt.Fprintf(w, "# HELP aggregator_satelit_saved_offset Last satellite offset saved, published and bookmarked; -1 before the first save.\n# TYPE aggregator_satelit_saved_offset gauge\naggregator_satelit_saved_offset %d\n", s.saved)
	_, _ = fmt.Fprintf(w, "# HELP aggregator_satelit_pile Satellite records read but not yet applied (bounded at %d).\n# TYPE aggregator_satelit_pile gauge\naggregator_satelit_pile %d\n", satelitPile, s.pile)
	_, _ = fmt.Fprintf(w, "# HELP aggregator_satelit_rebuild_seconds Time the last start took to replay the feed up to the bookmark.\n# TYPE aggregator_satelit_rebuild_seconds gauge\naggregator_satelit_rebuild_seconds %g\n", s.rebuild.Seconds())

	if s.latest >= 0 {
		_, _ = fmt.Fprintf(w, "# HELP aggregator_satelit_latest_offset latest_offset reported by the feed's /v1/session.\n# TYPE aggregator_satelit_latest_offset gauge\naggregator_satelit_latest_offset %d\n", s.latest)
		_, _ = fmt.Fprintf(w, "# HELP aggregator_satelit_offset_lag Feed offsets emitted but not yet saved: latest_offset minus the saved offset.\n# TYPE aggregator_satelit_offset_lag gauge\naggregator_satelit_offset_lag %d\n", max(0, s.latest-s.saved))
	}

	if s.feedNow > 0 && s.savedClock > 0 {
		_, _ = fmt.Fprintf(w, "# HELP aggregator_satelit_feed_time_lag_seconds Feed time behind the feed: virtual_now minus the feed clock at the saved offset. Its floor is the heartbeat spacing in feed time.\n# TYPE aggregator_satelit_feed_time_lag_seconds gauge\naggregator_satelit_feed_time_lag_seconds %d\n", max(0, s.feedNow-s.savedClock))
	}

	if !s.countsKnown {
		return
	}

	_, _ = fmt.Fprint(w, "# HELP aggregator_satelit_engine_count Running totals of the fire engine. A replay rebuilds them, so they match a run without a crash.\n# TYPE aggregator_satelit_engine_count gauge\n")

	for _, c := range []struct {
		kind string
		n    int64
	}{
		{parkTooLate, s.counts.TooLate},
		{parkUnknownTarget, s.counts.UnknownTarget},
		{"resent", s.counts.Resent},
		{"tombstone_rejected", s.counts.TombstoneRejects},
		{"singles", s.counts.Singles},
	} {
		_, _ = fmt.Fprintf(w, "aggregator_satelit_engine_count{kind=%q} %d\n", c.kind, c.n)
	}
}
