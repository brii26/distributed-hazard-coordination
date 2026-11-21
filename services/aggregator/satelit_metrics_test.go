package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func metricsText(s *satelitStats) string {
	var b strings.Builder

	s.writeMetrics(&b)

	return b.String()
}

// engineAfter is the fire engine after every record of recs that parses.
func engineAfter(recs []feedRecord) *fireEngine {
	e := newFireEngine(12 * time.Hour)

	for _, r := range recs {
		if ev, err := parseFeedEvent(r.Kind, r.Data); err == nil && ev.Offset == r.Offset {
			e.apply(ev)
		}
	}

	return e
}

func engineCountLines(c fireCounts) []string {
	return []string{
		fmt.Sprintf(`aggregator_satelit_engine_count{kind="too_late"} %d`, c.TooLate),
		fmt.Sprintf(`aggregator_satelit_engine_count{kind="unknown_or_closed_target"} %d`, c.UnknownTarget),
		fmt.Sprintf(`aggregator_satelit_engine_count{kind="resent"} %d`, c.Resent),
		fmt.Sprintf(`aggregator_satelit_engine_count{kind="tombstone_rejected"} %d`, c.TombstoneRejects),
		fmt.Sprintf(`aggregator_satelit_engine_count{kind="singles"} %d`, c.Singles),
	}
}

func hasLines(text string, lines ...string) bool {
	for _, l := range lines {
		if !strings.Contains(text, l+"\n") {
			return false
		}
	}

	return true
}

// Criterion 4: the lag series follow the feed's position and the saved offset, every record read is timed to its save, and the engine counts are those of the whole feed.
func TestSatelitMetricsFollowARun(t *testing.T) {
	t.Parallel()

	recs := testFeed(1000)
	ref := engineAfter(recs)
	last := recs[len(recs)-1].Offset

	stats := newSatelitStats()
	session := func(context.Context) (feedSession, error) {
		return feedSession{LatestOffset: last + 5, VirtualNow: time.Unix(ref.clock+600, 0)}, nil
	}

	stop := (&satelitRun{st: newFakeSatStore(), pub: &fakeConfirmPub{}, recs: recs, crashAfter: -1, stats: stats, session: session}).start(t)

	want := append([]string{
		fmt.Sprintf("aggregator_satelit_saved_offset %d", last),
		fmt.Sprintf("aggregator_satelit_latest_offset %d", last+5),
		"aggregator_satelit_offset_lag 5",
		"aggregator_satelit_feed_time_lag_seconds 600",
		`aggregator_satelit_read_to_saved_seconds_bucket{le="+Inf"} 1000`,
		"aggregator_satelit_read_to_saved_seconds_count 1000",
		// 2 malformed records, the rest parked by the engine
		fmt.Sprintf("aggregator_satelit_parked_total %d", 2+ref.counts.TooLate+ref.counts.UnknownTarget),
	}, engineCountLines(ref.counts)...)

	eventually(t, "the metrics of the whole feed", func() bool { return hasLines(metricsText(stats), want...) })

	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}

	text := metricsText(stats)
	if strings.Contains(text, "aggregator_satelit_offset_lag") || strings.Contains(text, "aggregator_satelit_engine_count") {
		t.Fatalf("lag and counts still reported after the pipeline stopped:\n%s", text)
	}

	if !hasLines(text, "aggregator_satelit_read_to_saved_seconds_count 1000") {
		t.Fatalf("the histogram is per process and must stay:\n%s", text)
	}
}

// After a restart only the records past the bookmark are timed, and the counts come back as those of the whole feed.
func TestSatelitMetricsAfterARestart(t *testing.T) {
	t.Parallel()

	recs := testFeed(1000)
	st := newFakeSatStore()
	toEnd(t, st, &fakeConfirmPub{}, recs[:600])

	stats := newSatelitStats()
	stop := (&satelitRun{st: st, pub: &fakeConfirmPub{}, recs: recs, crashAfter: -1, stats: stats}).start(t)

	want := append([]string{"aggregator_satelit_saved_offset 999", "aggregator_satelit_read_to_saved_seconds_count 400"}, engineCountLines(engineAfter(recs).counts)...)

	eventually(t, "the metrics after the restart", func() bool { return hasLines(metricsText(stats), want...) })

	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}

	if !strings.Contains(metricsText(stats), "aggregator_satelit_read_to_saved_seconds_count 400\n") {
		t.Fatal("records replayed below the bookmark were timed")
	}
}

// Before the replay reaches the bookmark the counts are only partly rebuilt, so they are not reported.
func TestSatelitCountsWaitForTheBookmark(t *testing.T) {
	t.Parallel()

	s := newSatelitStats()
	s.started(599)
	s.progress(10, fireCounts{Singles: 3})

	if text := metricsText(s); strings.Contains(text, "engine_count") || !hasLines(text, "aggregator_satelit_pile 10") {
		t.Fatalf("counts reported during the replay, or the pile missing:\n%s", text)
	}

	s.rebuilt(2*time.Second, 100)
	s.progress(0, fireCounts{Singles: 7})

	if text := metricsText(s); !hasLines(text, `aggregator_satelit_engine_count{kind="singles"} 7`, "aggregator_satelit_rebuild_seconds 2") {
		t.Fatalf("counts or rebuild time missing once past the bookmark:\n%s", text)
	}
}

func TestSatelitReadToSavedBuckets(t *testing.T) {
	t.Parallel()

	s := newSatelitStats()
	now := time.Now()
	s.savedBatch(10, 0, fireCounts{}, 0, []time.Time{now.Add(-50 * time.Millisecond), now.Add(-3 * time.Second), now.Add(-time.Hour)})

	want := []string{
		`aggregator_satelit_read_to_saved_seconds_bucket{le="0.1"} 1`,
		`aggregator_satelit_read_to_saved_seconds_bucket{le="2.5"} 1`,
		`aggregator_satelit_read_to_saved_seconds_bucket{le="5"} 2`,
		`aggregator_satelit_read_to_saved_seconds_bucket{le="300"} 2`,
		`aggregator_satelit_read_to_saved_seconds_bucket{le="+Inf"} 3`,
		"aggregator_satelit_read_to_saved_seconds_count 3",
	}

	if text := metricsText(s); !hasLines(text, want...) {
		t.Fatalf("buckets are not cumulative:\n%s", text)
	}
}

func TestSatelitSessionReadsTheFeedPosition(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/session" || r.Header.Get("Authorization") != "Bearer good" {
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		_, _ = w.Write([]byte(`{"profile":"practice","speed":60,"group":"g","latest_offset":433599,"virtual_now":"2027-01-04T19:52:05Z"}`))
	}))
	defer srv.Close()

	r, err := newSatelitReader(srv.URL, "good")
	if err != nil {
		t.Fatal(err)
	}

	s, err := r.session(context.Background())
	if err != nil || s.LatestOffset != 433599 || !s.VirtualNow.Equal(time.Date(2027, 1, 4, 19, 52, 5, 0, time.UTC)) {
		t.Fatalf("session %+v %v", s, err)
	}

	bad, _ := newSatelitReader(srv.URL, "bad")
	if _, err := bad.session(context.Background()); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("a refused token gave %v, want the 401", err)
	}
}
