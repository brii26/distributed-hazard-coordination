package main

import (
	"context"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/aggregator/aggregatorpb"
)

const tsunamiAttr = "potential_tsunami"

// What the SQL does, in Go, so the fake nodes can answer like a real one.
func countRows(rows []HazardEvent, shard int, sp analyticsSpec) *analytics {
	out := newAnalytics()

	for _, h := range rows {
		if shardOf(h.HazardID) != shard || h.IngestedAt.Before(sp.Since) || h.IngestedAt.After(sp.AsOf) {
			continue
		}

		cat := "other"

		switch {
		case h.HazardType == typeSeismic && h.Attributes[tsunamiAttr] == true:
			cat = "tsunami"
		case h.HazardType == typeSeismic:
			cat = "earthquake"
		case h.HazardType == typeVolcanic:
			cat = "volcano"
		}

		out.Series[seriesKey{h.IngestedAt.Unix() / sp.BucketSec * sp.BucketSec, cat}]++
		out.Source[h.Source]++

		sev, area := "", ""
		if h.Severity != nil {
			sev = *h.Severity
		}

		if h.AreaName != nil {
			area = *h.AreaName
		}

		out.Severity[sev]++
		out.Area[area]++
	}

	return out
}

func (f *fakeStore) analyticsCluster(names ...string) *cluster {
	c := f.cluster(names...)
	c.analyticsShard = func(ctx context.Context, n *node, shard int, sp analyticsSpec) (*analytics, error) {
		if err := f.gate(ctx, n); err != nil {
			return nil, err
		}

		f.mu.Lock()
		defer f.mu.Unlock()

		var rows []HazardEvent
		for _, h := range f.rows[n.name] {
			rows = append(rows, h)
		}

		return countRows(rows, shard, sp), nil
	}

	return c
}

// 30 events a minute apart, ending a minute before now: every 5th one is a tsunami, every 5th+1 an earthquake, the rest volcanic. Every 7th has no severity and no area.
func seedAnalytics(t *testing.T, c *cluster) {
	t.Helper()

	end := time.Now().UTC().Add(-time.Minute)

	for i := range 30 {
		h := hz("a" + strconv.Itoa(i))
		h.IngestedAt = end.Add(-time.Duration(i) * time.Minute)
		h.HazardType = typeVolcanic

		switch i % 5 {
		case 0:
			h.HazardType, h.Attributes = typeSeismic, map[string]any{tsunamiAttr: true}
		case 1:
			h.HazardType = typeSeismic
		}

		if i%7 != 0 {
			sev, area := "SIAGA", "Area "+strconv.Itoa(i%3)
			h.Severity, h.AreaName = &sev, &area
		}

		if _, err := c.upsert(context.Background(), h); err != nil {
			t.Fatal(err)
		}
	}
}

func sumSeries(r *aggregatorpb.GetAnalyticsResponse, category string) (n int64) {
	for _, s := range r.GetSeries() {
		if category == "" || s.GetCategory() == category {
			n += s.GetCount()
		}
	}

	return n
}

func sumKeys(kc []*aggregatorpb.KeyCount) (n int64) {
	for _, k := range kc {
		n += k.GetCount()
	}

	return n
}

func TestAnalyticsSumsAcrossNodesAndCategoriesAddUp(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.analyticsCluster("a", "b", "c")
	seedAnalytics(t, c)

	resp, err := apiOf(c).GetAnalytics(t.Context(), &aggregatorpb.GetAnalyticsRequest{Range: "1h"})
	if err != nil {
		t.Fatal(err)
	}

	if resp.GetPartial() || len(resp.GetUnavailableUnits()) != 0 {
		t.Fatalf("partial=%v units=%v", resp.GetPartial(), resp.GetUnavailableUnits())
	}

	// i%5==0 is 6 tsunami, i%5==1 is 6 earthquake, 18 volcano, all inside the hour.
	for cat, want := range map[string]int64{"tsunami": 6, "earthquake": 6, "volcano": 18} {
		if got := sumSeries(resp, cat); got != want {
			t.Errorf("%s = %d, want %d", cat, got, want)
		}
	}

	for name, got := range map[string]int64{"series": sumSeries(resp, ""), "severity": sumKeys(resp.GetBySeverity()), "area": sumKeys(resp.GetByArea()), "source": sumKeys(resp.GetBySource())} {
		if got != 30 {
			t.Errorf("%s adds up to %d, want 30", name, got)
		}
	}

	// i%7==0 for i in 0..29 is 5 events with no severity.
	for _, k := range resp.GetBySeverity() {
		if k.GetKey() == "" && k.GetCount() != 5 {
			t.Errorf("unknown severity = %d, want 5", k.GetCount())
		}
	}
}

func TestAnalyticsWindowAndBuckets(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.analyticsCluster("a", "b", "c")
	seedAnalytics(t, c)

	// The 6h range covers all 30; a window of 10 minutes keeps only the newest ones, and an event after as_of is out.
	asOf := time.Now().UTC().Add(-time.Minute - 30*time.Second)
	total, _, _, _ := c.analytics(t.Context(), analyticsSpec{Since: asOf.Add(-10 * time.Minute), AsOf: asOf, BucketSec: 300})

	var n int64

	for k, v := range total.Series {
		if k.Bucket%300 != 0 {
			t.Fatalf("bucket %d is not on a 5 minute edge", k.Bucket)
		}

		n += v
	}

	// Events sit at now-1m-i minutes; asOf is now-1m30s, so i=1..10 qualify.
	if n != 10 {
		t.Fatalf("window holds %d events, want 10", n)
	}
}

func TestAnalyticsOneNodeDownIsDegradedNotPartial(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.analyticsCluster("a", "b", "c")
	seedAnalytics(t, c)

	f.down["a"] = true

	resp, err := apiOf(c).GetAnalytics(t.Context(), &aggregatorpb.GetAnalyticsRequest{Range: "1h"})
	if err != nil {
		t.Fatal(err)
	}

	if resp.GetPartial() || sumSeries(resp, "") != 30 {
		t.Fatalf("partial=%v total=%d, want complete 30", resp.GetPartial(), sumSeries(resp, ""))
	}

	if !resp.GetDegraded() || resp.GetUnreachableCopies() == 0 {
		t.Fatalf("degraded=%v copies=%d, want degraded", resp.GetDegraded(), resp.GetUnreachableCopies())
	}
}

// Every node answers, but a and b do not hold their shards in full yet (a rebuilt node). Shards owned by a and b only have an unsynced answer, which may be low, so the page must be told.
func TestAnalyticsUnsyncedAnswerIsDegraded(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.analyticsCluster("a", "b", "c")
	seedAnalytics(t, c)

	c.isSynced = func(name string, _ int) bool { return name == "c" }

	total, unavailable, unreachable, unsynced := c.analytics(t.Context(), analyticsSpec{Since: time.Now().Add(-time.Hour), AsOf: time.Now(), BucketSec: 300})

	want := 0

	for s := range numShards {
		if own := owners(s, c.live()); own[0].node.name != "c" && own[1].node.name != "c" {
			want++
		}
	}

	if want == 0 || unsynced != want || unreachable != 0 || len(unavailable) != 0 || len(total.Source) == 0 {
		t.Fatalf("unsynced=%d want %d, unreachable=%d unavailable=%v", unsynced, want, unreachable, unavailable)
	}

	resp, err := apiOf(c).GetAnalytics(t.Context(), &aggregatorpb.GetAnalyticsRequest{Range: "1h"})
	if err != nil || !resp.GetDegraded() || resp.GetPartial() || resp.GetUnreachableCopies() != 0 {
		t.Fatalf("err=%v degraded=%v partial=%v", err, resp.GetDegraded(), resp.GetPartial())
	}
}

// Membership has already dropped the dead node, so no copy fails and every answer is synced; only the expected node count shows that a node is gone.
func TestAnalyticsMissingExpectedNodeIsDegraded(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.analyticsCluster("a", "b")
	seedAnalytics(t, c)

	for want, expected := range map[bool]int{false: 0, true: 3} {
		c.expectedNodes = expected

		resp, err := apiOf(c).GetAnalytics(t.Context(), &aggregatorpb.GetAnalyticsRequest{Range: "1h"})
		if err != nil || resp.GetDegraded() != want || resp.GetPartial() || resp.GetUnreachableCopies() != 0 {
			t.Fatalf("expected=%d err=%v degraded=%v partial=%v, want degraded=%v", expected, err, resp.GetDegraded(), resp.GetPartial(), want)
		}
	}
}

func TestAnalyticsAllNodesUpIsNotDegraded(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.analyticsCluster("a", "b", "c")
	seedAnalytics(t, c)

	resp, err := apiOf(c).GetAnalytics(t.Context(), &aggregatorpb.GetAnalyticsRequest{Range: "1h"})
	if err != nil || resp.GetDegraded() || resp.GetUnreachableCopies() != 0 {
		t.Fatalf("err=%v degraded=%v", err, resp.GetDegraded())
	}
}

func TestAnalyticsTwoNodesDownIsPartialWithUnits(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.analyticsCluster("a", "b", "c")
	seedAnalytics(t, c)

	f.down["a"], f.down["b"] = true, true

	resp, err := apiOf(c).GetAnalytics(t.Context(), &aggregatorpb.GetAnalyticsRequest{Range: "1h"})
	if err != nil {
		t.Fatal(err)
	}

	// With 3 nodes and 2 copies, 2 down nodes leave exactly the shards held on both a and b without a copy.
	want := 0

	for s := range numShards {
		if own := owners(s, c.live()); own[0].node.name != "c" && own[1].node.name != "c" {
			want++
		}
	}

	if want == 0 || len(resp.GetUnavailableUnits()) != want || !resp.GetPartial() {
		t.Fatalf("partial=%v units=%v, want %d units", resp.GetPartial(), resp.GetUnavailableUnits(), want)
	}
}

func TestAnalyticsNoNodesListsAllUnits(t *testing.T) {
	t.Parallel()

	c := newFake().analyticsCluster()

	resp, err := apiOf(c).GetAnalytics(t.Context(), &aggregatorpb.GetAnalyticsRequest{Range: "24h"})
	if err != nil {
		t.Fatal(err)
	}

	if !resp.GetPartial() || len(resp.GetUnavailableUnits()) != numShards || sumSeries(resp, "") != 0 {
		t.Fatalf("partial=%v units=%d", resp.GetPartial(), len(resp.GetUnavailableUnits()))
	}
}

func TestAnalyticsRejectsUnknownRange(t *testing.T) {
	t.Parallel()

	for _, r := range []string{"", "2h", "7D"} {
		_, err := apiOf(newFake().analyticsCluster("a")).GetAnalytics(t.Context(), &aggregatorpb.GetAnalyticsRequest{Range: r})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("range %q: code %v, want InvalidArgument", r, status.Code(err))
		}
	}
}

// Real SQL: the GROUPING SETS query on rows with null severity and area_name, a tsunami flag and a late row. Skips without CANONICAL_DB_TEST_URL.
func TestAnalyticsShardOnRealDB(t *testing.T) { //nolint:paralleltest // shared database, fixed far-future window
	s := testStore(t, 2)
	ctx := context.Background()
	at := time.Date(2099, 3, 1, 12, 0, 0, 0, time.UTC)

	shard := -1
	mk := func(typ string, attrs map[string]any, sev, area *string, when time.Time) {
		for {
			h := hazard(sourcePVMBG, ref(t, s))
			if shard < 0 {
				shard = shardOf(h.HazardID)
			}

			if shardOf(h.HazardID) != shard {
				continue
			}

			h.HazardType, h.Attributes, h.Severity, h.AreaName, h.IngestedAt = typ, attrs, sev, area, when

			if _, err := s.upsert(ctx, h); err != nil {
				t.Fatal(err)
			}

			return
		}
	}

	awas, area := severityAwas, "Padang"
	mk(typeSeismic, map[string]any{tsunamiAttr: true}, &awas, &area, at)
	mk(typeSeismic, map[string]any{tsunamiAttr: false}, nil, nil, at.Add(time.Minute))
	mk(typeVolcanic, nil, &awas, &area, at.Add(6*time.Minute))
	mk(typeVolcanic, nil, &awas, &area, at.Add(time.Hour)) // after as_of

	got, err := analyticsShardOn(ctx, s.db, shard, analyticsSpec{Since: at, AsOf: at.Add(10 * time.Minute), BucketSec: 300})
	if err != nil {
		t.Fatal(err)
	}

	b := at.Unix() / 300 * 300
	for k, want := range map[seriesKey]int64{{b, "tsunami"}: 1, {b, "earthquake"}: 1, {b + 300, "volcano"}: 1} {
		if got.Series[k] != want {
			t.Errorf("series %v = %d, want %d", k, got.Series[k], want)
		}
	}

	if got.Severity["AWAS"] != 2 || got.Severity[""] != 1 || got.Area["Padang"] != 2 || got.Area[""] != 1 || got.Source[sourcePVMBG] != 3 {
		t.Errorf("severity=%v area=%v source=%v", got.Severity, got.Area, got.Source)
	}
}
