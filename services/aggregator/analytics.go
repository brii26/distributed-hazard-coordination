package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/brii26/distributed-hazard-coordination/services/aggregator/aggregatorpb"
)

// How long one copy gets to count a shard. Longer than copyTimeout: a count scans the window, a list reads one page, and the 7d range has no index on ingested_at.
const analyticsCopyTimeout = 4 * time.Second

// What a range name means: how far back the window goes and how wide one chart bucket is.
type analyticsRange struct {
	window, bucket time.Duration
}

var analyticsRanges = map[string]analyticsRange{
	"1h":  {time.Hour, 5 * time.Minute},
	"6h":  {6 * time.Hour, 30 * time.Minute},
	"24h": {24 * time.Hour, time.Hour},
	"7d":  {7 * 24 * time.Hour, 6 * time.Hour},
}

// One event series cell: a time bucket (unix seconds) and a category.
type seriesKey struct {
	Bucket   int64
	Category string
}

// Counts over some events. Adding two of these is the merge, since every event lives on exactly one shard.
type analytics struct {
	Series   map[seriesKey]int64
	Severity map[string]int64
	Area     map[string]int64
	Source   map[string]int64
}

func newAnalytics() *analytics {
	return &analytics{Series: map[seriesKey]int64{}, Severity: map[string]int64{}, Area: map[string]int64{}, Source: map[string]int64{}}
}

func addCounts[K comparable](dst, src map[K]int64) {
	for k, v := range src {
		dst[k] += v
	}
}

func (a *analytics) add(b *analytics) {
	addCounts(a.Series, b.Series)
	addCounts(a.Severity, b.Severity)
	addCounts(a.Area, b.Area)
	addCounts(a.Source, b.Source)
}

// What one shard is asked to count: events with since <= ingested_at <= asOf, bucketed by bucketSec.
type analyticsSpec struct {
	Since, AsOf time.Time
	BucketSec   int64
}

// One shard's counts from one node. Category is exclusive: a tsunami is SEISMIC with attributes.potential_tsunami true and is not also an earthquake. Unknown severity or area comes back as "". The mask from GROUPING tells the four result sets apart.
func analyticsShardOn(ctx context.Context, db *sql.DB, shard int, sp analyticsSpec) (*analytics, error) {
	rows, err := db.QueryContext(ctx, `
WITH e AS (
  SELECT (floor(extract(epoch FROM ingested_at) / $4) * $4)::bigint AS bucket,
         CASE WHEN hazard_type = 'SEISMIC' AND attributes->>'potential_tsunami' = 'true' THEN 'tsunami'
              WHEN hazard_type = 'SEISMIC' THEN 'earthquake'
              WHEN hazard_type = 'VOLCANIC' THEN 'volcano'
              WHEN hazard_type = 'COASTAL' THEN 'coastal'
              WHEN hazard_type = 'WILDFIRE' THEN 'wildfire'
              ELSE 'other' END AS category,
         COALESCE(severity, '') AS severity,
         COALESCE(area_name, '') AS area,
         source
  FROM hazard_events
  WHERE shard = $1 AND ingested_at >= $2::timestamptz AND ingested_at <= $3::timestamptz
    AND withdrawn_at IS NULL
)
SELECT bucket, category, severity, area, source, count(*),
       GROUPING(bucket, category, severity, area, source)
FROM e
GROUP BY GROUPING SETS ((bucket, category), (severity), (area), (source))`,
		shard, sp.Since, sp.AsOf, sp.BucketSec)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	out := newAnalytics()

	for rows.Next() {
		var (
			bucket                      sql.NullInt64
			category, sev, area, source sql.NullString
			n                           int64
			mask                        int
		)

		if err := rows.Scan(&bucket, &category, &sev, &area, &source, &n, &mask); err != nil {
			return nil, err
		}

		switch mask {
		case 0b00111:
			out.Series[seriesKey{bucket.Int64, category.String}] += n
		case 0b11011:
			out.Severity[sev.String] += n
		case 0b11101:
			out.Area[area.String] += n
		case 0b11110:
			out.Source[source.String] += n
		}
	}

	return out, rows.Err()
}

// Counts over all shards. Each shard asks all its copies at once and takes the first answer in order (synced first), so an event is counted once. A shard with no answering copy is named in unavailable and left out. unreachable is how many copies did not answer, so a lone dead replica is visible even though the numbers are complete. unsynced is how many shards were answered by a copy that does not hold the shard in full yet (it may undercount). Never fails because of a down shard.
func (c *cluster) analytics(ctx context.Context, sp analyticsSpec) (total *analytics, unavailable []string, unreachable, unsynced int) {
	live := c.live()
	parts := make([]*analytics, numShards)
	down := make([]int, numShards)
	fromUnsynced := make([]bool, numShards)

	var wg sync.WaitGroup

	for s := range numShards {
		wg.Go(func() {
			own := c.syncedFirst(s, owners(s, live))
			got := make([]*analytics, len(own))

			var cw sync.WaitGroup

			for i, o := range own {
				cw.Go(func() {
					cctx, cancel := context.WithTimeout(ctx, analyticsCopyTimeout)
					defer cancel()

					if a, err := c.analyticsShard(cctx, o.node, s, sp); err == nil {
						got[i] = a
					}
				})
			}

			cw.Wait()

			for i, a := range got {
				if a == nil {
					down[s]++
				} else if parts[s] == nil {
					parts[s] = a
					fromUnsynced[s] = c.isSynced != nil && !c.isSynced(own[i].node.name, s)
				}
			}
		})
	}

	wg.Wait()

	total = newAnalytics()

	for _, d := range down {
		unreachable += d
	}

	for s, p := range parts {
		if p == nil {
			unavailable = append(unavailable, fmt.Sprintf("shard-%d", s))

			continue
		}

		if fromUnsynced[s] {
			unsynced++
		}

		total.add(p)
	}

	return total, unavailable, unreachable, unsynced
}

func keyCounts(m map[string]int64) []*aggregatorpb.KeyCount {
	out := make([]*aggregatorpb.KeyCount, 0, len(m))
	for k, v := range m {
		out = append(out, &aggregatorpb.KeyCount{Key: k, Count: v})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}

		return out[i].Key < out[j].Key
	})

	return out
}

// Event counts by time bucket and category, severity, area and source over a recent window, as of now. Partial when a shard has no reachable copy.
func (a *readAPI) GetAnalytics(ctx context.Context, req *aggregatorpb.GetAnalyticsRequest) (*aggregatorpb.GetAnalyticsResponse, error) {
	id, role := caller(ctx)
	withID(id).Info("read api request", "method", "GetAnalytics", "role", role)

	if a.cl == nil {
		return nil, status.Error(codes.Unimplemented, "analytics needs STORE_SERVICE")
	}

	r, ok := analyticsRanges[req.GetRange()]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "range must be 1h, 6h, 24h or 7d")
	}

	asOf := time.Now().UTC().Truncate(time.Second)
	missing := a.cl.missingNodes()
	total, unavailable, unreachable, unsynced := a.cl.analytics(withCorrelation(ctx, id), analyticsSpec{Since: asOf.Add(-r.window), AsOf: asOf, BucketSec: int64(r.bucket / time.Second)})

	resp := &aggregatorpb.GetAnalyticsResponse{
		AsOf:              timestamppb.New(asOf),
		BucketSeconds:     int32(r.bucket / time.Second), //nolint:gosec // fixed table above, at most 21600
		Partial:           len(unavailable) > 0,
		Degraded:          unreachable > 0 || unsynced > 0 || missing > 0,
		UnreachableCopies: int32(unreachable), //nolint:gosec // at most 2 per shard
		UnavailableUnits:  unavailable,
		BySeverity:        keyCounts(total.Severity),
		ByArea:            keyCounts(total.Area),
		BySource:          keyCounts(total.Source),
	}

	for k, v := range total.Series {
		resp.Series = append(resp.Series, &aggregatorpb.SeriesCount{BucketStart: k.Bucket, Category: k.Category, Count: v})
	}

	sort.Slice(resp.Series, func(i, j int) bool {
		x, y := resp.Series[i], resp.Series[j]
		if x.BucketStart != y.BucketStart {
			return x.BucketStart < y.BucketStart
		}

		return x.Category < y.Category
	})

	return resp, nil
}
