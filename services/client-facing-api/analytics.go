package main

import (
	"context"
	"net/http"
	"slices"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/aggregatorpb"
)

var analyticsRanges = []string{"1h", "6h", "24h", "7d"}

// Least time the route waits for the Aggregator: its per-copy count timeout (4 s) plus a margin.
const analyticsMinTimeout = 5 * time.Second

// GET /analytics?range=: the Aggregator's GetAnalytics. Counts only, so every role gets the same body. Sits behind the JWT middleware.
type analyticsAPI struct {
	agg     aggregatorpb.AggregatorClient
	timeout time.Duration // UPSTREAM_TIMEOUT_MS
}

func (h analyticsAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rng := r.URL.Query().Get("range")
	if !slices.Contains(analyticsRanges, rng) {
		writeJSON(w, http.StatusBadRequest, errBody("range must be 1h, 6h, 24h or 7d"))

		return
	}

	// The Aggregator gives each store copy up to 4 s to count, so UPSTREAM_TIMEOUT_MS (1 s) would turn one slow or dead node into a 504 instead of the degraded answer.
	ctx, cancel := context.WithTimeout(r.Context(), max(h.timeout, analyticsMinTimeout))
	defer cancel()

	if claims, ok := claimsFrom(r.Context()); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-client-role", claims.Role)
	}

	resp, err := h.agg.GetAnalytics(ctx, &aggregatorpb.GetAnalyticsRequest{Range: rng})
	if err != nil {
		hazardsFailed(r.Context(), w, err)

		return
	}

	series := make([]map[string]any, 0, len(resp.GetSeries()))
	for _, s := range resp.GetSeries() {
		series = append(series, map[string]any{"bucket_start": s.GetBucketStart(), "category": s.GetCategory(), "count": s.GetCount()})
	}

	counts := func(kc []*aggregatorpb.KeyCount) []map[string]any {
		out := make([]map[string]any, 0, len(kc))
		for _, k := range kc {
			out = append(out, map[string]any{"key": k.GetKey(), "count": k.GetCount()})
		}

		return out
	}

	units := resp.GetUnavailableUnits()
	if units == nil {
		units = []string{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"range":              rng,
		"as_of":              resp.GetAsOf().AsTime().UTC().Format(time.RFC3339),
		"bucket_seconds":     resp.GetBucketSeconds(),
		"series":             series,
		"by_severity":        counts(resp.GetBySeverity()),
		"by_area":            counts(resp.GetByArea()),
		"by_source":          counts(resp.GetBySource()),
		"partial":            resp.GetPartial(),
		"degraded":           resp.GetDegraded(),
		"unreachable_copies": resp.GetUnreachableCopies(),
		"unavailable_units":  units,
	})
}
