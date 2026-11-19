package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/aggregatorpb"
)

// Stands in for the Aggregator's GetAnalytics and records the range it was asked for.
type fakeAnalytics struct {
	aggregatorpb.AggregatorClient

	resp *aggregatorpb.GetAnalyticsResponse
	err  error
	got  *string
}

func (f fakeAnalytics) GetAnalytics(_ context.Context, in *aggregatorpb.GetAnalyticsRequest, _ ...grpc.CallOption) (*aggregatorpb.GetAnalyticsResponse, error) {
	if f.got != nil {
		*f.got = in.GetRange()
	}

	return f.resp, f.err
}

// Calls GET /analytics through the JWT middleware as a caller with role.
//
//nolint:dupl // same shape as getHazards, a different handler and fake
func getAnalytics(t *testing.T, f fakeAnalytics, role, query string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/analytics?"+query, nil)
	if role != "" {
		req.AddCookie(sessionCookie(cookieJWT, validToken(t, role)))
	}

	rec := httptest.NewRecorder()
	newVerifier().require(analyticsAPI{agg: f, timeout: time.Second}).ServeHTTP(rec, req)

	var body map[string]any

	_ = json.Unmarshal(rec.Body.Bytes(), &body)

	return rec, body
}

func TestAnalyticsNeedsASession(t *testing.T) {
	t.Parallel()

	if rec, _ := getAnalytics(t, fakeAnalytics{}, "", "range=1h"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestAnalyticsBadRangeIs400BeforeTheAggregator(t *testing.T) {
	t.Parallel()

	for _, q := range []string{"", "range=2h", "range=1H"} {
		var asked string

		rec, _ := getAnalytics(t, fakeAnalytics{got: &asked}, roleMedia, q)
		if rec.Code != http.StatusBadRequest || asked != "" {
			t.Errorf("%q: code %d, aggregator asked for %q", q, rec.Code, asked)
		}
	}
}

func TestAnalyticsPassesPartialThroughForEveryRole(t *testing.T) {
	t.Parallel()

	resp := &aggregatorpb.GetAnalyticsResponse{
		AsOf:              timestamppb.New(occurred),
		BucketSeconds:     300,
		Series:            []*aggregatorpb.SeriesCount{{BucketStart: 1200, Category: "tsunami", Count: 2}},
		BySeverity:        []*aggregatorpb.KeyCount{{Key: "AWAS", Count: 2}},
		Partial:           true,
		Degraded:          true,
		UnreachableCopies: 2,
		UnavailableUnits:  []string{"shard-3"},
	}

	for _, role := range []string{roleMedia, "Operator"} {
		var asked string

		rec, body := getAnalytics(t, fakeAnalytics{resp: resp, got: &asked}, role, "range=6h")
		if rec.Code != http.StatusOK || asked != "6h" {
			t.Fatalf("%s: code %d, asked %q", role, rec.Code, asked)
		}

		if body["degraded"] != true || body["unreachable_copies"] != 2.0 || body["partial"] != true || body["bucket_seconds"] != 300.0 || body["as_of"] != occurred.Format(time.RFC3339) {
			t.Errorf("%s: body %v", role, body)
		}

		if units, _ := body["unavailable_units"].([]any); len(units) != 1 || units[0] != "shard-3" {
			t.Errorf("%s: units %v", role, body["unavailable_units"])
		}

		if series, _ := body["series"].([]any); len(series) != 1 {
			t.Errorf("%s: series %v", role, body["series"])
		}
	}
}

func TestAnalyticsAggregatorDownIs503(t *testing.T) {
	t.Parallel()

	rec, _ := getAnalytics(t, fakeAnalytics{err: status.Error(codes.Unavailable, "down")}, roleMedia, "range=1h")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}
