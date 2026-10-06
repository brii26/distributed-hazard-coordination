package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/aggregatorpb"
)

// fakeHazards stands in for the Aggregator's GetHazards and records what it was asked.
type fakeHazards struct {
	aggregatorpb.AggregatorClient

	resp *aggregatorpb.GetHazardsResponse
	err  error

	calls *int
	role  *string
	got   **aggregatorpb.GetHazardsRequest
}

func (f fakeHazards) GetHazards(ctx context.Context, in *aggregatorpb.GetHazardsRequest, _ ...grpc.CallOption) (*aggregatorpb.GetHazardsResponse, error) {
	if f.calls != nil {
		*f.calls++
	}

	if f.got != nil {
		*f.got = in
	}

	if f.role != nil {
		md, _ := metadata.FromOutgoingContext(ctx)
		if v := md.Get("x-client-role"); len(v) > 0 {
			*f.role = v[0]
		}
	}

	return f.resp, f.err
}

//go:fix inline

var occurred = time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

// oneEvent is a response with a full event and one whose severity and area are unknown.
func oneEvent(t *testing.T) *aggregatorpb.GetHazardsResponse {
	t.Helper()

	attrs, err := structpb.NewStruct(map[string]any{"magnitude": 6.1})
	if err != nil {
		t.Fatal(err)
	}

	return &aggregatorpb.GetHazardsResponse{
		Data: []*aggregatorpb.HazardEvent{
			{
				HazardId: "h1", Source: "BMKG", SourceRefId: "E1", HazardType: "SEISMIC",
				Severity: new("WASPADA"), AreaName: new("Sumatera"), Latitude: new(-1.5), Longitude: new(100.2),
				OccurredAt: timestamppb.New(occurred), IngestedAt: timestamppb.New(occurred.Add(time.Second)), Attributes: attrs,
			},
			{HazardId: "h2", Source: "BMKG", SourceRefId: "E2", HazardType: "SEISMIC", OccurredAt: timestamppb.New(occurred), IngestedAt: timestamppb.New(occurred)},
		},
		Meta: &aggregatorpb.ResponseMeta{Source: "BMKG", SourceStatus: "UP", LastSuccessAt: timestamppb.New(occurred)},
	}
}

// getHazards calls GET /hazards through the JWT middleware as a caller with role.
func getHazards(t *testing.T, f fakeHazards, role, query string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/hazards?"+query, nil)
	if role != "" {
		req.AddCookie(sessionCookie(cookieJWT, validToken(t, role)))
	}

	rec := httptest.NewRecorder()
	newVerifier().require(hazardsAPI{agg: f, timeout: time.Second}).ServeHTTP(rec, req)

	var body map[string]any

	_ = json.Unmarshal(rec.Body.Bytes(), &body)

	return rec, body
}

func keysOf(m map[string]any) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}

	slices.Sort(out)

	return out
}

func firstEvent(t *testing.T, body map[string]any) map[string]any {
	t.Helper()

	data, ok := body["data"].([]any)
	if !ok || len(data) == 0 {
		t.Fatalf("no data in %v", body)
	}

	e, ok := data[0].(map[string]any)
	if !ok {
		t.Fatalf("bad event %v", data[0])
	}

	return e
}

func TestMediaSeesOnlyTheSummaryFields(t *testing.T) {
	t.Parallel()

	rec, body := getHazards(t, fakeHazards{resp: oneEvent(t)}, roleMedia, "type=SEISMIC")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d %s", rec.Code, rec.Body)
	}

	want := slices.Sorted(slices.Values(summaryFields))
	if got := keysOf(firstEvent(t, body)); !slices.Equal(got, want) {
		t.Fatalf("Media must get exactly the summary fields %v, got %v", want, got)
	}

	for _, raw := range []string{"source_ref_id", "latitude", "longitude", "attributes"} {
		if body := rec.Body.String(); strings.Contains(body, `"`+raw+`"`) {
			t.Fatalf("a Media response must never carry %s: %s", raw, body)
		}
	}
}

func TestLapanganAndOperatorSeeEveryField(t *testing.T) {
	t.Parallel()

	for _, role := range []string{roleLapangan, roleOperator} {
		rec, body := getHazards(t, fakeHazards{resp: oneEvent(t)}, role, "type=SEISMIC")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d", role, rec.Code)
		}

		e := firstEvent(t, body)
		if len(e) != len(summaryFields)+len(rawFields) || e["latitude"] != -1.5 || e["longitude"] != 100.2 {
			t.Fatalf("%s must get every field, got %v", role, e)
		}

		if attrs, ok := e["attributes"].(map[string]any); !ok || attrs["magnitude"] != 6.1 {
			t.Fatalf("%s: attributes missing or wrong: %v", role, e["attributes"])
		}
	}
}

func TestMediaAskingForARawFieldIsRefusedBeforeTheAggregator(t *testing.T) {
	t.Parallel()

	for _, fields := range []string{"latitude", "hazard_id,longitude", "attributes", "source_ref_id", "hazard_id,source_ref_id"} {
		calls := 0

		rec, _ := getHazards(t, fakeHazards{resp: oneEvent(t), calls: &calls}, roleMedia, "type=SEISMIC&fields="+fields)
		if rec.Code != http.StatusForbidden || calls != 0 {
			t.Fatalf("fields=%s: expected 403 without calling the Aggregator, got %d after %d calls", fields, rec.Code, calls)
		}
	}
}

func TestFieldsSelectsASubset(t *testing.T) {
	t.Parallel()

	rec, body := getHazards(t, fakeHazards{resp: oneEvent(t)}, roleOperator, "type=SEISMIC&fields=hazard_id,latitude,hazard_id")
	if got := keysOf(firstEvent(t, body)); rec.Code != http.StatusOK || !slices.Equal(got, []string{"hazard_id", "latitude"}) {
		t.Fatalf("expected exactly hazard_id and latitude, got %d %v", rec.Code, got)
	}
}

func TestBadQueriesAre400(t *testing.T) {
	t.Parallel()

	for _, q := range []string{"", "type=FLOOD", "type=seismic", "type=SEISMIC&fields=nope"} {
		calls := 0

		if rec, _ := getHazards(t, fakeHazards{resp: oneEvent(t), calls: &calls}, roleOperator, q); rec.Code != http.StatusBadRequest || calls != 0 {
			t.Fatalf("query %q: expected 400 without calling the Aggregator, got %d", q, rec.Code)
		}
	}
}

func TestNoSessionIs401(t *testing.T) {
	t.Parallel()

	if rec, _ := getHazards(t, fakeHazards{resp: oneEvent(t)}, "", "type=SEISMIC"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestUnknownValuesAreNullAndMetaPassesThrough(t *testing.T) {
	t.Parallel()

	resp := oneEvent(t)
	resp.Meta = &aggregatorpb.ResponseMeta{Source: "PVMBG", SourceStatus: "DOWN", LastSuccessAt: timestamppb.New(occurred), StaleSince: timestamppb.New(occurred)}

	_, body := getHazards(t, fakeHazards{resp: resp}, roleOperator, "type=VOLCANIC")

	data, _ := body["data"].([]any)
	second, _ := data[1].(map[string]any)

	if v, ok := second["severity"]; !ok || v != nil {
		t.Fatalf("an unknown severity must be null, not missing: %v", second)
	}

	meta, _ := body["meta"].(map[string]any)
	if meta["source_status"] != "DOWN" || meta["stale_since"] != occurred.Format(time.RFC3339) || meta["last_success_at"] == nil {
		t.Fatalf("meta must carry the stale state, got %v", meta)
	}
}

func TestAggregatorStatusesMapToHTTP(t *testing.T) {
	t.Parallel()

	tests := map[codes.Code]int{
		codes.Unavailable:       http.StatusServiceUnavailable,
		codes.ResourceExhausted: http.StatusTooManyRequests,
		codes.DeadlineExceeded:  http.StatusGatewayTimeout,
		codes.Internal:          http.StatusBadGateway,
	}

	for code, want := range tests {
		rec, body := getHazards(t, fakeHazards{err: status.Error(code, "x")}, roleOperator, "type=VOLCANIC")
		if rec.Code != want || body["error"] == nil {
			t.Fatalf("%v: expected %d with an error body, got %d %v", code, want, rec.Code, body)
		}

		if code == codes.ResourceExhausted && rec.Header().Get("Retry-After") == "" {
			t.Fatal("a 429 must carry Retry-After")
		}
	}
}

func TestRoleIsSentAsMetadata(t *testing.T) {
	t.Parallel()

	var role string

	getHazards(t, fakeHazards{resp: oneEvent(t), role: &role}, roleLapangan, "type=SEISMIC")

	if role != roleLapangan {
		t.Fatalf("expected x-client-role %q, got %q", roleLapangan, role)
	}
}

func TestPagingAndSearchReachTheAggregatorAndComeBackAsPage(t *testing.T) {
	t.Parallel()

	var got *aggregatorpb.GetHazardsRequest

	resp := oneEvent(t)
	resp.HasMore = true

	rec, body := getHazards(t, fakeHazards{resp: resp, got: &got}, roleMedia, "type=SEISMIC&limit=10&offset=20&severity=AWAS&q=%20aceh%20")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d %s", rec.Code, rec.Body)
	}

	if got.GetLimit() != 10 || got.GetOffset() != 20 || got.GetSeverity() != "AWAS" || got.GetQuery() != "aceh" {
		t.Fatalf("the Aggregator must get the page, filter and trimmed search, got %v", got)
	}

	page, _ := body["page"].(map[string]any)
	if page["limit"] != float64(10) || page["offset"] != float64(20) || page["has_more"] != true {
		t.Fatalf("the response must say which page this is, got %v", body["page"])
	}
}

func TestWithoutPagingParamsTheDefaultPageSizeIsUsed(t *testing.T) {
	t.Parallel()

	var got *aggregatorpb.GetHazardsRequest

	if rec, _ := getHazards(t, fakeHazards{resp: oneEvent(t), got: &got}, roleOperator, "type=SEISMIC"); rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if got.GetLimit() != defaultLimit || got.GetOffset() != 0 {
		t.Fatalf("expected the default page, got %v", got)
	}
}

func TestBadPagingParamsAreRefusedBeforeTheAggregator(t *testing.T) {
	t.Parallel()

	for _, q := range []string{"limit=0", "limit=101", "limit=abc", "offset=-1", "offset=1.5", "severity=SEVERE", "q=" + strings.Repeat("a", maxQueryLen+1)} {
		calls := 0

		rec, _ := getHazards(t, fakeHazards{resp: oneEvent(t), calls: &calls}, roleOperator, "type=SEISMIC&"+q)
		if rec.Code != http.StatusBadRequest || calls != 0 {
			t.Fatalf("%s: expected 400 without calling the Aggregator, got %d after %d calls", q, rec.Code, calls)
		}
	}
}

func TestMeReportsTheCallersEmailAndRole(t *testing.T) {
	t.Parallel()

	for _, role := range []string{roleMedia, roleLapangan, roleOperator} {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/me", nil)
		req.AddCookie(sessionCookie(cookieJWT, validToken(t, role)))

		rec := httptest.NewRecorder()
		newVerifier().require(http.HandlerFunc(me)).ServeHTTP(rec, req)

		var body map[string]string

		_ = json.Unmarshal(rec.Body.Bytes(), &body)

		if rec.Code != http.StatusOK || body["role"] != role || body["email"] == "" {
			t.Fatalf("%s: expected 200 with the role, got %d %s", role, rec.Code, rec.Body)
		}
	}

	rec := httptest.NewRecorder()
	newVerifier().require(http.HandlerFunc(me)).ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/me", nil))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("without a session /me must be 401, got %d", rec.Code)
	}
}
