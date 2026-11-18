package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/aggregatorpb"
)

const hazardUUID = "11111111-2222-3333-4444-555555555555"

// Stands in for ListHazards and GetHazard and records what they were asked.
type fakeList struct {
	fakeHazards

	list    func(*aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error)
	one     *aggregatorpb.GetHazardResponse
	oneErr  error
	delay   time.Duration
	listReq *aggregatorpb.ListHazardsRequest
	oneCall *int
	legacy  *int
}

func (f fakeList) ListHazards(ctx context.Context, in *aggregatorpb.ListHazardsRequest, _ ...grpc.CallOption) (*aggregatorpb.ListHazardsResponse, error) {
	if f.listReq != nil {
		*f.listReq = aggregatorpb.ListHazardsRequest{Limit: in.GetLimit(), Cursor: in.GetCursor()}
	}

	select {
	case <-time.After(f.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return f.list(in)
}

func (f fakeList) GetHazard(context.Context, *aggregatorpb.GetHazardRequest, ...grpc.CallOption) (*aggregatorpb.GetHazardResponse, error) {
	if f.oneCall != nil {
		*f.oneCall++
	}

	return f.one, f.oneErr
}

func (f fakeList) GetHazards(ctx context.Context, in *aggregatorpb.GetHazardsRequest, o ...grpc.CallOption) (*aggregatorpb.GetHazardsResponse, error) {
	if f.legacy != nil {
		*f.legacy++
	}

	return f.fakeHazards.GetHazards(ctx, in, o...)
}

// Calls the hazards routes through the JWT middleware and a mux, like main does.
func callHazards(t *testing.T, f fakeList, role, path string, read time.Duration) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()

	api := hazardsAPI{agg: f, timeout: time.Second, readTimeout: read}
	v := newVerifier()
	mux := http.NewServeMux()
	mux.Handle("GET /hazards", v.require(api))
	mux.Handle("GET /hazards/{id}", v.require(http.HandlerFunc(api.serveOne)))
	mux.Handle("GET /hazards/stream", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	req.AddCookie(sessionCookie(cookieJWT, validToken(t, role)))

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	var body map[string]any

	_ = json.Unmarshal(rec.Body.Bytes(), &body)

	return rec, body
}

func ev(id string, ingested time.Time) *aggregatorpb.HazardEvent {
	return &aggregatorpb.HazardEvent{
		HazardId: id, Source: "PVMBG", SourceRefId: "ref-" + id, HazardType: typSeismic, Latitude: new(-1.5),
		OccurredAt: timestamppb.New(occurred), IngestedAt: timestamppb.New(ingested),
	}
}

func TestListReturnsPageCursorAndPartial(t *testing.T) {
	t.Parallel()

	var got aggregatorpb.ListHazardsRequest

	f := fakeList{listReq: &got, list: func(*aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error) {
		return &aggregatorpb.ListHazardsResponse{
			Data: []*aggregatorpb.HazardEvent{ev("a", occurred)}, NextCursor: "next", HasMore: true,
			Partial: true, UnavailableUnits: []string{"shard-3"},
		}, nil
	}}

	rec, body := callHazards(t, f, roleOperator, "/hazards?limit=50&cursor=abc", 0)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}

	if got.GetLimit() != 50 || got.GetCursor() != "abc" {
		t.Errorf("rpc got %v", &got)
	}

	page, _ := body["page"].(map[string]any)
	if page["next_cursor"] != "next" || page["has_more"] != true || page["limit"] != float64(50) {
		t.Errorf("page %v", page)
	}

	if body["partial"] != true || !slices.Equal(toStrings(body["unavailable_units"]), []string{"shard-3"}) {
		t.Errorf("partial %v units %v", body["partial"], body["unavailable_units"])
	}

	if _, has := body["meta"]; has {
		t.Error("meta must not be on the new list")
	}
}

func toStrings(v any) []string {
	var out []string

	list, _ := v.([]any)
	for _, s := range list {
		out = append(out, fmt.Sprint(s))
	}

	return out
}

func TestListPartialIsAlwaysPresent(t *testing.T) {
	t.Parallel()

	f := fakeList{list: func(*aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error) {
		return &aggregatorpb.ListHazardsResponse{}, nil
	}}

	_, body := callHazards(t, f, roleOperator, "/hazards", 0)
	if v, ok := body["partial"]; !ok || v != false {
		t.Errorf("partial %v present %v", v, ok)
	}
}

func TestListMediaLosesRawFields(t *testing.T) {
	t.Parallel()

	f := fakeList{list: func(*aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error) {
		return &aggregatorpb.ListHazardsResponse{Data: []*aggregatorpb.HazardEvent{ev("a", occurred)}}, nil
	}}

	_, body := callHazards(t, f, roleMedia, "/hazards", 0)
	if slices.Contains(keysOf(firstEvent(t, body)), "latitude") {
		t.Error("media saw a raw field")
	}

	rec, _ := callHazards(t, f, roleMedia, "/hazards?fields=latitude", 0)
	if rec.Code != http.StatusForbidden {
		t.Errorf("raw field for media: %d", rec.Code)
	}
}

func TestListBadLimitAndCursor(t *testing.T) {
	t.Parallel()

	f := fakeList{list: func(*aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "bad cursor")
	}}

	for _, q := range []string{"limit=0", "limit=201", "limit=x"} {
		if rec, _ := callHazards(t, f, roleOperator, "/hazards?"+q, 0); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d", q, rec.Code)
		}
	}

	if rec, _ := callHazards(t, f, roleOperator, "/hazards?limit=200&cursor=zzz", 0); rec.Code != http.StatusBadRequest {
		t.Errorf("bad cursor: %d", rec.Code)
	}
}

// Walks the cursor over a fake that pages a fixed list, and expects each record once in order.
func TestWalkingThePagesReturnsEveryRecordOnce(t *testing.T) {
	t.Parallel()

	var all []*aggregatorpb.HazardEvent

	for i := range 7 {
		all = append(all, ev("id"+strconv.Itoa(i), occurred.Add(-time.Duration(i)*time.Second)))
	}

	f := fakeList{list: func(in *aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error) {
		start, _ := strconv.Atoi(in.GetCursor())
		end := min(start+int(in.GetLimit()), len(all))
		resp := &aggregatorpb.ListHazardsResponse{Data: all[start:end]}

		if end < len(all) {
			resp.NextCursor, resp.HasMore = strconv.Itoa(end), true
		}

		return resp, nil
	}}

	var seen []string

	cursor := ""

	for range 10 {
		_, body := callHazards(t, f, roleOperator, "/hazards?limit=3&cursor="+cursor, 0)

		data, _ := body["data"].([]any)
		for _, d := range data {
			m, _ := d.(map[string]any)
			seen = append(seen, fmt.Sprint(m["hazard_id"]))
		}

		page, _ := body["page"].(map[string]any)
		if page["has_more"] != true {
			break
		}

		cursor, _ = page["next_cursor"].(string)
	}

	want := []string{"id0", "id1", "id2", "id3", "id4", "id5", "id6"}
	if !slices.Equal(seen, want) {
		t.Errorf("seen %v", seen)
	}
}

func TestOldPathStillUsedWithLegacyParams(t *testing.T) {
	t.Parallel()

	var legacy int

	f := fakeList{legacy: &legacy, fakeHazards: fakeHazards{resp: oneEvent(t)}}

	for _, q := range []string{"type=SEISMIC", "offset=0&type=SEISMIC", "severity=AWAS&type=VOLCANIC", "q=x&type=SEISMIC"} {
		if rec, _ := callHazards(t, f, roleOperator, "/hazards?"+q, 0); rec.Code != http.StatusOK {
			t.Errorf("%s: %d", q, rec.Code)
		}
	}

	if legacy != 4 {
		t.Errorf("legacy calls %d", legacy)
	}
}

func TestGetByID(t *testing.T) {
	t.Parallel()

	var calls int

	f := fakeList{oneCall: &calls, one: &aggregatorpb.GetHazardResponse{Event: ev(hazardUUID, occurred), Placement: []string{"u@n"}}}

	rec, body := callHazards(t, f, roleOperator, "/hazards/"+hazardUUID, 0)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}

	data, _ := body["data"].(map[string]any)
	if data["hazard_id"] != hazardUUID {
		t.Errorf("data %v", data)
	}

	if _, has := body["placement"]; has {
		t.Error("placement leaked")
	}

	_, body = callHazards(t, f, roleMedia, "/hazards/"+hazardUUID, 0)
	if data, _ := body["data"].(map[string]any); slices.Contains(keysOf(data), "latitude") {
		t.Error("media saw a raw field")
	}
}

func TestGetByIDErrors(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		err  error
		want int
	}{
		"not found":     {status.Error(codes.NotFound, "no"), http.StatusNotFound},
		"no copy":       {status.Error(codes.Unavailable, "shard down"), http.StatusServiceUnavailable},
		"single db":     {status.Error(codes.Unimplemented, "x"), http.StatusServiceUnavailable},
		"invalid":       {status.Error(codes.InvalidArgument, "x"), http.StatusBadRequest},
		"other failure": {status.Error(codes.Internal, "x"), http.StatusBadGateway},
	}

	for name, c := range cases {
		rec, _ := callHazards(t, fakeList{oneErr: c.err}, roleOperator, "/hazards/"+hazardUUID, 0)
		if rec.Code != c.want {
			t.Errorf("%s: %d want %d", name, rec.Code, c.want)
		}
	}
}

func TestMalformedIDIs404WithoutRPC(t *testing.T) {
	t.Parallel()

	var calls int

	f := fakeList{oneCall: &calls, one: &aggregatorpb.GetHazardResponse{Event: ev("x", occurred)}}

	if rec, _ := callHazards(t, f, roleOperator, "/hazards/not-a-uuid", 0); rec.Code != http.StatusNotFound {
		t.Errorf("status %d", rec.Code)
	}

	if calls != 0 {
		t.Errorf("rpc called %d times", calls)
	}
}

func TestStreamRouteIsNotShadowedByID(t *testing.T) {
	t.Parallel()

	if rec, _ := callHazards(t, fakeList{}, roleOperator, "/hazards/stream", 0); rec.Code != http.StatusTeapot {
		t.Errorf("status %d", rec.Code)
	}
}

// A page with one node down takes about 1.5 s: the 3 s read timeout must let it through, the old 1 s one would not.
func TestSlowListPassesUnderReadTimeout(t *testing.T) {
	t.Parallel()

	f := fakeList{delay: 1500 * time.Millisecond, list: func(*aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error) {
		return &aggregatorpb.ListHazardsResponse{Partial: true}, nil
	}}

	if rec, _ := callHazards(t, f, roleOperator, "/hazards", 3*time.Second); rec.Code != http.StatusOK {
		t.Errorf("3 s: %d", rec.Code)
	}

	if rec, _ := callHazards(t, f, roleOperator, "/hazards", time.Second); rec.Code != http.StatusGatewayTimeout {
		t.Errorf("1 s: %d", rec.Code)
	}
}
