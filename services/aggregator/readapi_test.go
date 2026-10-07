package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/brii26/distributed-hazard-coordination/services/aggregator/aggregatorpb"
)

var errBoom = errors.New("boom")

// readAPI pake store berisi events dan semua breaker UP, plus client lewat gRPC in-memory.
func testAPI(t *testing.T, events map[string][]HazardEvent, maxInflight int) (*readAPI, aggregatorpb.AggregatorClient) {
	t.Helper()

	api := &readAPI{
		list: func(_ context.Context, q listQuery) ([]HazardEvent, bool, error) {
			return events[q.HazardType], false, nil
		},
		ping:     func(context.Context) error { return nil },
		rabbitUp: func(context.Context) error { return nil },
		breakers: map[string]*breaker{
			sourceBMKG:  newBreaker(sourceBMKG, 1, time.Hour),
			sourcePVMBG: newBreaker(sourcePVMBG, 1, time.Hour),
		},
		sem: make(chan struct{}, maxInflight),
	}

	lis := bufconn.Listen(1 << 20)
	srv := newGRPCServer(api)

	go func() { _ = srv.Serve(lis) }()

	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return api, aggregatorpb.NewAggregatorClient(conn)
}

// buka breaker satu source (threshold 1) kayak poll gagal.
func trip(api *readAPI, source string) {
	api.breakers[source].success()
	api.breakers[source].failure()
}

func TestSeismicIsUnaffectedByAPVMBGOutage(t *testing.T) {
	t.Parallel()

	api, c := testAPI(t, map[string][]HazardEvent{typeSeismic: {hazard(sourceBMKG, "e1")}}, 10)
	trip(api, sourcePVMBG)

	resp, err := c.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeSeismic})
	if err != nil {
		t.Fatalf("seismic during a PVMBG outage must succeed, got %v", err)
	}

	if len(resp.GetData()) != 1 || resp.GetMeta().GetSource() != sourceBMKG ||
		resp.GetMeta().GetSourceStatus() != statusUp || resp.GetMeta().GetStaleSince() != nil {
		t.Fatalf("expected one event with BMKG UP and no stale_since, got %v", resp)
	}
}

func TestVolcanicDownWithNoDataIsUnavailable(t *testing.T) {
	t.Parallel()

	api, c := testAPI(t, nil, 10)
	api.breakers[sourcePVMBG].failure()

	_, err := c.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeVolcanic})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected UNAVAILABLE, got %v", err)
	}
}

func TestVolcanicDownWithStoredDataIsServedStale(t *testing.T) {
	t.Parallel()

	api, c := testAPI(t, map[string][]HazardEvent{typeVolcanic: {hazard(sourcePVMBG, "r1")}}, 10)
	trip(api, sourcePVMBG)

	resp, err := c.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeVolcanic})
	if err != nil {
		t.Fatalf("stale data must still be served, got %v", err)
	}

	m := resp.GetMeta()
	if len(resp.GetData()) != 1 || m.GetSourceStatus() != statusDown || m.GetStaleSince() == nil || m.GetLastSuccessAt() == nil {
		t.Fatalf("expected data with DOWN, stale_since and last_success_at, got %v", resp)
	}
}

func TestUpSourceWithEmptyStoreIsAnEmptyOK(t *testing.T) {
	t.Parallel()

	_, c := testAPI(t, nil, 10)

	resp, err := c.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeVolcanic})
	if err != nil || len(resp.GetData()) != 0 {
		t.Fatalf("a healthy source with nothing yet is an empty OK, got %v %v", resp, err)
	}
}

func TestEventFieldsRoundTripWithNullsKept(t *testing.T) {
	t.Parallel()

	e := hazard(sourceBMKG, "e1")
	e.HazardType, e.Severity, e.AreaName = typeSeismic, nil, nil

	_, c := testAPI(t, map[string][]HazardEvent{typeSeismic: {e}}, 10)

	resp, err := c.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeSeismic})
	if err != nil {
		t.Fatal(err)
	}

	got := resp.GetData()[0]
	if got.Severity != nil || got.AreaName != nil || got.GetLatitude() != *e.Latitude ||
		!got.GetOccurredAt().AsTime().Equal(e.OccurredAt) || got.GetAttributes().AsMap()["eruption_count_24h"] != 4.0 {
		t.Fatalf("event did not round-trip (null severity/area must stay unset): %v", got)
	}
}

func TestBadHazardTypeIsInvalidArgument(t *testing.T) {
	t.Parallel()

	_, c := testAPI(t, nil, 10)

	for _, typ := range []string{"", "FLOOD", "seismic"} {
		if _, err := c.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typ}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("type %q: expected INVALID_ARGUMENT, got %v", typ, err)
		}
	}
}

func TestStoreErrorIsUnavailableNotUnknown(t *testing.T) {
	t.Parallel()

	api, c := testAPI(t, nil, 10)
	api.list = func(context.Context, listQuery) ([]HazardEvent, bool, error) { return nil, false, errBoom }

	_, err := c.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeSeismic})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected UNAVAILABLE, got %v", err)
	}
}

const degradedStatus = "degraded"

func TestOverTheCapIsResourceExhaustedAndTheRestFinish(t *testing.T) {
	t.Parallel()

	const limit = 2

	api, c := testAPI(t, nil, limit)

	started := make(chan struct{}, limit)
	release := make(chan struct{})
	api.list = func(context.Context, listQuery) ([]HazardEvent, bool, error) {
		started <- struct{}{}

		<-release

		return nil, false, nil
	}

	call := func() error {
		_, err := c.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeSeismic})

		return err
	}

	var wg sync.WaitGroup

	held := make(chan error, limit)

	for range limit {
		wg.Go(func() { held <- call() })
	}

	for range limit {
		<-started
	}

	for range 5 {
		if err := call(); status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("over the cap: expected RESOURCE_EXHAUSTED, got %v", err)
		}
	}

	close(release)
	wg.Wait()

	for range limit {
		if err := <-held; err != nil {
			t.Fatalf("a request within the cap must finish normally, got %v", err)
		}
	}

	if err := call(); err != nil {
		t.Fatalf("expected success after the burst, got %v", err)
	}
}

func TestCallerMetadataIsRead(t *testing.T) {
	t.Parallel()

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(metaCorrelationID, "cid-1", metaClientRole, "Media"))

	if id, role := caller(ctx); id != "cid-1" || role != "Media" {
		t.Fatalf("got %q %q", id, role)
	}

	if id, role := caller(context.Background()); id != "" || role != "" {
		t.Fatalf("no metadata must read as empty, got %q %q", id, role)
	}
}

func TestHealthStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*readAPI)
		want   string
	}{
		{"all up", func(*readAPI) {}, "ok"},
		{"a source down", func(a *readAPI) { trip(a, sourcePVMBG) }, degradedStatus},
		{"both sources down", func(a *readAPI) { trip(a, sourcePVMBG); trip(a, sourceBMKG) }, degradedStatus},
		{"rabbitmq down", func(a *readAPI) { a.rabbitUp = func(context.Context) error { return errBoom } }, degradedStatus},
		{"database down", func(a *readAPI) { a.ping = func(context.Context) error { return errBoom } }, "down"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api, c := testAPI(t, nil, 10)
			tt.mutate(api)

			h, err := c.GetHealth(context.Background(), &emptypb.Empty{})
			if err != nil || h.GetStatus() != tt.want {
				t.Fatalf("expected %q, got %v %v", tt.want, h, err)
			}

			if len(h.GetSources()) != 2 || h.GetSources()[0].GetSource() != sourceBMKG || h.GetSources()[1].GetSource() != sourcePVMBG {
				t.Fatalf("expected BMKG then PVMBG, got %v", h.GetSources())
			}
		})
	}
}

func TestHealthReportsConnectivityFlags(t *testing.T) {
	t.Parallel()

	api, c := testAPI(t, nil, 10)
	api.rabbitUp = func(context.Context) error { return errBoom }

	h, err := c.GetHealth(context.Background(), &emptypb.Empty{})
	if err != nil || !h.GetDatabaseConnected() || h.GetRabbitmqConnected() {
		t.Fatalf("expected database up and rabbitmq down, got %v %v", h, err)
	}
}

// panggil REST /health kayak healthcheck compose.
func getHealth(t *testing.T, api *readAPI) (int, restHealth) {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	newMux(api).ServeHTTP(rec, req)

	var rep restHealth
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
		t.Fatalf("health body is not the report JSON: %v: %s", err, rec.Body)
	}

	return rec.Code, rep
}

func TestRESTHealthStatusAndCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutate   func(*readAPI)
		want     string
		wantCode int
	}{
		{"all up", func(*readAPI) {}, "ok", http.StatusOK},
		{"PVMBG down is still reachable", func(a *readAPI) { trip(a, sourcePVMBG) }, degradedStatus, http.StatusOK},
		{"rabbitmq down is degraded", func(a *readAPI) { a.rabbitUp = func(context.Context) error { return errBoom } }, degradedStatus, http.StatusOK},
		{"canonical store down", func(a *readAPI) { a.ping = func(context.Context) error { return errBoom } }, "down", http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api, _ := testAPI(t, nil, 10)
			tt.mutate(api)

			code, rep := getHealth(t, api)
			if code != tt.wantCode || rep.Status != tt.want {
				t.Fatalf("expected %d %q, got %d %q", tt.wantCode, tt.want, code, rep.Status)
			}
		})
	}
}

func TestRESTHealthBreaksDownEachSource(t *testing.T) {
	t.Parallel()

	api, _ := testAPI(t, nil, 10)
	trip(api, sourcePVMBG)

	_, rep := getHealth(t, api)

	if len(rep.Sources) != 2 || rep.Sources[0].Source != sourceBMKG || rep.Sources[0].Status != "up" ||
		rep.Sources[1].Source != sourcePVMBG || rep.Sources[1].Status != "down" ||
		rep.Sources[1].LastSuccessAt == nil || rep.Sources[1].StaleSince == nil {
		t.Fatalf("expected BMKG up and PVMBG down with last_success_at and stale_since, got %+v", rep.Sources)
	}

	if rep.Database != connected || rep.RabbitMQ != connected {
		t.Fatalf("expected both dependencies connected, got %+v", rep)
	}
}

func TestRESTHealthIsNotHeldUpByTheGRPCCap(t *testing.T) {
	t.Parallel()

	api, _ := testAPI(t, nil, 1)
	api.sem <- struct{}{}

	if code, _ := getHealth(t, api); code != http.StatusOK {
		t.Fatalf("health must answer while the read API is saturated, got %d", code)
	}
}

func TestPageQueryDefaultsCapsAndRefuses(t *testing.T) {
	t.Parallel()

	req := func(limit, offset int32, sev, q string) *aggregatorpb.GetHazardsRequest {
		return &aggregatorpb.GetHazardsRequest{HazardType: typeSeismic, Limit: limit, Offset: offset, Severity: sev, Query: q}
	}

	if got, err := pageQuery(req(0, 0, "", "")); err != nil || got.Limit != defaultPageLimit {
		t.Fatalf("no limit must mean the default, got %+v %v", got, err)
	}

	if got, err := pageQuery(req(100000, 20, "AWAS", "  aceh ")); err != nil || got.Limit != maxPageLimit || got.Offset != 20 || got.Severity != "AWAS" || got.Search != "aceh" {
		t.Fatalf("a big limit must be capped and the search trimmed, got %+v %v", got, err)
	}

	for name, r := range map[string]*aggregatorpb.GetHazardsRequest{
		"negative limit":  req(-1, 0, "", ""),
		"negative offset": req(10, -1, "", ""),
		"unknown level":   req(10, 0, "SEVERE", ""),
		"long search":     req(10, 0, "", strings.Repeat("a", maxSearchLen+1)),
	} {
		if _, err := pageQuery(r); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%s: expected INVALID_ARGUMENT, got %v", name, err)
		}
	}
}

func TestAFilteredEmptyPageIsNotNothingStoredWhileTheSourceIsDown(t *testing.T) {
	t.Parallel()

	api, c := testAPI(t, nil, 10)
	trip(api, sourceBMKG)

	if _, err := c.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeSeismic}); status.Code(err) != codes.Unavailable {
		t.Fatalf("an unfiltered empty first page of a down source is UNAVAILABLE, got %v", err)
	}

	resp, err := c.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeSeismic, Query: "nowhere"})
	if err != nil || len(resp.GetData()) != 0 {
		t.Fatalf("a search with no match must be an empty OK page, got %v %v", resp, err)
	}
}
