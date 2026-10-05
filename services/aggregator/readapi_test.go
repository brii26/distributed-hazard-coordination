package main

import (
	"context"
	"errors"
	"net"
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

// testAPI returns a readAPI whose store holds events (by hazard type) and whose
// breakers are all UP, plus a client talking to it over an in-memory gRPC server,
// so the interceptor and status codes are exercised as a real caller sees them.
func testAPI(t *testing.T, events map[string][]HazardEvent, maxInflight int) (*readAPI, aggregatorpb.AggregatorClient) {
	t.Helper()

	api := &readAPI{
		list: func(_ context.Context, hazardType string) ([]HazardEvent, error) {
			return events[hazardType], nil
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

// trip opens a source's breaker (threshold 1), as a poll failure would.
func trip(api *readAPI, source string) {
	api.breakers[source].success() // a source that worked once, so stale_since is its last good poll
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
	api.breakers[sourcePVMBG].failure() // never succeeded, now DOWN

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
	api.list = func(context.Context, string) ([]HazardEvent, error) { return nil, errBoom }

	_, err := c.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeSeismic})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected UNAVAILABLE, got %v", err)
	}
}

func TestOverTheCapIsResourceExhaustedAndTheRestFinish(t *testing.T) {
	t.Parallel()

	const cap = 2

	api, c := testAPI(t, nil, cap)

	started := make(chan struct{}, cap)
	release := make(chan struct{})
	api.list = func(context.Context, string) ([]HazardEvent, error) {
		started <- struct{}{}
		<-release

		return nil, nil
	}

	call := func() error {
		_, err := c.GetHazards(context.Background(), &aggregatorpb.GetHazardsRequest{HazardType: typeSeismic})

		return err
	}

	var wg sync.WaitGroup

	held := make(chan error, cap)

	for range cap {
		wg.Go(func() { held <- call() })
	}

	for range cap {
		<-started // both slots are now in flight
	}

	for range 5 {
		if err := call(); status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("over the cap: expected RESOURCE_EXHAUSTED, got %v", err)
		}
	}

	close(release)
	wg.Wait()

	for range cap {
		if err := <-held; err != nil {
			t.Fatalf("a request within the cap must finish normally, got %v", err)
		}
	}

	// A slot is free again once the earlier requests are done.
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
		{"a source down", func(a *readAPI) { trip(a, sourcePVMBG) }, "degraded"},
		{"both sources down", func(a *readAPI) { trip(a, sourcePVMBG); trip(a, sourceBMKG) }, "degraded"},
		{"rabbitmq down", func(a *readAPI) { a.rabbitUp = func(context.Context) error { return errBoom } }, "degraded"},
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
