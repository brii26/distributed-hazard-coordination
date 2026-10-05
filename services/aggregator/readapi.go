package main

import (
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/brii26/distributed-hazard-coordination/services/aggregator/aggregatorpb"
)

const (
	defaultMaxInflight = 100

	metaCorrelationID = "x-correlation-id"
	metaClientRole    = "x-client-role"
)

// typeSources maps a hazard type to the one source that feeds it, so a request
// for one type only ever looks at that source's state (doc/TRD_M1.md 3.3).
var typeSources = map[string]string{typeSeismic: sourceBMKG, typeVolcanic: sourcePVMBG}

// healthSources fixes the order sources are reported in.
var healthSources = []string{sourceBMKG, sourcePVMBG}

// readAPI is the Aggregator's internal gRPC read API (doc/TRD_M1.md 3.4).
type readAPI struct {
	aggregatorpb.UnimplementedAggregatorServer

	list     func(ctx context.Context, hazardType string) ([]HazardEvent, error)
	ping     func(ctx context.Context) error // the Canonical Store
	rabbitUp func(ctx context.Context) error
	breakers map[string]*breaker // by source name

	// sem is the global concurrency cap: one token per in-flight request.
	sem chan struct{}
}

func newReadAPI(db *store, rabbitUp func(context.Context) error, breakers map[string]*breaker, maxInflight int) *readAPI {
	return &readAPI{
		list:     db.listByType,
		ping:     db.ping,
		rabbitUp: rabbitUp,
		breakers: breakers,
		sem:      make(chan struct{}, maxInflight),
	}
}

func newGRPCServer(a *readAPI) *grpc.Server {
	s := grpc.NewServer(grpc.UnaryInterceptor(a.limit))
	aggregatorpb.RegisterAggregatorServer(s, a)

	return s
}

// limit is the concurrency cap. It never queues: past the cap a request is
// refused at once with RESOURCE_EXHAUSTED, so overload cannot pile up or hang.
func (a *readAPI) limit(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	select {
	case a.sem <- struct{}{}:
		defer func() { <-a.sem }()
	default:
		id, role := caller(ctx)
		log.Printf("read api method=%s correlation_id=%s role=%s result=rejected reason=max_inflight", info.FullMethod, id, role)

		return nil, status.Error(codes.ResourceExhausted, "too many in-flight requests")
	}

	return next(ctx, req)
}

// caller reads the correlation ID and client role the Client-Facing API sends
// as gRPC metadata.
func caller(ctx context.Context) (correlationID, role string) {
	md, _ := metadata.FromIncomingContext(ctx)

	first := func(key string) string {
		if v := md.Get(key); len(v) > 0 {
			return v[0]
		}

		return ""
	}

	return first(metaCorrelationID), first(metaClientRole)
}

// GetHazards serves one hazard type from the Canonical Store along with the state
// of its source. A source that is down still serves what is stored (OK, with
// stale_since set); only a down source with nothing stored is UNAVAILABLE.
func (a *readAPI) GetHazards(ctx context.Context, req *aggregatorpb.GetHazardsRequest) (*aggregatorpb.GetHazardsResponse, error) {
	id, role := caller(ctx)
	log.Printf("read api method=GetHazards correlation_id=%s role=%s hazard_type=%s", id, role, req.GetHazardType())

	src, ok := typeSources[req.GetHazardType()]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "hazard_type must be SEISMIC or VOLCANIC")
	}

	events, err := a.list(ctx, req.GetHazardType())
	if err != nil {
		log.Printf("read api method=GetHazards correlation_id=%s canonical store: %v", id, err)

		return nil, status.Error(codes.Unavailable, "canonical store unavailable")
	}

	snap := a.breakers[src].snapshot()

	if len(events) == 0 && snap.Status == statusDown {
		return nil, status.Errorf(codes.Unavailable, "%s is down and no %s data is stored", src, req.GetHazardType())
	}

	data := make([]*aggregatorpb.HazardEvent, 0, len(events))

	for _, e := range events {
		pb, err := toProto(e)
		if err != nil {
			log.Printf("read api method=GetHazards correlation_id=%s hazard_id=%s: %v", id, e.HazardID, err)

			return nil, status.Error(codes.Internal, "failed to encode hazard event")
		}

		data = append(data, pb)
	}

	return &aggregatorpb.GetHazardsResponse{Data: data, Meta: metaOf(snap)}, nil
}

// GetHealth reports each source, the Canonical Store and RabbitMQ. Only the
// store being unreachable is "down"; a down source or broker is "degraded",
// since reads still work (doc/TRD_M1.md 0.2, 3.5).
func (a *readAPI) GetHealth(ctx context.Context, _ *emptypb.Empty) (*aggregatorpb.HealthStatus, error) {
	h := &aggregatorpb.HealthStatus{
		DatabaseConnected: a.ping(ctx) == nil,
		RabbitmqConnected: a.rabbitUp(ctx) == nil,
		Status:            "ok",
	}

	for _, name := range healthSources {
		s := a.breakers[name].snapshot()
		h.Sources = append(h.Sources, metaOf(s))

		if s.Status == statusDown {
			h.Status = "degraded"
		}
	}

	switch {
	case !h.GetDatabaseConnected():
		h.Status = "down"
	case !h.GetRabbitmqConnected():
		h.Status = "degraded"
	}

	return h, nil
}

func metaOf(s sourceStatus) *aggregatorpb.ResponseMeta {
	m := &aggregatorpb.ResponseMeta{Source: s.Source, SourceStatus: s.Status}

	if s.LastSuccessAt != nil {
		m.LastSuccessAt = timestamppb.New(*s.LastSuccessAt)
	}

	if s.StaleSince != nil {
		m.StaleSince = timestamppb.New(*s.StaleSince)
	}

	return m
}

func toProto(e HazardEvent) (*aggregatorpb.HazardEvent, error) {
	attrs, err := structpb.NewStruct(e.Attributes)
	if err != nil {
		return nil, err
	}

	return &aggregatorpb.HazardEvent{
		HazardId:    e.HazardID,
		Source:      e.Source,
		SourceRefId: e.SourceRefID,
		HazardType:  e.HazardType,
		Severity:    e.Severity,
		AreaName:    e.AreaName,
		Latitude:    e.Latitude,
		Longitude:   e.Longitude,
		OccurredAt:  timestamppb.New(e.OccurredAt),
		IngestedAt:  timestamppb.New(e.IngestedAt),
		Attributes:  attrs,
	}, nil
}
