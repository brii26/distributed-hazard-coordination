package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
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

// Maps a hazard type to the one source that feeds it.
var typeSources = map[string]string{typeSeismic: sourceBMKG, typeVolcanic: sourcePVMBG}

// Order sources are reported in.
var healthSources = []string{sourceBMKG, sourcePVMBG}

// The Aggregator's internal gRPC read API.
type readAPI struct {
	aggregatorpb.UnimplementedAggregatorServer

	list     func(ctx context.Context, q listQuery) ([]HazardEvent, bool, error)
	ping     func(ctx context.Context) error // checks the Canonical Store
	rabbitUp func(ctx context.Context) error
	breakers map[string]*breaker // by source name

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
	s := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()), grpc.UnaryInterceptor(a.limit))
	aggregatorpb.RegisterAggregatorServer(s, a)

	return s
}

// Concurrency cap. Never queues: past the cap a request gets RESOURCE_EXHAUSTED at once.
func (a *readAPI) limit(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	select {
	case a.sem <- struct{}{}:
		defer func() { <-a.sem }()
	default:
		id, role := caller(ctx)
		withID(id).Warn("read api request", "method", info.FullMethod, "role", role, "result", "rejected", "reason", "max_inflight")

		return nil, status.Error(codes.ResourceExhausted, "too many in-flight requests")
	}

	return next(ctx, req)
}

// Reads the correlation ID and client role from gRPC metadata.
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

// Page size: the default when none is sent, and the max a caller may ask for.
const (
	defaultPageLimit = 50
	maxPageLimit     = 200
	maxSearchLen     = 100
)

var severities = map[string]bool{"NORMAL": true, "WASPADA": true, "SIAGA": true, "AWAS": true}

// Validates paging, filter and search into a store query. Limit 0 is the default, a bigger one is capped.
func pageQuery(req *aggregatorpb.GetHazardsRequest) (listQuery, error) {
	limit := int(req.GetLimit())

	switch {
	case limit < 0:
		return listQuery{}, status.Error(codes.InvalidArgument, "limit must not be negative")
	case limit == 0:
		limit = defaultPageLimit
	case limit > maxPageLimit:
		limit = maxPageLimit
	}

	if req.GetOffset() < 0 {
		return listQuery{}, status.Error(codes.InvalidArgument, "offset must not be negative")
	}

	if sev := req.GetSeverity(); sev != "" && !severities[sev] {
		return listQuery{}, status.Error(codes.InvalidArgument, "severity must be NORMAL, WASPADA, SIAGA or AWAS")
	}

	search := strings.TrimSpace(req.GetQuery())
	if utf8.RuneCountInString(search) > maxSearchLen {
		return listQuery{}, status.Errorf(codes.InvalidArgument, "query must be at most %d characters", maxSearchLen)
	}

	return listQuery{
		HazardType: req.GetHazardType(),
		Severity:   req.GetSeverity(),
		Search:     search,
		Limit:      limit,
		Offset:     int(req.GetOffset()),
	}, nil
}

// Serves one hazard type from the Canonical Store with its source's status. A down source still serves stored data, UNAVAILABLE only when it is down and empty.
func (a *readAPI) GetHazards(ctx context.Context, req *aggregatorpb.GetHazardsRequest) (*aggregatorpb.GetHazardsResponse, error) {
	id, role := caller(ctx)
	withID(id).Info("read api request", "method", "GetHazards", "role", role, "hazard_type", req.GetHazardType())

	src, ok := typeSources[req.GetHazardType()]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "hazard_type must be SEISMIC or VOLCANIC")
	}

	q, err := pageQuery(req)
	if err != nil {
		return nil, err
	}

	events, more, err := a.list(withCorrelation(ctx, id), q)
	if err != nil {
		withID(id).Warn("read api canonical store failed", "method", "GetHazards", "error", err.Error())

		return nil, status.Error(codes.Unavailable, "canonical store unavailable")
	}

	snap := a.breakers[src].snapshot()

	unfiltered := q.Offset == 0 && q.Severity == "" && q.Search == ""
	if len(events) == 0 && unfiltered && snap.Status == statusDown {
		return nil, status.Errorf(codes.Unavailable, "%s is down and no %s data is stored", src, req.GetHazardType())
	}

	data := make([]*aggregatorpb.HazardEvent, 0, len(events))

	for _, e := range events {
		pb, err := toProto(e)
		if err != nil {
			withID(id).Warn("read api encode failed", "method", "GetHazards", "hazard_id", e.HazardID, "error", err.Error())

			return nil, status.Error(codes.Internal, "failed to encode hazard event")
		}

		data = append(data, pb)
	}

	return &aggregatorpb.GetHazardsResponse{Data: data, Meta: metaOf(snap), HasMore: more}, nil
}

// Health of each source, the Canonical Store and RabbitMQ. Only an unreachable store is "down", the rest is "degraded".
func (a *readAPI) GetHealth(ctx context.Context, _ *emptypb.Empty) (*aggregatorpb.HealthStatus, error) {
	r := a.health(ctx)
	h := &aggregatorpb.HealthStatus{
		Status:            r.Status,
		DatabaseConnected: r.Database == connected,
		RabbitmqConnected: r.RabbitMQ == connected,
	}

	for _, s := range r.Sources {
		h.Sources = append(h.Sources, metaOf(s))
	}

	return h, nil
}

const (
	connected    = "connected"
	disconnected = "disconnected"
)

// The Aggregator's health, used by gRPC GetHealth and REST /health.
type healthReport struct {
	Status   string `json:"status"` // ok, degraded or down
	Database string `json:"database"`
	RabbitMQ string `json:"rabbitmq"`

	Sources []sourceStatus `json:"-"`
}

// One source in the REST body, lowercase up/degraded/down (the read API meta stays uppercase).
type sourceHealth struct {
	Source        string     `json:"source"`
	Status        string     `json:"status"`
	LastSuccessAt *time.Time `json:"last_success_at"`
	StaleSince    *time.Time `json:"stale_since"`
}

// healthReport as /health serves it.
type restHealth struct {
	healthReport

	Sources []sourceHealth `json:"sources"`
}

func (r healthReport) rest() restHealth {
	out := restHealth{healthReport: r}

	for _, s := range r.Sources {
		out.Sources = append(out.Sources, sourceHealth{
			Source: s.Source, Status: strings.ToLower(s.Status), LastSuccessAt: s.LastSuccessAt, StaleSince: s.StaleSince,
		})
	}

	return out
}

// Checks the Canonical Store and RabbitMQ and reads each source's state.
func (a *readAPI) health(ctx context.Context) healthReport {
	r := healthReport{Status: "ok", Database: connected, RabbitMQ: connected}

	if a.ping(ctx) != nil {
		r.Database = disconnected
	}

	if a.rabbitUp(ctx) != nil {
		r.RabbitMQ = disconnected
	}

	for _, name := range healthSources {
		s := a.breakers[name].snapshot()
		r.Sources = append(r.Sources, s)

		if s.Status == statusDown {
			r.Status = "degraded"
		}
	}

	switch {
	case r.Database == disconnected:
		r.Status = "down"
	case r.RabbitMQ == disconnected:
		r.Status = "degraded"
	}

	return r
}

// GET /health. Only "down" is 503, "degraded" stays 200.
func (a *readAPI) healthHandler(w http.ResponseWriter, r *http.Request) {
	rep := a.health(r.Context())

	code := http.StatusOK
	if rep.Status == "down" {
		code = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	if err := json.NewEncoder(w).Encode(rep.rest()); err != nil {
		log.Printf("failed to write health response: %v", err)
	}
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
