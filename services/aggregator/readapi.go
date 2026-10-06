package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

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

	list     func(ctx context.Context, q listQuery) ([]HazardEvent, bool, error)
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
		withID(id).Warn("read api request", "method", info.FullMethod, "role", role, "result", "rejected", "reason", "max_inflight")

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

// Page size: the default when a caller sends none, and the cap on what it may ask.
const (
	defaultPageLimit = 50
	maxPageLimit     = 200
	maxSearchLen     = 100
)

var severities = map[string]bool{"NORMAL": true, "WASPADA": true, "SIAGA": true, "AWAS": true}

// pageQuery checks a request's paging, filter and search and turns it into a
// store query. A limit of 0 is the default; a bigger one is capped, not refused.
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

// GetHazards serves one hazard type from the Canonical Store along with the state
// of its source. A source that is down still serves what is stored (OK, with
// stale_since set); only a down source with nothing stored is UNAVAILABLE.
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

	// Only an unfiltered first page that is empty means "nothing stored".
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

// GetHealth reports each source, the Canonical Store and RabbitMQ. Only the
// store being unreachable is "down"; a down source or broker is "degraded",
// since reads still work (doc/TRD_M1.md 0.2, 3.5).
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

// healthReport is the Aggregator's health (doc/TRD_M1.md 0.2, 3.5), shared by the
// gRPC GetHealth and the REST /health.
type healthReport struct {
	Status   string `json:"status"` // ok | degraded | down
	Database string `json:"database"`
	RabbitMQ string `json:"rabbitmq"`

	Sources []sourceStatus `json:"-"`
}

// sourceHealth is one source in the REST body, with the lowercase up/degraded/down
// of doc/TRD_M1.md 3.5 (the read API's meta keeps 3.3's uppercase).
type sourceHealth struct {
	Source        string     `json:"source"`
	Status        string     `json:"status"`
	LastSuccessAt *time.Time `json:"last_success_at"`
	StaleSince    *time.Time `json:"stale_since"`
}

// restHealth is healthReport as /health serves it.
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

// health checks the Canonical Store and RabbitMQ and reads each source's state.
// Only the store being unreachable is "down"; a down source or broker is
// "degraded", since reads still work.
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

// healthHandler serves GET /health. Only "down" is a 503: "degraded" still serves
// and stays 200, so compose and callers can tell "limping" from "gone".
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
