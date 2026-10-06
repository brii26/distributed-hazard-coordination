package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/aggregatorpb"
)

// The two field classes of doc/TRD_M1.md 4: every role sees the summary, only
// Tim Lapangan and BNPB Ops see the raw fields.
var (
	summaryFields = []string{"hazard_id", "source", "source_ref_id", "hazard_type", "severity", "area_name", "occurred_at", "ingested_at"}
	rawFields     = []string{"latitude", "longitude", "attributes"}
)

func fieldsFor(role string) []string {
	if role == roleMedia {
		return summaryFields
	}

	return append(slices.Clone(summaryFields), rawFields...)
}

// hazardsAPI is GET /hazards: the Aggregator's GetHazards, filtered per role
// server-side. It sits behind the JWT middleware, so the caller's claims are set.
type hazardsAPI struct {
	agg     aggregatorpb.AggregatorClient
	timeout time.Duration // UPSTREAM_TIMEOUT_MS
}

// errFieldForbidden is a raw field asked for by a role that may not see it.
var errFieldForbidden = errors.New("field not available to this role")

// requestedFields resolves ?fields= for a role. Empty means every field the role
// may see. An unknown field is an error; a raw field asked for explicitly by a
// Media caller is refused outright (TRD 4), not silently dropped.
func requestedFields(raw, role string) ([]string, error) {
	allowed := fieldsFor(role)
	if raw == "" {
		return allowed, nil
	}

	var out []string

	for f := range strings.SplitSeq(raw, ",") {
		f = strings.TrimSpace(f)

		switch {
		case slices.Contains(allowed, f):
			if !slices.Contains(out, f) {
				out = append(out, f)
			}
		case slices.Contains(rawFields, f):
			return nil, errFieldForbidden
		default:
			return nil, errors.New("unknown field " + f)
		}
	}

	return out, nil
}

func (h hazardsAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	claims, _ := claimsFrom(r.Context())
	typ, rawFieldList := r.URL.Query().Get("type"), r.URL.Query().Get("fields")

	if typ != "SEISMIC" && typ != "VOLCANIC" {
		writeJSON(w, http.StatusBadRequest, errBody("type must be SEISMIC or VOLCANIC"))

		return
	}

	fields, err := requestedFields(rawFieldList, claims.Role)
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, errFieldForbidden) {
			code = http.StatusForbidden
		}

		log.Printf("hazards role=%s type=%s fields=%q authorize=denied: %v", claims.Role, typ, rawFieldList, err)
		writeJSON(w, code, errBody(err.Error()))

		return
	}

	log.Printf("hazards role=%s type=%s fields=%q authorize=allowed", claims.Role, typ, rawFieldList)

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	// Role and correlation ID travel as gRPC metadata, never as message fields (TRD 0.0).
	md := metadata.Pairs("x-client-role", claims.Role)
	if id := r.Header.Get("X-Correlation-ID"); id != "" {
		md.Append("x-correlation-id", id)
	}

	resp, err := h.agg.GetHazards(metadata.NewOutgoingContext(ctx, md), &aggregatorpb.GetHazardsRequest{HazardType: typ})
	if err != nil {
		hazardsFailed(w, err)

		return
	}

	data := make([]map[string]any, 0, len(resp.GetData()))
	for _, e := range resp.GetData() {
		data = append(data, project(eventJSON(e), fields))
	}

	writeJSON(w, http.StatusOK, map[string]any{"data": data, "meta": metaJSON(resp.GetMeta())})
}

// hazardsFailed maps the Aggregator's status to the client (TRD 3.3, 3.4).
func hazardsFailed(w http.ResponseWriter, err error) {
	switch status.Code(err) {
	case codes.Unavailable:
		// The source is down and nothing is stored: an explicit 503, not a 500 or a hang.
		writeJSON(w, http.StatusServiceUnavailable, errBody("no data available: "+status.Convert(err).Message()))
	case codes.ResourceExhausted:
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, errBody("too many requests"))
	case codes.InvalidArgument:
		writeJSON(w, http.StatusBadRequest, errBody(status.Convert(err).Message()))
	case codes.DeadlineExceeded:
		log.Print("hazards: aggregator timed out")
		writeJSON(w, http.StatusGatewayTimeout, errBody("aggregator timed out"))
	default:
		if errors.Is(err, context.DeadlineExceeded) {
			writeJSON(w, http.StatusGatewayTimeout, errBody("aggregator timed out"))

			return
		}

		log.Printf("hazards: aggregator error: %v", err)
		writeJSON(w, http.StatusBadGateway, errBody("aggregator unavailable"))
	}
}

// eventJSON renders a HazardEvent with null for what the source did not give.
func eventJSON(e *aggregatorpb.HazardEvent) map[string]any {
	var attrs map[string]any
	if e.GetAttributes() != nil {
		attrs = e.GetAttributes().AsMap()
	}

	return map[string]any{
		"hazard_id":     e.GetHazardId(),
		"source":        e.GetSource(),
		"source_ref_id": e.GetSourceRefId(),
		"hazard_type":   e.GetHazardType(),
		"severity":      optional(e.Severity),
		"area_name":     optional(e.AreaName),
		"latitude":      optional(e.Latitude),
		"longitude":     optional(e.Longitude),
		"occurred_at":   e.GetOccurredAt().AsTime().UTC().Format(time.RFC3339),
		"ingested_at":   e.GetIngestedAt().AsTime().UTC().Format(time.RFC3339),
		"attributes":    attrs,
	}
}

// optional turns a proto3 optional field into its value, or nil (JSON null).
func optional[T any](p *T) any {
	if p == nil {
		return nil
	}

	return *p
}

func project(event map[string]any, fields []string) map[string]any {
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		out[f] = event[f]
	}

	return out
}

// metaJSON is the source state envelope of TRD 3.3.
func metaJSON(m *aggregatorpb.ResponseMeta) map[string]any {
	ts := func(set bool, t time.Time) any {
		if !set {
			return nil
		}

		return t.UTC().Format(time.RFC3339)
	}

	return map[string]any{
		"source":          m.GetSource(),
		"source_status":   m.GetSourceStatus(),
		"last_success_at": ts(m.GetLastSuccessAt() != nil, m.GetLastSuccessAt().AsTime()),
		"stale_since":     ts(m.GetStaleSince() != nil, m.GetStaleSince().AsTime()),
	}
}
