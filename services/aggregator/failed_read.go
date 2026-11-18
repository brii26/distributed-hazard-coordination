package main

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/brii26/distributed-hazard-coordination/services/aggregator/aggregatorpb"
)

const (
	sourceFieldReport = "FIELD_REPORT"
	roleOperator      = "Operator"

	failedDefaultLimit = 50
	failedMaxLimit     = 100
	failedPageBudget   = 2 << 20 // payload bytes per page, so one gRPC reply stays under the 4 MiB message cap
)

// The sources the unprocessed list can be filtered by: the hazard sources plus field reports.
func knownFailedSource(s string) bool {
	return s == sourceFieldReport || knownSource(s)
}

// Cuts rows after the page budget (always keeping the first one) and moves next to the last kept row.
func trimToBudget(rows []failedMessage, next string) ([]failedMessage, string) {
	total := 0

	for i := range rows {
		total += len(rows[i].Payload)
		if total > failedPageBudget && i > 0 {
			kept := rows[:i]
			last := kept[len(kept)-1]

			return kept, encodeFailedCursor(last.ReceivedAt, last.MessageID)
		}
	}

	return rows, next
}

// Rejected messages with raw payloads for the Operator. The Client-Facing API checks the verified JWT role; the metadata check
// here is defense in depth only (the header is spoofable), the NetworkPolicy aggregator-grpc-from-api is the real barrier.
func (a *readAPI) ListFailedMessages(ctx context.Context, req *aggregatorpb.ListFailedMessagesRequest) (*aggregatorpb.ListFailedMessagesResponse, error) {
	id, role := caller(ctx)
	withID(id).Info("read api request", "method", "ListFailedMessages", "role", role)

	if role != roleOperator {
		return nil, status.Error(codes.PermissionDenied, "failed messages are for the Operator")
	}

	if a.failed == nil {
		return nil, status.Error(codes.Unavailable, "failed messages need the database")
	}

	statuses := []string{failedRetrying, failedParked, failedRecovered}

	switch st := req.GetStatus(); st {
	case "":
	case failedRetrying, failedParked, failedRecovered:
		statuses = []string{st}
	default:
		return nil, status.Error(codes.InvalidArgument, "status must be retrying, parked or recovered")
	}

	limit := int(req.GetLimit())

	switch {
	case limit < 0:
		return nil, status.Error(codes.InvalidArgument, "limit must not be negative")
	case limit == 0:
		limit = failedDefaultLimit
	case limit > failedMaxLimit:
		limit = failedMaxLimit
	}

	if c := req.GetCursor(); c != "" {
		if _, _, err := decodeFailedCursor(c); err != nil {
			return nil, status.Error(codes.InvalidArgument, "cursor tidak valid")
		}
	}

	rows, next, err := a.failed(ctx, failedFilter{Statuses: statuses}, req.GetCursor(), limit)
	if err != nil {
		withID(id).Warn("list failed messages", "error", err)

		return nil, status.Error(codes.Unavailable, "database unavailable")
	}

	rows, next = trimToBudget(rows, next)

	resp := &aggregatorpb.ListFailedMessagesResponse{NextCursor: next}

	for _, m := range rows {
		resp.Data = append(resp.Data, &aggregatorpb.FailedMessage{
			MessageId:     m.MessageID,
			Source:        m.Source,
			ReceivedAt:    timestamppb.New(m.ReceivedAt),
			Reason:        m.Reason,
			Retryable:     m.Retryable,
			Attempts:      int32(m.Attempts), //nolint:gosec // attempts stays tiny
			Status:        m.Status,
			LastErrorAt:   timestamppb.New(m.LastErrorAt),
			SourceRefId:   m.SourceRefID,
			Payload:       m.Payload,
			PayloadSha256: m.PayloadSHA,
			PayloadSize:   int32(m.PayloadSize), //nolint:gosec // capped by the 1 MiB oversize cut
			Oversize:      m.Oversize,
		})
	}

	return resp, nil
}

// The seven fields the auditor sees; no payload on purpose, it can hold personal data.
type unprocessedItem struct {
	MessageID  string    `json:"message_id"`
	Source     string    `json:"source"`
	ReceivedAt time.Time `json:"received_at"`
	Reason     string    `json:"reason"`
	Retryable  bool      `json:"retryable"`
	Attempts   int       `json:"attempts"`
	Status     string    `json:"status"`
}

type unprocessedResponse struct {
	Items      []unprocessedItem `json:"items"`
	NextCursor *string           `json:"next_cursor"`
}

// Reads and checks the unprocessed query. A non-empty msg is the 400 message.
func parseUnprocessedQuery(q url.Values) (source, cursor string, limit int, msg string) {
	source, cursor, limit = q.Get("source"), q.Get("cursor"), auditDefaultLimit

	if source != "" && !knownFailedSource(source) {
		return "", "", 0, "source tidak dikenal"
	}

	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > auditMaxLimit {
			return "", "", 0, "limit harus 1 sampai 1000"
		}

		limit = n
	}

	if _, _, err := decodeFailedCursor(cursor); cursor != "" && err != nil {
		return "", "", 0, "cursor tidak valid"
	}

	return source, cursor, limit, ""
}

// GET /internal/audit/unprocessed: retrying and parked rows, oldest first, by source and opaque cursor. Recovered rows never show.
func (a *readAPI) auditUnprocessed(w http.ResponseWriter, r *http.Request) {
	if !a.auditToken(w, r) {
		return
	}

	source, cursor, limit, msg := parseUnprocessedQuery(r.URL.Query())
	if msg != "" {
		writeAuditError(w, http.StatusBadRequest, "bad_request", msg)

		return
	}

	if a.failed == nil {
		writeAuditError(w, http.StatusServiceUnavailable, "unavailable", "audit needs the database")

		return
	}

	rows, next, err := a.failed(r.Context(), failedFilter{Statuses: []string{failedRetrying, failedParked}, Source: source}, cursor, limit)
	if err != nil {
		writeAuditError(w, http.StatusServiceUnavailable, "unavailable", "database unavailable")

		return
	}

	resp := unprocessedResponse{Items: []unprocessedItem{}}

	for _, m := range rows {
		resp.Items = append(resp.Items, unprocessedItem{
			MessageID: m.MessageID, Source: m.Source, ReceivedAt: m.ReceivedAt.UTC(), Reason: m.Reason,
			Retryable: m.Retryable, Attempts: m.Attempts, Status: m.Status,
		})
	}

	if next != "" {
		resp.NextCursor = &next
	}

	writeJSON(w, http.StatusOK, resp)
}
