package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/aggregatorpb"
)

const (
	failedDefaultLimit = 50
	failedMaxLimit     = 100

	keySource = "source" // JSON key shared with the hazard rows
	keyData   = "data"
)

// Lets only the Operator through. The role comes from the verified JWT claims, so it sits behind the JWT middleware, which already answered 401 for a missing or bad token.
func requireOperator(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if claims, ok := claimsFrom(r.Context()); !ok || claims.Role != roleOperator {
			writeJSON(w, http.StatusForbidden, errBody("forbidden"))

			return
		}

		next.ServeHTTP(w, r)
	})
}

// GET /admin/failed-messages: the Aggregator's failed rows with the raw payload as base64, oldest first, optionally of one source. Operator only.
type failedMessagesAPI struct {
	agg     aggregatorpb.AggregatorClient
	timeout time.Duration // UPSTREAM_TIMEOUT_MS
}

func (h failedMessagesAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := int32(failedDefaultLimit)

	if s := q.Get("limit"); s != "" {
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil || n < 1 || n > failedMaxLimit {
			writeJSON(w, http.StatusBadRequest, errBody(fmt.Sprintf("limit must be a whole number from 1 to %d", failedMaxLimit)))

			return
		}

		limit = int32(n)
	}

	st := q.Get("status")
	if st != "" && st != "retrying" && st != "parked" && st != "recovered" {
		writeJSON(w, http.StatusBadRequest, errBody("status must be retrying, parked or recovered"))

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "x-client-role", roleOperator)

	resp, err := h.agg.ListFailedMessages(ctx, &aggregatorpb.ListFailedMessagesRequest{Status: st, Limit: limit, Cursor: q.Get("cursor"), Source: q.Get("source")})
	if err != nil {
		failedMessagesError(r.Context(), w, err)

		return
	}

	data := make([]map[string]any, 0, len(resp.GetData()))
	for _, m := range resp.GetData() {
		data = append(data, failedJSON(m))
	}

	var next any
	if c := resp.GetNextCursor(); c != "" {
		next = c
	}

	logFor(r.Context()).Info("failed messages request", "rows", len(data))

	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{keyData: data, "next_cursor": next})
}

// Maps the Aggregator's status to the client: a bad value is the caller's, anything else is the upstream's.
func failedMessagesError(ctx context.Context, w http.ResponseWriter, err error) {
	switch {
	case status.Code(err) == codes.InvalidArgument:
		writeJSON(w, http.StatusBadRequest, errBody(status.Convert(err).Message()))
	case status.Code(err) == codes.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded):
		writeJSON(w, http.StatusGatewayTimeout, errBody("aggregator timed out"))
	default:
		logFor(ctx).Warn("failed messages aggregator error", "error", err)
		writeJSON(w, http.StatusBadGateway, errBody("aggregator unavailable"))
	}
}

// One row as JSON. The payload is base64 because it can be invalid UTF-8 or hold NUL bytes.
func failedJSON(m *aggregatorpb.FailedMessage) map[string]any {
	out := map[string]any{
		"message_id":       m.GetMessageId(),
		keySource:          m.GetSource(),
		"received_at":      m.GetReceivedAt().AsTime().UTC().Format(time.RFC3339Nano),
		"reason":           m.GetReason(),
		"retryable":        m.GetRetryable(),
		"attempts":         m.GetAttempts(),
		"status":           m.GetStatus(),
		"last_error_at":    m.GetLastErrorAt().AsTime().UTC().Format(time.RFC3339Nano),
		"payload":          base64.StdEncoding.EncodeToString(m.GetPayload()),
		"payload_encoding": "base64",
		"payload_sha256":   hex.EncodeToString(m.GetPayloadSha256()),
		"payload_size":     m.GetPayloadSize(),
		"oversize":         m.GetOversize(),
	}

	if ref := m.GetSourceRefId(); ref != "" {
		out["source_ref_id"] = ref
	}

	return out
}
