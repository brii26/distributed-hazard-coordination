package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

const (
	backfillMaxBody    = 8 << 20
	backfillMaxRecords = 1000
	backfillWriteLimit = 2 * time.Minute
)

// Writes a batch the poller never sees, such as a historical catalog: same mapping, parking and dual write, but no cursor, no lease and no publish, so old events raise no alerts.
type backfill struct {
	// No schema adjust like the replay mapper has: the catalog uses our own BMKG field names.
	m      *mapper
	upsert func(context.Context, HazardEvent) (bool, error)
	fail   func(context.Context, failedMessage) error
	retry  retryConfig
}

func (b *backfill) run(ctx context.Context, recs []map[string]any) {
	ing := &ingest{
		name:      sourceBMKG,
		mapBatch:  func(r pollResult) ([]HazardEvent, error) { return b.m.mapSeismic(r.records, r.warnings) },
		upsert:    b.upsert,
		publish:   func(context.Context, HazardEvent, string) {},
		retryBase: retryBase,
		retryMax:  retryMax,
		fail:      b.fail,
		retry:     b.retry,
	}

	ing.handle(ctx, pollResult{records: recs, correlationID: newHazardID()})
}

// POST /internal/admin/ingest: a JSON array of up to 1000 BMKG earthquake records.
func (a *readAPI) adminIngest(w http.ResponseWriter, r *http.Request) {
	if !a.adminGate(w, r) {
		return
	}

	if a.backfill == nil {
		writeAuditError(w, http.StatusServiceUnavailable, "unavailable", "backfill is not set up")

		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, backfillMaxBody)

	var recs []map[string]any

	if err := json.NewDecoder(r.Body).Decode(&recs); err != nil {
		if _, tooBig := errors.AsType[*http.MaxBytesError](err); tooBig {
			writeAuditError(w, http.StatusRequestEntityTooLarge, "too_large", "body is over 8 MiB")

			return
		}

		writeAuditError(w, http.StatusBadRequest, "bad_request", "body must be a JSON array of objects")

		return
	}

	if len(recs) == 0 || len(recs) > backfillMaxRecords {
		writeAuditError(w, http.StatusBadRequest, "bad_request", "send 1 to 1000 records")

		return
	}

	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(backfillWriteLimit))

	// A dropped connection must not stop the batch halfway, so the work gets its own deadline.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), backfillWriteLimit)
	defer cancel()

	a.backfill.run(ctx, recs)
	writeJSON(w, http.StatusOK, map[string]int{"received": len(recs)})
}
