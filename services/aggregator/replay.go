package main

import (
	"cmp"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	replayDefaultLimit = 5000
	replayMaxLimit     = 50000
	replayMaxErrors    = 20
	replayDefaultBatch = 500
)

// What the replayer needs from the failed_messages table.
type replayQueue interface {
	lockReplay(ctx context.Context, source string) (release func(), ok bool, err error)
	parkedPage(ctx context.Context, source string, afterAt time.Time, afterID string, limit int) ([]failedMessage, error)
	countParkedOversize(ctx context.Context, source string) (int, error)
	markReplayed(ctx context.Context, id string, superseded bool) (bool, error)
	recordReplayFailure(ctx context.Context, id, reason string) error
}

type replayError struct {
	MessageID string `json:"message_id"`
	Reason    string `json:"reason"`
}

// What one replay run did.
type replayReport struct {
	Source          string        `json:"source"`
	Scanned         int           `json:"scanned"`
	Recovered       int           `json:"recovered"`
	Superseded      int           `json:"superseded"`
	StillParked     int           `json:"still_parked"`
	SkippedOversize int           `json:"skipped_oversize"`
	Errors          []replayError `json:"errors"`
}

var errReplayBusy = errors.New("a replay for this source is already running")

// Maps parked rows again with the current mapper and writes them in source order. The guarded upsert, not the page boundary, keeps an old version from overwriting a newer one.
type replayer struct {
	q       replayQueue
	m       *mapper
	upsert  func(context.Context, HazardEvent) (bool, error)
	publish func(ctx context.Context, h HazardEvent, correlationID string)
	batch   int

	recovered, superseded, stillParked atomic.Int64
}

// Decodes the stored record and maps it with the current mapper into exactly one event. The same guards as at ingest run
// first, so a record set aside as too deep, not UTF-8 or too large keeps that reason instead of a schema error.
func (m *mapper) mapParked(row failedMessage) (HazardEvent, error) {
	if row.Oversize {
		return HazardEvent{}, errOversize
	}

	if err := checkRecord(row.Payload); err != nil {
		return HazardEvent{}, err
	}

	var rec map[string]any

	if err := json.Unmarshal(row.Payload, &rec); err != nil {
		return HazardEvent{}, err
	}

	var (
		out []HazardEvent
		err error
	)

	switch row.Source {
	case sourceBMKG:
		out, err = m.mapSeismic([]map[string]any{rec}, nil)
	case sourcePVMBG:
		out, err = m.mapVolcanic([]map[string]any{rec})
	default:
		err = fmt.Errorf("replay does not handle source %s", row.Source)
	}

	if err != nil {
		return HazardEvent{}, err
	}

	if len(out) != 1 {
		return HazardEvent{}, fmt.Errorf("payload mapped to %d events, want 1", len(out))
	}

	return out[0], nil
}

type replayItem struct {
	row failedMessage
	ev  HazardEvent
}

// Replays up to limit parked rows of source. On a store outage it stops and returns what was done so far with the error.
func (r *replayer) run(ctx context.Context, source string, limit int) (rep replayReport, err error) {
	rep = replayReport{Source: source, Errors: []replayError{}}

	release, ok, err := r.q.lockReplay(ctx, source)
	if err != nil {
		return rep, err
	}

	if !ok {
		return rep, errReplayBusy
	}

	defer release()

	if rep.SkippedOversize, err = r.q.countParkedOversize(ctx, source); err != nil {
		return rep, err
	}

	ctx = withCorrelation(ctx, "replay-"+strings.ToLower(source)+"-"+strconv.FormatInt(time.Now().Unix(), 10))

	var (
		afterAt time.Time
		afterID string
	)

	for rep.Scanned < limit {
		rows, err := r.q.parkedPage(ctx, source, afterAt, afterID, min(r.batch, limit-rep.Scanned))
		if err != nil {
			return rep, err
		}

		if len(rows) == 0 {
			return rep, nil
		}

		last := rows[len(rows)-1]
		afterAt, afterID = last.ReceivedAt, last.MessageID
		rep.Scanned += len(rows)

		if err = r.page(ctx, rows, &rep); err != nil {
			return rep, err
		}
	}

	return rep, nil
}

// Handles one page: rows that cannot be mapped stay parked, the rest are written per id in source order.
func (r *replayer) page(ctx context.Context, rows []failedMessage, rep *replayReport) error {
	items := make([]replayItem, 0, len(rows))

	for _, row := range rows {
		ev, err := r.m.mapParked(row)
		if err != nil {
			if err = r.keep(ctx, row, err, rep); err != nil {
				return err
			}

			continue
		}

		items = append(items, replayItem{row, ev})
	}

	slices.SortFunc(items, func(a, b replayItem) int {
		return cmp.Or(
			cmp.Compare(a.ev.SourceRefID, b.ev.SourceRefID),
			a.ev.OccurredAt.Compare(b.ev.OccurredAt),
			a.row.ReceivedAt.Compare(b.row.ReceivedAt),
			cmp.Compare(a.row.MessageID, b.row.MessageID),
		)
	})

	for _, it := range items {
		if err := r.write(ctx, it, rep); err != nil {
			return err
		}
	}

	return nil
}

func (r *replayer) write(ctx context.Context, it replayItem, rep *replayReport) error {
	changed, err := r.upsert(withReplayAt(ctx, it.row.ReceivedAt), it.ev)

	switch {
	case err == nil || errors.Is(err, errSuperseded):
		sup := err != nil

		if _, merr := r.q.markReplayed(ctx, it.row.MessageID, sup); merr != nil {
			return merr
		}

		if sup {
			rep.Superseded++

			r.superseded.Add(1)
		} else {
			rep.Recovered++

			r.recovered.Add(1)
		}

		if changed {
			r.publish(ctx, it.ev, correlationOf(ctx))
		}

		return nil
	default:
		if retryable, _ := classifyFailure(err); retryable {
			return err
		}

		return r.keep(ctx, it.row, err, rep)
	}
}

// A row that still fails stays parked with a new reason.
func (r *replayer) keep(ctx context.Context, row failedMessage, cause error, rep *replayReport) error {
	_, code := classifyFailure(cause)

	reason := cause.Error()
	if !strings.HasPrefix(reason, code+":") {
		reason = code + ": " + reason
	}

	if err := r.q.recordReplayFailure(ctx, row.MessageID, reason); err != nil {
		return err
	}

	rep.StillParked++

	r.stillParked.Add(1)

	if len(rep.Errors) < replayMaxErrors {
		rep.Errors = append(rep.Errors, replayError{row.MessageID, truncateReason(reason)})
	}

	withID(correlationOf(ctx)).Warn("replay still failing", "message_id", row.MessageID, "error", truncateReason(cause.Error()))

	return nil
}

const replayReasonMax = 200

// Cuts a failure reason to replayReasonMax characters for the response and the log, because a decode error can echo a fragment of the raw payload.
func truncateReason(s string) string {
	r := []rune(s)
	if len(r) <= replayReasonMax {
		return s
	}

	return string(r[:replayReasonMax]) + "..."
}

func (r *replayer) writeMetrics(out io.Writer) {
	_, _ = fmt.Fprintf(out, "# TYPE aggregator_replay_total counter\n"+
		"aggregator_replay_total{result=\"recovered\"} %d\n"+
		"aggregator_replay_total{result=\"superseded\"} %d\n"+
		"aggregator_replay_total{result=\"still_parked\"} %d\n",
		r.recovered.Load(), r.superseded.Load(), r.stillParked.Load())
}

// Bearer gate on ADMIN_TOKEN, apart from the read-only audit token. No token configured means every call is refused.
func (a *readAPI) adminGate(w http.ResponseWriter, r *http.Request) bool {
	if a.adminTok == "" || a.replay == nil {
		writeAuditError(w, http.StatusServiceUnavailable, "unavailable", "admin not configured")

		return false
	}

	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(a.adminTok)) != 1 {
		writeAuditError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")

		return false
	}

	return true
}

// POST /internal/admin/replay?source=BMKG|PVMBG[&limit=N].
func (a *readAPI) adminReplay(w http.ResponseWriter, r *http.Request) {
	if !a.adminGate(w, r) {
		return
	}

	source := r.URL.Query().Get("source")
	if source != sourceBMKG && source != sourcePVMBG {
		writeAuditError(w, http.StatusBadRequest, "bad_request", "source must be BMKG or PVMBG")

		return
	}

	limit := replayDefaultLimit

	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > replayMaxLimit {
			writeAuditError(w, http.StatusBadRequest, "bad_request", "limit must be 1 to "+strconv.Itoa(replayMaxLimit))

			return
		}

		limit = n
	}

	// A long run outlives the server's default write timeout.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(10 * time.Minute))

	rep, err := a.replay.run(r.Context(), source, limit)

	switch {
	case errors.Is(err, errReplayBusy):
		writeAuditError(w, http.StatusConflict, "conflict", err.Error())
	case err != nil:
		withID("").Warn("replay stopped", "source", source, "error", err.Error())
		writeJSON(w, http.StatusServiceUnavailable, rep)
	default:
		writeJSON(w, http.StatusOK, rep)
	}
}
