package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/brii26/distributed-hazard-coordination/services/aggregator/aggregatorpb"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxDescription = 4096

	resultStored    = "STORED"
	resultDuplicate = "DUPLICATE"
	resultParked    = "PARKED"
)

// A field report as the Client-Facing API queues it.
type fieldReport struct {
	ReportID     string   `json:"report_id"`
	Description  string   `json:"description"`
	Latitude     *float64 `json:"latitude"`
	Longitude    *float64 `json:"longitude"`
	Kind         string   `json:"kind"`
	ReporterID   string   `json:"reporter_id"`
	ReporterRole string   `json:"reporter_role"`
	ReceivedAt   string   `json:"received_at"`
}

// Checks a queue body against its message id. The errors are fixed phrases and never carry body text, because the body holds an e-mail address.
func parseFieldReport(messageID string, body []byte) (fieldReport, error) {
	var r fieldReport

	if !utf8.Valid(body) {
		return r, errors.New("body is not valid UTF-8")
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&r); err != nil {
		return r, errors.New("body is not a JSON object of the expected types")
	}

	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return r, errors.New("trailing data after the JSON object")
	}

	switch {
	case hasNUL(r):
		return r, errors.New("text contains a NUL byte")
	case r.ReportID == "" || r.ReportID != messageID:
		return r, errors.New("report_id missing or different from the message id")
	case strings.TrimSpace(r.Description) == "" || len(r.Description) > maxDescription:
		return r, errors.New("description empty or too long")
	}

	return r, checkRest(r)
}

// True when any text field holds a NUL. JSON allows it and it is valid UTF-8, but Postgres refuses it in text columns (SQLSTATE 22021), so it can never be saved.
func hasNUL(r fieldReport) bool {
	return slices.ContainsFunc([]string{r.ReportID, r.Description, r.Kind, r.ReporterID, r.ReporterRole, r.ReceivedAt},
		func(s string) bool { return strings.ContainsRune(s, 0) })
}

// True when Postgres refused the data itself (SQLSTATE class 22 data exception or 23 integrity violation). Trying again cannot help, unlike a timeout or a lost connection.
func rejectedByStore(err error) bool {
	var pe *pgconn.PgError

	return errors.As(err, &pe) && (strings.HasPrefix(pe.Code, "22") || strings.HasPrefix(pe.Code, "23"))
}

// The checks after id and description: reporter, coordinates and time.
func checkRest(r fieldReport) error {
	switch {
	case (r.Latitude == nil) != (r.Longitude == nil):
		return errors.New("latitude and longitude must come together")
	case r.ReporterID == "" || r.ReporterRole == "":
		return errors.New("reporter missing")
	case r.Latitude != nil && (!withinAbs(*r.Latitude, 90) || !withinAbs(*r.Longitude, 180)):
		return errors.New("coordinates out of range")
	}

	if _, err := time.Parse(time.RFC3339, r.ReceivedAt); r.ReceivedAt != "" && err != nil {
		return errors.New("received_at is not RFC 3339")
	}

	return nil
}

func withinAbs(v, limit float64) bool { return !math.IsNaN(v) && math.Abs(v) <= limit }

const insertFieldReportSQL = `
INSERT INTO field_reports (report_id, shard, description, latitude, longitude, kind, reporter_id, reporter_role, received_at, correlation_id)
VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8, NULLIF($9, '')::timestamptz, NULLIF($10, ''))
ON CONFLICT (report_id) DO NOTHING
RETURNING report_id`

// Inserts r once on one store. False means the id was already there and nothing was written.
func insertFieldReportOn(ctx context.Context, db *sql.DB, r fieldReport, correlationID string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	var id string

	err := db.QueryRowContext(ctx, insertFieldReportSQL, r.ReportID, shardOf(r.ReportID), r.Description, r.Latitude, r.Longitude,
		r.Kind, r.ReporterID, r.ReporterRole, r.ReceivedAt, correlationID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	return err == nil, err
}

// Where field reports go: save writes one valid report, park keeps a rejected body.
type fieldReports struct {
	save func(ctx context.Context, r fieldReport, correlationID string) (bool, error)
	park func(ctx context.Context, m failedMessage) error

	stored, duplicate, parked atomic.Int64
}

// Single-store mode writes to db, cluster mode to the two owners of the shard.
func newFieldReports(db *store, cl *cluster) *fieldReports {
	f := &fieldReports{park: db.insertFailed}

	f.save = func(ctx context.Context, r fieldReport, corr string) (bool, error) {
		return insertFieldReportOn(ctx, db.db, r, corr)
	}

	if cl != nil {
		f.save = func(ctx context.Context, r fieldReport, corr string) (bool, error) {
			return cl.fanout(ctx, shardOf(r.ReportID), func(ctx context.Context, n *node) (bool, error) {
				return insertFieldReportOn(ctx, n.db, r, corr)
			})
		}
	}

	return f
}

func (f *fieldReports) writeMetrics(w io.Writer) {
	_, _ = fmt.Fprintf(w, "# TYPE aggregator_field_reports_total counter\n")
	_, _ = fmt.Fprintf(w, "aggregator_field_reports_total{result=\"new\"} %d\n", f.stored.Load())
	_, _ = fmt.Fprintf(w, "aggregator_field_reports_total{result=\"duplicate\"} %d\n", f.duplicate.Load())
	_, _ = fmt.Fprintf(w, "aggregator_field_reports_total{result=\"parked\"} %d\n", f.parked.Load())
}

// Saves a field report once. Any non-error reply means the report is durable, so the consumer may ack. A body that can never be saved is parked instead.
func (a *readAPI) SaveFieldReport(ctx context.Context, req *aggregatorpb.SaveFieldReportRequest) (*aggregatorpb.SaveFieldReportResponse, error) {
	corr, _ := caller(ctx)
	log := withID(corr)

	if a.fields == nil {
		return nil, status.Error(codes.Unavailable, "field reports are not enabled")
	}

	reason := req.GetParkReason()

	r, err := parseFieldReport(req.GetMessageId(), req.GetBody())
	if reason == "" && err != nil {
		reason = "malformed: " + err.Error()
	}

	if reason != "" {
		m := newFailedMessage(sourceFieldReport, req.GetBody(), reason, false, time.Now())
		m.CorrelationID = corr

		if err := a.fields.park(ctx, m); err != nil {
			log.Warn("field report park failed", "message_id", req.GetMessageId())

			return nil, status.Error(codes.Unavailable, "could not park")
		}

		a.fields.parked.Add(1)
		log.Warn("field report parked", "message_id", req.GetMessageId(), "reason", reason)

		return &aggregatorpb.SaveFieldReportResponse{Result: resultParked}, nil
	}

	changed, err := a.fields.save(ctx, r, corr)
	if err != nil {
		log.Warn("field report save failed", "report_id", r.ReportID, "error", err.Error())

		// Not Unavailable: the consumer would retry it for ever. InvalidArgument counts toward its park limit.
		if rejectedByStore(err) {
			return nil, status.Error(codes.InvalidArgument, "the store rejected this report")
		}

		return nil, status.Error(codes.Unavailable, "no store copy committed")
	}

	if !changed {
		a.fields.duplicate.Add(1)
		log.Info("field report duplicate", "report_id", r.ReportID)

		return &aggregatorpb.SaveFieldReportResponse{Result: resultDuplicate}, nil
	}

	a.fields.stored.Add(1)
	log.Info("field report stored", "report_id", r.ReportID)

	return &aggregatorpb.SaveFieldReportResponse{Result: resultStored}, nil
}
