package main

import (
	"context"
	"database/sql"
	"time"
)

// A field_reports row as stored, copied as is so the second copy equals the first (stored_at included).
type reportRow struct {
	ReportID      string
	Shard         int
	Description   string
	Latitude      sql.NullFloat64
	Longitude     sql.NullFloat64
	Kind          sql.NullString
	ReporterID    string
	ReporterRole  string
	ReceivedAt    sql.NullTime
	StoredAt      time.Time
	CorrelationID sql.NullString
}

// One page of the reports of a shard on one node, after the given report_id.
func listReportsOn(ctx context.Context, db *sql.DB, shard int, after string, limit int) ([]reportRow, error) {
	rows, err := db.QueryContext(ctx, `
SELECT report_id, shard, description, latitude, longitude, kind, reporter_id, reporter_role, received_at, stored_at, correlation_id
FROM field_reports WHERE shard = $1::smallint AND report_id > $2 ORDER BY report_id LIMIT $3`, shard, after, limit)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var out []reportRow

	for rows.Next() {
		var r reportRow

		if err := rows.Scan(&r.ReportID, &r.Shard, &r.Description, &r.Latitude, &r.Longitude, &r.Kind, &r.ReporterID,
			&r.ReporterRole, &r.ReceivedAt, &r.StoredAt, &r.CorrelationID); err != nil {
			return nil, err
		}

		out = append(out, r)
	}

	return out, rows.Err()
}

// Writes the reports to one node. A report that is already there is left alone, so a repeated copy changes nothing.
func copyReportsOn(ctx context.Context, db *sql.DB, rows []reportRow) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() { _ = tx.Rollback() }()

	for _, r := range rows {
		_, err := tx.ExecContext(ctx, `
INSERT INTO field_reports (report_id, shard, description, latitude, longitude, kind, reporter_id, reporter_role, received_at, stored_at, correlation_id)
VALUES ($1, $2::smallint, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (report_id) DO NOTHING`,
			r.ReportID, r.Shard, r.Description, r.Latitude, r.Longitude, r.Kind, r.ReporterID, r.ReporterRole, r.ReceivedAt, r.StoredAt, r.CorrelationID)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// False on a node that never applied migration 013: the rebalancer then works on hazard_events only, as before.
func hasFieldReports(ctx context.Context, db *sql.DB) bool {
	var ok bool

	return db.QueryRowContext(ctx, `SELECT to_regclass('field_reports') IS NOT NULL`).Scan(&ok) == nil && ok
}

// Copies the reports of one shard from src to dst, page by page. Without the two hooks (tests with fakes) it does nothing.
func (r *rebalancer) copyReports(ctx context.Context, shard int, src, dst *node) error {
	if r.listReports == nil || r.copyReportsTo == nil {
		return nil
	}

	after := ""

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		cctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
		rows, err := r.listReports(cctx, src, shard, after, r.pageSize)

		cancel()

		if err != nil {
			return err
		}

		if len(rows) > 0 {
			wctx, wcancel := context.WithTimeout(ctx, dbQueryTimeout)
			err := r.copyReportsTo(wctx, dst, rows)

			wcancel()

			if err != nil {
				return err
			}

			r.repairRows.Add(int64(len(rows)))
			after = rows[len(rows)-1].ReportID
		}

		if len(rows) < r.pageSize {
			return nil
		}

		if !sleepCtx(ctx, r.pause) {
			return ctx.Err()
		}
	}
}
