package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// dbQueryTimeout bounds every call to the Canonical Store, so a slow database
// can hold an ingest goroutine for this long at most (doc/TRD_M1.md 3.4).
const dbQueryTimeout = 2 * time.Second

// upsertSQL writes one HazardEvent keyed by (source, source_ref_id) and returns a
// row only when something was written: a new record, or an existing one whose
// content changed. An identical repeat matches the conflict, fails the WHERE and
// returns nothing, so the caller can tell "new or changed" from "seen before".
//
// hazard_id and ingested_at are left out of the UPDATE on purpose: the id handed
// out when the record first arrived stays stable, and ingested_at stays the time
// it first arrived (a fresh one would also make every repeat look changed).
const upsertSQL = `
INSERT INTO hazard_events
    (hazard_id, source, source_ref_id, hazard_type, severity, area_name,
     latitude, longitude, occurred_at, ingested_at, attributes)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
ON CONFLICT (source, source_ref_id) DO UPDATE SET
    hazard_type = EXCLUDED.hazard_type,
    severity    = EXCLUDED.severity,
    area_name   = EXCLUDED.area_name,
    latitude    = EXCLUDED.latitude,
    longitude   = EXCLUDED.longitude,
    occurred_at = EXCLUDED.occurred_at,
    attributes  = EXCLUDED.attributes
WHERE (hazard_events.hazard_type, hazard_events.severity, hazard_events.area_name,
       hazard_events.latitude, hazard_events.longitude, hazard_events.occurred_at,
       hazard_events.attributes)
      IS DISTINCT FROM
      (EXCLUDED.hazard_type, EXCLUDED.severity, EXCLUDED.area_name,
       EXCLUDED.latitude, EXCLUDED.longitude, EXCLUDED.occurred_at,
       EXCLUDED.attributes)
RETURNING hazard_id`

// store is the Aggregator's only door to the Canonical Store (doc/TRD_M1.md 3.5, 6).
// *sql.DB is already a pool that is safe for concurrent use, so the goroutines
// using the store need no lock of their own.
type store struct {
	db *sql.DB
}

// openStore connects with a pool capped at poolSize (DB_POOL_SIZE) and checks
// the database answers, so a wrong URL fails at startup rather than on first use.
func openStore(url string, poolSize int) (*store, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(poolSize)

	ctx, cancel := context.WithTimeout(context.Background(), dbQueryTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()

		return nil, err
	}

	return &store{db: db}, nil
}

func (s *store) close() error {
	return s.db.Close()
}

// upsert writes h in one transaction and reports whether it was new or changed
// (true) or an identical repeat that wrote nothing (false). Two goroutines may
// upsert the same record at once: ON CONFLICT is atomic in Postgres, and BMKG and
// PVMBG never share a (source, source_ref_id).
func (s *store) upsert(ctx context.Context, h HazardEvent) (_ bool, err error) {
	start := time.Now()
	defer func() { logCall(ctx, "canonical-store upsert", start, err) }()

	attrs := h.Attributes
	if attrs == nil {
		attrs = map[string]any{}
	}

	raw, err := json.Marshal(attrs)
	if err != nil {
		return false, err
	}

	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}

	defer func() { _ = tx.Rollback() }() // a no-op once committed

	var id string

	err = tx.QueryRowContext(ctx, upsertSQL,
		h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity, h.AreaName,
		h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, string(raw),
	).Scan(&id)

	changed := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}

	return changed, tx.Commit()
}

// listByType returns every stored event of one hazard type, newest first.
// ponytail: no pagination, the whole type is returned in one response; fine for
// the mock-sized data set. Add a limit or paging before real volumes.
func (s *store) listByType(ctx context.Context, hazardType string) (_ []HazardEvent, err error) {
	start := time.Now()
	defer func() { logCall(ctx, "canonical-store list "+hazardType, start, err) }()

	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name,
       latitude, longitude, occurred_at, ingested_at, attributes
FROM hazard_events WHERE hazard_type = $1 ORDER BY occurred_at DESC`, hazardType)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var out []HazardEvent

	for rows.Next() {
		var (
			h        HazardEvent
			severity sql.NullString
			area     sql.NullString
			lat, lon sql.NullFloat64
			attrs    []byte
		)

		if err := rows.Scan(&h.HazardID, &h.Source, &h.SourceRefID, &h.HazardType, &severity, &area,
			&lat, &lon, &h.OccurredAt, &h.IngestedAt, &attrs); err != nil {
			return nil, err
		}

		if severity.Valid {
			h.Severity = &severity.String
		}

		if area.Valid {
			h.AreaName = &area.String
		}

		if lat.Valid {
			h.Latitude = &lat.Float64
		}

		if lon.Valid {
			h.Longitude = &lon.Float64
		}

		if len(attrs) > 0 {
			if err := json.Unmarshal(attrs, &h.Attributes); err != nil {
				return nil, err
			}
		}

		out = append(out, h)
	}

	return out, rows.Err()
}

// ping reports whether the Canonical Store answers, for the health check.
func (s *store) ping(ctx context.Context) (err error) {
	start := time.Now()
	defer func() { logCall(ctx, "canonical-store ping", start, err) }()

	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	return s.db.PingContext(ctx)
}
