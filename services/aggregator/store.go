package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver with database/sql
)

// Timeout for every call to the Canonical Store.
const dbQueryTimeout = 2 * time.Second

// Upserts by (source, source_ref_id) and returns a row only when something is new or changed, flagging which. hazard_id and ingested_at stay out of the UPDATE so the id is stable and a repeat never looks changed.
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
RETURNING hazard_id, (xmax = 0) AS inserted`

// The only door to the Canonical Store. *sql.DB is a pool, safe for concurrent use.
type store struct {
	db *sql.DB
}

// Connects with a pool of at most poolSize and pings, so a wrong URL fails at startup.
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

// Writes h in one transaction. True if new or changed, false for an identical repeat.
func (s *store) upsert(ctx context.Context, h HazardEvent) (_ bool, err error) {
	start := time.Now()
	ctx = startCall(ctx, "canonical-store upsert")

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

	defer func() { _ = tx.Rollback() }()

	var (
		id       string
		inserted bool
	)

	err = tx.QueryRowContext(ctx, upsertSQL,
		h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity, h.AreaName,
		h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, string(raw),
	).Scan(&id, &inserted)

	changed := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}

	if err = tx.Commit(); err != nil {
		return false, err
	}

	if changed {
		result := "updated"
		if inserted {
			result = "new"
		}

		withID(correlationOf(ctx)).Info("upsert", "hazard_id", id, "source_ref_id", h.SourceRefID, "result", result)
	}

	return changed, nil
}

// One page of one hazard type. Search is a case-insensitive substring of area_name.
type listQuery struct {
	HazardType string
	Severity   string
	Search     string
	Limit      int
	Offset     int
}

// Makes search text literal inside LIKE.
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// One page of a hazard type, newest first, plus whether another page follows (it asks for one extra row).
// ponytail: OFFSET still walks the skipped rows and search scans the type. At millions of rows move to keyset paging (occurred_at, hazard_id) and a pg_trgm index on area_name.
func (s *store) listByType(ctx context.Context, q listQuery) (_ []HazardEvent, _ bool, err error) {
	start := time.Now()
	ctx = startCall(ctx, "canonical-store list "+q.HazardType)

	defer func() { logCall(ctx, "canonical-store list "+q.HazardType, start, err) }()

	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name,
       latitude, longitude, occurred_at, ingested_at, attributes
FROM hazard_events
WHERE hazard_type = $1
  AND ($2::text = '' OR severity = $2::text)
  AND ($3::text = '' OR area_name ILIKE '%' || $3::text || '%' ESCAPE '\')
ORDER BY occurred_at DESC, hazard_id
LIMIT $4 OFFSET $5`, q.HazardType, q.Severity, likeEscaper.Replace(q.Search), q.Limit+1, q.Offset)
	if err != nil {
		return nil, false, err
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
			return nil, false, err
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
				return nil, false, err
			}
		}

		out = append(out, h)
	}

	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	more := len(out) > q.Limit
	if more {
		out = out[:q.Limit]
	}

	return out, more, nil
}

// Whether the Canonical Store answers, for the health check.
func (s *store) ping(ctx context.Context) (err error) {
	start := time.Now()
	defer func() { logCall(ctx, "canonical-store ping", start, err) }()

	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	return s.db.PingContext(ctx)
}
