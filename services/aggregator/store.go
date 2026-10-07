package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // daftarin driver pgx ke database/sql
)

// batas waktu tiap call ke Canonical Store.
const dbQueryTimeout = 2 * time.Second

// upsert per (source, source_ref_id), hanya mengembalikan row kalau baru atau berubah. hazard_id dan ingested_at sengaja nggak di-UPDATE biar id stabil dan repeat nggak kelihatan berubah.
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

// satu-satunya pintu ke Canonical Store, pool *sql.DB sudah aman dipakai concurrent.
type store struct {
	db *sql.DB
}

// connect dengan pool maksimal poolSize dan ping, jadi URL salah gagal saat start.
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

// tulis h dalam satu transaksi, true kalau baru atau berubah, false kalau repeat identik.
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

// satu halaman satu tipe hazard, Search itu substring case-insensitive dari area_name.
type listQuery struct {
	HazardType string
	Severity   string
	Search     string
	Limit      int
	Offset     int
}

// bikin teks search jadi literal di LIKE.
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// satu halaman tipe hazard dari yang terbaru, plus apakah ada halaman berikutnya (minta satu row ekstra).
// ponytail: OFFSET masih nelusuri row yang di-skip dan search itu scan per tipe, di jutaan row pindah ke keyset paging (occurred_at, hazard_id) dan index pg_trgm di area_name.
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

// cek Canonical Store jawab, buat health check.
func (s *store) ping(ctx context.Context) (err error) {
	start := time.Now()
	defer func() { logCall(ctx, "canonical-store ping", start, err) }()

	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	return s.db.PingContext(ctx)
}
