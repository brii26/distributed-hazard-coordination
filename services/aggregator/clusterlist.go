package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Where a page stopped: the last record handed out. The next page is everything strictly older in (ingested_at, hazard_id) order. The position is global, so every shard resumes from the same key and no record repeats or is skipped.
type listPos struct {
	T  time.Time `json:"t"`
	ID string    `json:"id"`
}

func (p listPos) encode() string {
	b, _ := json.Marshal(p)

	return base64.RawURLEncoding.EncodeToString(b)
}

// Empty cursor is the first page (nil). Anything undecodable is an error.
func decodeCursor(s string) (*listPos, error) {
	if s == "" {
		return nil, nil //nolint:nilnil // no cursor is the first page
	}

	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.New("invalid cursor")
	}

	var p listPos
	if err := json.Unmarshal(b, &p); err != nil || p.ID == "" {
		return nil, errors.New("invalid cursor")
	}

	return &p, nil
}

// True when a sorts before b in the client order: ingested_at, then hazard_id, both newest first.
func newer(a, b HazardEvent) bool {
	if !a.IngestedAt.Equal(b.IngestedAt) {
		return a.IngestedAt.After(b.IngestedAt)
	}

	return a.HazardID > b.HazardID
}

// K-way merge of lists that are each already in client order. Returns at most limit+1 records so the caller can tell whether another page follows.
func mergeLists(lists [][]HazardEvent, limit int) []HazardEvent {
	idx := make([]int, len(lists))

	var out []HazardEvent

	for len(out) <= limit {
		best := -1

		for i, l := range lists {
			if idx[i] >= len(l) {
				continue
			}

			if best < 0 || newer(l[idx[i]], lists[best][idx[best]]) {
				best = i
			}
		}

		if best < 0 {
			break
		}

		out = append(out, lists[best][idx[best]])
		idx[best]++
	}

	return out
}

// One shard's rows from one node, newest first, strictly after the position. Asks for limit rows.
func listShardOn(ctx context.Context, db *sql.DB, shard int, after *listPos, limit int) ([]HazardEvent, error) {
	var ts, id any

	if after != nil {
		ts, id = after.T, after.ID
	}

	rows, err := db.QueryContext(ctx, `
SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name,
       latitude, longitude, occurred_at, ingested_at, attributes
FROM hazard_events
WHERE shard = $1
  AND ($2::timestamptz IS NULL OR (ingested_at, hazard_id) < ($2::timestamptz, $3::uuid))
ORDER BY ingested_at DESC, hazard_id DESC
LIMIT $4`, shard, ts, id, limit)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var out []HazardEvent

	for rows.Next() {
		h, err := scanHazard(rows)
		if err != nil {
			return nil, err
		}

		out = append(out, h)
	}

	return out, rows.Err()
}

// One page of every hazard across all shards, merged into client order. Each shard is asked in parallel, primary first, then the replica, each copy with copyTimeout. A shard with no answering copy is listed in unavailable (as "shard-N") and the result is partial; this never fails because of a down shard.
func (c *cluster) list(ctx context.Context, limit int, after *listPos) (page []HazardEvent, more bool, unavailable []string) {
	live := c.live()
	lists := make([][]HazardEvent, numShards)
	failed := make([]bool, numShards)

	var wg sync.WaitGroup

	for s := range numShards {
		wg.Go(func() {
			for _, o := range c.syncedFirst(s, owners(s, live)) {
				cctx, cancel := context.WithTimeout(ctx, copyTimeout)
				rows, err := c.listShard(cctx, o.node, s, after, limit+1)

				cancel()

				if err == nil {
					lists[s] = rows

					return
				}
			}

			failed[s] = true
		})
	}

	wg.Wait()

	for s, f := range failed {
		if f {
			unavailable = append(unavailable, fmt.Sprintf("shard-%d", s))
		}
	}

	merged := mergeLists(lists, limit)
	if len(merged) > limit {
		return merged[:limit], true, unavailable
	}

	return merged, false, unavailable
}

// Writes a page of copied rows to one node in one transaction. DO NOTHING keeps whatever is there: a copied old page never overwrites a newer live write.
func copyOn(ctx context.Context, db *sql.DB, rows []HazardEvent) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() { _ = tx.Rollback() }()

	for _, h := range rows {
		attrs := h.Attributes
		if attrs == nil {
			attrs = map[string]any{}
		}

		raw, err := json.Marshal(attrs)
		if err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx, `
INSERT INTO hazard_events
    (hazard_id, source, source_ref_id, hazard_type, severity, area_name,
     latitude, longitude, occurred_at, ingested_at, attributes, shard)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9::timestamptz, $10::timestamptz, $11::jsonb, $12::smallint)
ON CONFLICT DO NOTHING`,
			h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity, h.AreaName,
			h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, string(raw), shardOf(h.HazardID)); err != nil {
			return err
		}
	}

	return tx.Commit()
}
