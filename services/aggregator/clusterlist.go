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

// What a listing asks for: direction, an exact source, and a lower bound on ingested_at. The zero value is the client order (newest first, everything).
type listFilter struct {
	Asc    bool
	Source string
	Since  *time.Time
}

// True when a sorts before b in the order f asks for.
func (f listFilter) before(a, b HazardEvent) bool {
	if f.Asc {
		return newer(b, a)
	}

	return newer(a, b)
}

// True when h passes the source and since filters.
func (f listFilter) keeps(h HazardEvent) bool {
	return (f.Source == "" || h.Source == f.Source) && (f.Since == nil || !h.IngestedAt.Before(*f.Since))
}

// K-way merge of lists that are each already in the order f asks for. Returns at most limit+1 records so the caller can tell whether another page follows.
func mergeLists(lists [][]HazardEvent, limit int, f listFilter) []HazardEvent {
	idx := make([]int, len(lists))

	var out []HazardEvent

	for len(out) <= limit {
		best := -1

		for i, l := range lists {
			if idx[i] >= len(l) {
				continue
			}

			if best < 0 || f.before(l[idx[i]], lists[best][idx[best]]) {
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

// One shard's rows from one node in the order f asks for, strictly after the position. Asks for limit rows.
func listShardOn(ctx context.Context, db *sql.DB, shard int, after *listPos, limit int, f listFilter) ([]HazardEvent, error) {
	var ts, id, src, since any

	if after != nil {
		ts, id = after.T, after.ID
	}

	if f.Source != "" {
		src = f.Source
	}

	if f.Since != nil {
		since = *f.Since
	}

	op, dir := "<", "DESC"
	if f.Asc {
		op, dir = ">", "ASC"
	}

	rows, err := db.QueryContext(ctx, `
SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name,
       latitude, longitude, occurred_at, ingested_at, attributes
FROM hazard_events
WHERE shard = $1
  AND ($2::timestamptz IS NULL OR (ingested_at, hazard_id) `+op+` ($2::timestamptz, $3::uuid))
  AND ($5::text IS NULL OR source = $5::text)
  AND ($6::timestamptz IS NULL OR ingested_at >= $6::timestamptz)
ORDER BY ingested_at `+dir+`, hazard_id `+dir+`
LIMIT $4`, shard, ts, id, limit, src, since)
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
func (c *cluster) list(ctx context.Context, limit int, after *listPos, f listFilter) (page []HazardEvent, more bool, unavailable []string) {
	live := c.live()
	lists := make([][]HazardEvent, numShards)
	failed := make([]bool, numShards)

	var wg sync.WaitGroup

	for s := range numShards {
		wg.Go(func() {
			for _, o := range owners(s, live) {
				cctx, cancel := context.WithTimeout(ctx, copyTimeout)
				rows, err := c.listShard(cctx, o.node, s, after, limit+1, f)

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

	merged := mergeLists(lists, limit, f)
	if len(merged) > limit {
		return merged[:limit], true, unavailable
	}

	return merged, false, unavailable
}

// One shard's source_ref_id values from one node for a source, in byte order. COLLATE "C" matters: the default collation is not byte order.
func idsShardOn(ctx context.Context, db *sql.DB, shard int, source string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT source_ref_id FROM hazard_events
WHERE shard = $1 AND source = $2::text
ORDER BY source_ref_id COLLATE "C"`, shard, source)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var out []string

	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}

		out = append(out, id)
	}

	return out, rows.Err()
}

// Every source_ref_id of a source in one byte-sorted list. One copy per shard (primary, else replica), so no id is counted twice. Shards with no answering copy are named in unavailable and left out.
func (c *cluster) sourceIDs(ctx context.Context, source string) (ids, unavailable []string) {
	live := c.live()
	lists := make([][]string, numShards)
	failed := make([]bool, numShards)

	var wg sync.WaitGroup

	for s := range numShards {
		wg.Go(func() {
			for _, o := range owners(s, live) {
				cctx, cancel := context.WithTimeout(ctx, copyTimeout)
				got, err := c.idsShard(cctx, o.node, s, source)

				cancel()

				if err == nil {
					lists[s] = got

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

	return mergeIDs(lists), unavailable
}

// K-way merge of byte-sorted lists.
func mergeIDs(lists [][]string) []string {
	idx := make([]int, len(lists))

	var out []string

	for {
		best := -1

		for i, l := range lists {
			if idx[i] < len(l) && (best < 0 || l[idx[i]] < lists[best][idx[best]]) {
				best = i
			}
		}

		if best < 0 {
			return out
		}

		out = append(out, lists[best][idx[best]])
		idx[best]++
	}
}
