package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultRebalanceInterval = 30 * time.Second
	defaultPagePause         = 100 * time.Millisecond
	rebalancePage            = 500
	liveCheckEvery           = 2 * time.Second
	verifyTries              = 3
	verifyWait               = 500 * time.Millisecond
)

// What one node holds of one shard: row count, digest of the sorted ids, and whether the node marked it synced.
type shardStat struct {
	count  int
	digest string
	synced bool
}

// Keeps two copies of every shard on its owners. Per shard: compare, copy what is missing, mark synced, then optionally drop copies on non-owners. The per-node calls are injected so tests need no database.
type rebalancer struct {
	c         *cluster
	scan      func(ctx context.Context, n *node) (map[int]shardStat, error)
	copyTo    func(ctx context.Context, n *node, rows []HazardEvent) error
	setSynced func(ctx context.Context, n *node, shard int) error
	purge     func(ctx context.Context, n *node, shard int) error

	pageSize   int
	pause      time.Duration
	del        bool
	verifyWait time.Duration

	repairRows atomic.Int64
	under      atomic.Int64
	misplaced  atomic.Int64

	mu       sync.Mutex
	rowGauge map[string]map[int]int
}

func newRebalancer(c *cluster, pause time.Duration, del bool) *rebalancer {
	return &rebalancer{
		c: c, pageSize: rebalancePage, pause: pause, del: del, verifyWait: verifyWait,
		scan:      scanOn,
		copyTo:    func(ctx context.Context, n *node, rows []HazardEvent) error { return copyOn(ctx, n.db, rows) },
		setSynced: func(ctx context.Context, n *node, shard int) error { return markSyncedOn(ctx, n.db, shard) },
		purge:     func(ctx context.Context, n *node, shard int) error { return purgeShardOn(ctx, n.db, shard) },
		rowGauge:  map[string]map[int]int{},
	}
}

func liveSignature(live []*node) string {
	names := make([]string, len(live))
	for i, n := range live {
		names[i] = n.name
	}

	sort.Strings(names)

	return strings.Join(names, ",")
}

// Runs a pass at start, every interval, and whenever the live set changes. Returns when ctx is done (lease lost or shutdown).
func (r *rebalancer) run(ctx context.Context, interval time.Duration) {
	sig := ""
	last := time.Time{}
	t := time.NewTicker(liveCheckEvery)

	defer t.Stop()

	for {
		if cur := liveSignature(r.c.live()); cur != sig || time.Since(last) >= interval {
			sig, last = cur, time.Now()

			r.pass(ctx)
		}

		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Scans every live node once, then settles each shard.
func (r *rebalancer) pass(ctx context.Context) {
	live := r.c.live()
	stats := map[string]map[int]shardStat{}
	allOK := true

	for _, n := range live {
		s, err := r.scan(ctx, n)
		if err != nil {
			allOK = false

			withID("-").Warn("rebalance scan failed", "node", n.name, "error", err.Error())

			continue
		}

		stats[n.name] = s
	}

	var under, mis int64

	for shard := range numShards {
		if ctx.Err() != nil {
			return
		}

		own := owners(shard, live)
		if len(own) == 0 {
			continue
		}

		if !r.settle(ctx, shard, own, live, stats, allOK) {
			under++
		}

		for _, n := range live {
			if _, isOwner := ownerIndex(own, n.name); !isOwner && stats[n.name][shard].count > 0 {
				mis++

				break
			}
		}
	}

	r.under.Store(under)
	r.misplaced.Store(mis)

	g := map[string]map[int]int{}

	for name, s := range stats {
		g[name] = map[int]int{}
		for sh, st := range s {
			g[name][sh] = st.count
		}
	}

	r.mu.Lock()
	r.rowGauge = g
	r.mu.Unlock()
}

func ownerIndex(own []copyRef, name string) (int, bool) {
	for i, o := range own {
		if o.node.name == name {
			return i, true
		}
	}

	return 0, false
}

// Makes every owner of one shard complete. True when all owners are synced and equal.
func (r *rebalancer) settle(ctx context.Context, shard int, own []copyRef, live []*node, stats map[string]map[int]shardStat, allOK bool) bool {
	for _, o := range own {
		if _, ok := stats[o.node.name]; !ok {
			return false // owner unreachable: leave the shard alone
		}
	}

	copiedFrom := map[string]bool{}

	for try := range verifyTries {
		if ctx.Err() != nil {
			return false
		}

		if r.fillOwners(ctx, shard, own, live, stats, copiedFrom) {
			r.rescan(ctx, live, stats)
		}

		if r.ownersEqual(own, shard, stats) {
			break
		}

		if try == verifyTries-1 {
			return false
		}

		if !sleepCtx(ctx, r.verifyWait) {
			return false
		}

		r.rescan(ctx, live, stats)
	}

	if !r.markOwners(ctx, shard, own, stats) {
		return false
	}

	r.dropDisplaced(ctx, shard, own, live, stats, copiedFrom, allOK)

	return true
}

// Copies into each owner from every holder whose digest differs. True if any page was copied.
func (r *rebalancer) fillOwners(ctx context.Context, shard int, own []copyRef, live []*node, stats map[string]map[int]shardStat, copiedFrom map[string]bool) bool {
	copied := false

	for _, o := range own {
		for _, h := range live {
			hs, os := stats[h.name][shard], stats[o.node.name][shard]
			if h.name == o.node.name || hs.count == 0 || hs.digest == os.digest {
				continue
			}

			if err := r.copyShard(ctx, shard, h, o.node); err != nil {
				withID("-").Warn("rebalance copy failed", "shard", shard, "from", h.name, "to", o.node.name, "error", err.Error())

				continue
			}

			copiedFrom[h.name], copied = true, true
		}
	}

	return copied
}

// Sets the synced mark on every owner that lacks it.
func (r *rebalancer) markOwners(ctx context.Context, shard int, own []copyRef, stats map[string]map[int]shardStat) bool {
	for _, o := range own {
		st := stats[o.node.name][shard]
		if st.synced {
			continue
		}

		if err := r.setSynced(ctx, o.node, shard); err != nil {
			withID("-").Warn("rebalance mark synced failed", "shard", shard, "node", o.node.name, "error", err.Error())

			return false
		}

		st.synced = true
		stats[o.node.name][shard] = st
	}

	return true
}

func (r *rebalancer) ownersEqual(own []copyRef, shard int, stats map[string]map[int]shardStat) bool {
	first := stats[own[0].node.name][shard].digest

	for _, o := range own[1:] {
		if stats[o.node.name][shard].digest != first {
			return false
		}
	}

	return true
}

// Rescans nodes again; a node that fails keeps its old numbers.
func (r *rebalancer) rescan(ctx context.Context, live []*node, stats map[string]map[int]shardStat) {
	for _, n := range live {
		if _, ok := stats[n.name]; !ok {
			continue
		}

		if s, err := r.scan(ctx, n); err == nil {
			stats[n.name] = s
		}
	}
}

// Deletes the shard from nodes that no longer own it, only when everything is safe: switch on, all scans fine, owners synced and equal, placement unchanged, and every row there is also on the owners.
func (r *rebalancer) dropDisplaced(ctx context.Context, shard int, own []copyRef, live []*node, stats map[string]map[int]shardStat, copiedFrom map[string]bool, allOK bool) {
	if !r.del || !allOK {
		return
	}

	fresh := owners(shard, r.c.live())
	if len(fresh) != len(own) {
		return
	}

	for i := range own {
		if fresh[i].node.name != own[i].node.name {
			return
		}
	}

	want := stats[own[0].node.name][shard].digest

	for _, h := range live {
		if _, isOwner := ownerIndex(own, h.name); isOwner || stats[h.name][shard].count == 0 {
			continue
		}

		if stats[h.name][shard].digest != want && !copiedFrom[h.name] {
			continue
		}

		if ctx.Err() != nil {
			return
		}

		if err := r.purge(ctx, h, shard); err != nil {
			withID("-").Warn("rebalance delete failed", "shard", shard, "node", h.name, "error", err.Error())

			continue
		}

		stats[h.name][shard] = shardStat{}
	}
}

// Copies one shard from src to dst in pages, newest first, with a pause between pages. A retry starts over and skips rows that exist.
func (r *rebalancer) copyShard(ctx context.Context, shard int, src, dst *node) error {
	var after *listPos

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		cctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
		rows, err := r.c.listShard(cctx, src, shard, after, r.pageSize)

		cancel()

		if err != nil {
			return err
		}

		if len(rows) > 0 {
			wctx, wcancel := context.WithTimeout(ctx, dbQueryTimeout)
			err := r.copyTo(wctx, dst, rows)

			wcancel()

			if err != nil {
				return err
			}

			r.repairRows.Add(int64(len(rows)))

			last := rows[len(rows)-1]
			after = &listPos{T: last.IngestedAt, ID: last.HazardID}
		}

		if len(rows) < r.pageSize {
			return nil
		}

		if !sleepCtx(ctx, r.pause) {
			return ctx.Err()
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// Prometheus text for the rebalancer. Only the lease holder exports; others print nothing.
func (r *rebalancer) writeMetrics(w io.Writer, leader bool) {
	if !leader {
		return
	}

	var b strings.Builder

	fmt.Fprintf(&b, "# TYPE store_shards_underreplicated gauge\nstore_shards_underreplicated %d\n", r.under.Load())
	fmt.Fprintf(&b, "# TYPE store_shards_misplaced gauge\nstore_shards_misplaced %d\n", r.misplaced.Load())
	fmt.Fprintf(&b, "# TYPE store_repair_rows_total counter\nstore_repair_rows_total %d\n", r.repairRows.Load())
	b.WriteString("# TYPE store_shard_rows gauge\n")

	r.mu.Lock()
	defer func() {
		r.mu.Unlock()

		_, _ = io.WriteString(w, b.String())
	}()

	names := make([]string, 0, len(r.rowGauge))
	for name := range r.rowGauge {
		names = append(names, name)
	}

	slices.Sort(names)

	for _, name := range names {
		for sh := range numShards {
			if c, ok := r.rowGauge[name][sh]; ok {
				fmt.Fprintf(&b, "store_shard_rows{node=%q,shard=\"%d\"} %d\n", name, sh, c)
			}
		}
	}
}

// Row count and id digest per shard, plus the synced mark, from one node.
func scanOn(ctx context.Context, n *node) (map[int]shardStat, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*dbQueryTimeout)
	defer cancel()

	out, err := digestsOn(ctx, n.db)
	if err != nil {
		return nil, err
	}

	synced, err := syncedShards(ctx, n.db)
	if err != nil {
		return nil, err
	}

	for sh := range synced {
		st := out[sh]
		st.synced = true
		out[sh] = st
	}

	// A synced empty shard has no hazard rows: its digest is the empty string, same as a node with no row.
	return out, nil
}

func digestsOn(ctx context.Context, db *sql.DB) (map[int]shardStat, error) {
	rows, err := db.QueryContext(ctx, `
SELECT shard, count(*), md5(string_agg(hazard_id::text, ',' ORDER BY hazard_id))
FROM hazard_events WHERE shard IS NOT NULL GROUP BY shard`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	out := map[int]shardStat{}

	for rows.Next() {
		var (
			sh int
			st shardStat
		)

		if err := rows.Scan(&sh, &st.count, &st.digest); err != nil {
			return nil, err
		}

		out[sh] = st
	}

	return out, rows.Err()
}

// The shards a node has marked synced.
func syncedShards(ctx context.Context, db *sql.DB) (map[int]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT shard FROM shard_state WHERE synced`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	out := map[int]bool{}

	for rows.Next() {
		var sh int
		if err := rows.Scan(&sh); err != nil {
			return nil, err
		}

		out[sh] = true
	}

	return out, rows.Err()
}

func markSyncedOn(ctx context.Context, db *sql.DB, shard int) error {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	_, err := db.ExecContext(ctx, `
INSERT INTO shard_state (shard, synced, updated_at) VALUES ($1::smallint, true, now())
ON CONFLICT (shard) DO UPDATE SET synced = true, updated_at = now()`, shard)

	return err
}

// Deletes a whole shard from one node in batches, then its shard_state row.
func purgeShardOn(ctx context.Context, db *sql.DB, shard int) error {
	for {
		cctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
		res, err := db.ExecContext(cctx, `
DELETE FROM hazard_events WHERE hazard_id IN (SELECT hazard_id FROM hazard_events WHERE shard = $1::smallint LIMIT 500)`, shard)

		cancel()

		if err != nil {
			return err
		}

		if n, _ := res.RowsAffected(); n == 0 {
			break
		}

		if err := ctx.Err(); err != nil {
			return err
		}
	}

	cctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	_, err := db.ExecContext(cctx, `DELETE FROM shard_state WHERE shard = $1::smallint`, shard)

	return err
}
