package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"
)

// How long one copy gets to answer before it counts as down.
const copyTimeout = time.Second

var errNoNodes = errors.New("no live store nodes")

// Writes and reads hazards over the live store nodes, two copies per shard. The per-node calls are injected so tests need no database.
type cluster struct {
	live  func() []*node
	write func(ctx context.Context, n *node, h HazardEvent) (bool, error)
	read  func(ctx context.Context, n *node, id string) (*HazardEvent, error)

	listShard func(ctx context.Context, n *node, shard int, after *listPos, limit int, f listFilter) ([]HazardEvent, error)
	idsShard  func(ctx context.Context, n *node, shard int, source string) ([]string, error)

	analyticsShard func(ctx context.Context, n *node, shard int, sp analyticsSpec) (*analytics, error)

	// Whether a node holds a shard in full (shard_state). Nil means every copy counts as synced.
	isSynced func(name string, shard int) bool

	// How many store nodes the cluster should have (STORE_EXPECTED_NODES). 0 means not set.
	expectedNodes int

	mu    sync.Mutex
	dirty map[int]bool

	// Copies written successfully per node since start, under mu.
	nodeWrites map[string]int64
}

func (c *cluster) countWrite(node string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nodeWrites == nil {
		c.nodeWrites = map[string]int64{}
	}

	c.nodeWrites[node]++
}

// Prometheus text for the writes each node took on this pod. Every pod exports its own, so a report sums them.
func (c *cluster) writeMetrics(w io.Writer) {
	c.mu.Lock()
	defer c.mu.Unlock()

	names := make([]string, 0, len(c.nodeWrites))
	for name := range c.nodeWrites {
		names = append(names, name)
	}

	slices.Sort(names)

	_, _ = io.WriteString(w, "# TYPE store_node_writes_total counter\n")

	for _, name := range names {
		_, _ = fmt.Fprintf(w, "store_node_writes_total{node=%q} %d\n", name, c.nodeWrites[name])
	}
}

// How many expected store nodes are missing from the live list. Membership drops a dead node after its grace period, so this is what keeps a lost node visible afterwards. 0 when expectedNodes is not set.
func (c *cluster) missingNodes() int {
	return max(0, c.expectedNodes-len(c.live()))
}

// Owners with synced copies first, so a joined node that is still empty answers only as a last resort. The original order is kept inside each group.
func (c *cluster) syncedFirst(shard int, own []copyRef) []copyRef {
	if c.isSynced == nil || len(own) < 2 {
		return own
	}

	out := make([]copyRef, 0, len(own))

	for _, o := range own {
		if c.isSynced(o.node.name, shard) {
			out = append(out, o)
		}
	}

	for _, o := range own {
		if !c.isSynced(o.node.name, shard) {
			out = append(out, o)
		}
	}

	return out
}

func newCluster(live func() []*node) *cluster {
	return &cluster{
		live:  live,
		write: func(ctx context.Context, n *node, h HazardEvent) (bool, error) { return upsertOn(ctx, n.db, h) },
		read:  func(ctx context.Context, n *node, id string) (*HazardEvent, error) { return getOn(ctx, n.db, id) },
		listShard: func(ctx context.Context, n *node, shard int, after *listPos, limit int, f listFilter) ([]HazardEvent, error) {
			return listShardOn(ctx, n.db, shard, after, limit, f)
		},
		idsShard: func(ctx context.Context, n *node, shard int, source string) ([]string, error) {
			return idsShardOn(ctx, n.db, shard, source)
		},
		analyticsShard: func(ctx context.Context, n *node, shard int, sp analyticsSpec) (*analytics, error) {
			return analyticsShardOn(ctx, n.db, shard, sp)
		},
		dirty: map[int]bool{},
	}
}

// Writes h to every owner of its shard, see fanout.
func (c *cluster) upsert(ctx context.Context, h HazardEvent) (bool, error) {
	return c.fanout(ctx, shardOf(h.HazardID), func(ctx context.Context, n *node) (bool, error) {
		changed, err := c.write(ctx, n, h)
		if err == nil {
			c.countWrite(n.name)
		}

		return changed, err
	})
}

// Runs write on every owner of the shard at once, each with its own timeout. One commit is enough; a copy that missed marks the shard dirty. True if any copy was new or changed.
func (c *cluster) fanout(ctx context.Context, shard int, write func(context.Context, *node) (bool, error)) (bool, error) {
	own := owners(shard, c.live())
	if len(own) == 0 {
		return false, errNoNodes
	}

	type result struct {
		changed bool
		err     error
	}

	res := make([]result, len(own))

	var wg sync.WaitGroup

	for i, o := range own {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, copyTimeout)
			defer cancel()

			res[i].changed, res[i].err = write(cctx, o.node)
		})
	}

	wg.Wait()

	var (
		changed, committed bool
		errs               []error
	)

	for _, r := range res {
		if r.err != nil {
			errs = append(errs, r.err)

			continue
		}

		committed, changed = true, changed || r.changed
	}

	fenced := slices.ContainsFunc(errs, func(e error) bool { return errors.Is(e, errFenced) })

	if !committed {
		return false, errors.Join(errs...)
	}

	if len(errs) > 0 || len(own) < 2 {
		c.markDirty(shard)
	}

	// One copy committed, but a stale writer must still stop.
	if fenced {
		return changed, errors.Join(errs...)
	}

	return changed, nil
}

// Remembers that a shard has fewer than two good copies. Memory only: after a restart the repair job compares copies anyway.
func (c *cluster) markDirty(shard int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dirty[shard] = true
}

// The shards that missed a copy, for the repair job.
func (c *cluster) dirtyShards() []int {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]int, 0, len(c.dirty))
	for s := range c.dirty {
		out = append(out, s)
	}

	return out
}

// Reads one hazard: the primary first, then the replica when the primary fails or has no row (a missed write can leave a hole). Returns the placement too. Both copies unreachable is an error.
func (c *cluster) get(ctx context.Context, id string) (*HazardEvent, []copyRef, error) {
	own := owners(shardOf(id), c.live())
	if len(own) == 0 {
		return nil, nil, errNoNodes
	}

	var (
		errs     []error
		answered bool
	)

	for _, o := range c.syncedFirst(shardOf(id), own) {
		cctx, cancel := context.WithTimeout(ctx, copyTimeout)
		h, err := c.read(cctx, o.node, id)

		cancel()

		if err != nil {
			errs = append(errs, err)

			continue
		}

		if h != nil {
			return h, own, nil
		}

		answered = true
	}

	if !answered {
		return nil, own, errors.Join(errs...)
	}

	return nil, own, nil
}

// Asks every owner of the hazard's shard at once, each with copyTimeout, and returns the copies that really hold the row, primary first. silent names the units of the copies that errored or timed out; asked is how many owners there were.
func (c *cluster) locate(ctx context.Context, id string) (found []copyRef, silent []string, asked int) {
	own := owners(shardOf(id), c.live())
	has := make([]bool, len(own))
	ok := make([]bool, len(own))

	var wg sync.WaitGroup

	for i, o := range own {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, copyTimeout)
			defer cancel()

			h, err := c.read(cctx, o.node, id)
			ok[i], has[i] = err == nil, err == nil && h != nil
		})
	}

	wg.Wait()

	for i, o := range own {
		if !ok[i] {
			silent = append(silent, o.unit)
		}

		if has[i] {
			found = append(found, o)
		}
	}

	return found, silent, len(own)
}
