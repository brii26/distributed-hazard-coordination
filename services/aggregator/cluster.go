package main

import (
	"context"
	"errors"
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

	mu    sync.Mutex
	dirty map[int]bool
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
		dirty: map[int]bool{},
	}
}

// Writes h to every owner of its shard at once, each with its own timeout. One commit is enough; a copy that missed marks the shard dirty. True if any copy was new or changed.
func (c *cluster) upsert(ctx context.Context, h HazardEvent) (bool, error) {
	shard := shardOf(h.HazardID)

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

			res[i].changed, res[i].err = c.write(cctx, o.node, h)
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

	for _, o := range own {
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
