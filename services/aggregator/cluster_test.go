package main

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

var errDown = errors.New("node down")

// Fake store nodes: a map of rows per node name, plus switches for down and slow.
type fakeStore struct {
	mu   sync.Mutex
	rows map[string]map[string]HazardEvent
	down map[string]bool
	slow map[string]bool
	hits map[string]int
}

func newFake() *fakeStore {
	return &fakeStore{rows: map[string]map[string]HazardEvent{}, down: map[string]bool{}, slow: map[string]bool{}, hits: map[string]int{}}
}

func (f *fakeStore) gate(ctx context.Context, n *node) error {
	f.mu.Lock()
	f.hits[n.name]++
	down, slow := f.down[n.name], f.slow[n.name]
	f.mu.Unlock()

	if down {
		return errDown
	}

	if slow {
		<-ctx.Done()

		return ctx.Err()
	}

	return nil
}

func (f *fakeStore) cluster(names ...string) *cluster {
	c := newCluster(func() []*node { return nodes(names...) })
	c.write = func(ctx context.Context, n *node, h HazardEvent) (bool, error) {
		if err := f.gate(ctx, n); err != nil {
			return false, err
		}

		f.mu.Lock()
		defer f.mu.Unlock()

		if f.rows[n.name] == nil {
			f.rows[n.name] = map[string]HazardEvent{}
		}

		_, had := f.rows[n.name][h.HazardID]
		f.rows[n.name][h.HazardID] = h

		return !had, nil
	}
	c.read = func(ctx context.Context, n *node, id string) (*HazardEvent, error) {
		if err := f.gate(ctx, n); err != nil {
			return nil, err
		}

		f.mu.Lock()
		defer f.mu.Unlock()

		if h, ok := f.rows[n.name][id]; ok {
			return &h, nil
		}

		return nil, nil //nolint:nilnil // not found
	}

	return c
}

func hz(ref string) HazardEvent {
	return HazardEvent{HazardID: hazardID("BMKG", ref), Source: "BMKG", SourceRefID: ref}
}

func TestUpsertWritesBothOwners(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.cluster("a", "b", "c")
	h := hz("E1")

	changed, err := c.upsert(t.Context(), h)
	if err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}

	for _, o := range owners(shardOf(h.HazardID), c.live()) {
		if _, ok := f.rows[o.node.name][h.HazardID]; !ok {
			t.Fatalf("owner %s has no copy", o.node.name)
		}
	}

	if len(c.dirtyShards()) != 0 {
		t.Fatal("no copy missed, shard must stay clean")
	}
}

func TestUpsertOneFailsStillSucceedsAndMarksDirty(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.cluster("a", "b", "c")
	h := hz("E1")
	own := owners(shardOf(h.HazardID), c.live())
	f.down[own[1].node.name] = true

	if _, err := c.upsert(t.Context(), h); err != nil {
		t.Fatalf("one copy is enough: %v", err)
	}

	if d := c.dirtyShards(); len(d) != 1 || d[0] != shardOf(h.HazardID) {
		t.Fatalf("dirty = %v", d)
	}
}

func TestUpsertBothFailGivesError(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.cluster("a", "b")
	f.down["a"], f.down["b"] = true, true

	if _, err := c.upsert(t.Context(), hz("E1")); !errors.Is(err, errDown) {
		t.Fatalf("err = %v", err)
	}
}

func TestUpsertNoNodesAndSingleNode(t *testing.T) {
	t.Parallel()

	if _, err := newFake().cluster().upsert(t.Context(), hz("E1")); !errors.Is(err, errNoNodes) {
		t.Fatalf("err = %v", err)
	}

	f := newFake()
	c := f.cluster("only")

	if _, err := c.upsert(t.Context(), hz("E1")); err != nil {
		t.Fatal(err)
	}

	if len(c.dirtyShards()) != 1 {
		t.Fatal("one live node leaves the shard under-replicated")
	}
}

func TestSlowCopyDoesNotBlockTheOther(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.cluster("a", "b")
	f.slow["a"] = true
	start := time.Now()

	if _, err := c.upsert(t.Context(), hz("E1")); err != nil {
		t.Fatal(err)
	}

	if d := time.Since(start); d > copyTimeout+500*time.Millisecond {
		t.Fatalf("took %v, the 1 s cap did not hold", d)
	}

	if len(f.rows["b"]) != 1 || len(c.dirtyShards()) != 1 {
		t.Fatal("fast copy must be written and the shard marked dirty")
	}
}

func TestGetFallsBackOnErrorAndTimeout(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"err", "slow"} {
		f := newFake()
		c := f.cluster("a", "b", "c")
		h := hz("E1")

		if _, err := c.upsert(t.Context(), h); err != nil {
			t.Fatal(err)
		}

		own := owners(shardOf(h.HazardID), c.live())
		if mode == "err" {
			f.down[own[0].node.name] = true
		} else {
			f.slow[own[0].node.name] = true
		}

		got, place, err := c.get(t.Context(), h.HazardID)
		if err != nil || got == nil || got.SourceRefID != "E1" {
			t.Fatalf("%s: got=%v err=%v", mode, got, err)
		}

		if len(place) != 2 || place[0].node == place[1].node {
			t.Fatalf("%s: placement %v", mode, place)
		}
	}
}

func TestGetAsksReplicaOnPrimaryMiss(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.cluster("a", "b", "c")
	h := hz("E1")
	own := owners(shardOf(h.HazardID), c.live())
	f.rows[own[1].node.name] = map[string]HazardEvent{h.HazardID: h}

	got, _, err := c.get(t.Context(), h.HazardID)
	if err != nil || got == nil {
		t.Fatalf("replica copy not found: %v %v", got, err)
	}

	got, _, err = c.get(t.Context(), hz("nope").HazardID)
	if err != nil || got != nil {
		t.Fatalf("real miss: %v %v", got, err)
	}
}

func TestGetBothUnreachableIsError(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.cluster("a", "b")
	f.down["a"], f.down["b"] = true, true

	if _, _, err := c.get(t.Context(), hz("E1").HazardID); !errors.Is(err, errDown) {
		t.Fatalf("err = %v", err)
	}
}

func TestPlacementIsTwoDifferentNodesFor100Ids(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.cluster("a", "b", "c")

	for i := range 100 {
		_, place, _ := c.get(t.Context(), hz(strconv.Itoa(i)).HazardID)
		if len(place) != 2 || place[0].node.name == place[1].node.name {
			t.Fatalf("id %d placement %v", i, place)
		}
	}
}
