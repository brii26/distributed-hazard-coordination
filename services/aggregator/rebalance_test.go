package main

import (
	"context"
	"maps"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Rebalancer over the fake nodes. The fake keeps its own synced marks and a log of marks and purges.
type rbFake struct {
	*fakeStore

	synced map[string]map[int]bool
	log    []string
}

func newRBFake(t *testing.T, f *fakeStore, c *cluster, del bool) (*rebalancer, *rbFake) {
	t.Helper()

	x := &rbFake{fakeStore: f, synced: map[string]map[int]bool{}}
	rb := newRebalancer(c, 0, del)
	rb.pageSize, rb.verifyWait = 7, 0

	rb.scan = func(ctx context.Context, n *node) (map[int]shardStat, error) {
		if err := f.gate(ctx, n); err != nil {
			return nil, err
		}

		f.mu.Lock()
		defer f.mu.Unlock()

		ids := map[int][]string{}
		for id := range f.rows[n.name] {
			ids[shardOf(id)] = append(ids[shardOf(id)], id)
		}

		out := map[int]shardStat{}

		for sh, l := range ids {
			sort.Strings(l)

			out[sh] = shardStat{count: len(l), digest: strings.Join(l, ",")}
		}

		for sh := range x.synced[n.name] {
			st := out[sh]
			st.synced = true
			out[sh] = st
		}

		return out, nil
	}
	rb.copyTo = func(_ context.Context, n *node, rows []HazardEvent) error {
		f.mu.Lock()
		defer f.mu.Unlock()

		if f.rows[n.name] == nil {
			f.rows[n.name] = map[string]HazardEvent{}
		}

		for _, h := range rows {
			if _, ok := f.rows[n.name][h.HazardID]; !ok {
				f.rows[n.name][h.HazardID] = h
			}
		}

		return nil
	}
	rb.setSynced = func(_ context.Context, n *node, shard int) error {
		f.mu.Lock()
		defer f.mu.Unlock()

		if x.synced[n.name] == nil {
			x.synced[n.name] = map[int]bool{}
		}

		x.synced[n.name][shard] = true
		x.log = append(x.log, "sync:"+n.name+":"+strconv.Itoa(shard))

		return nil
	}
	rb.purge = func(_ context.Context, n *node, shard int) error {
		f.mu.Lock()
		defer f.mu.Unlock()

		for id := range f.rows[n.name] {
			if shardOf(id) == shard {
				delete(f.rows[n.name], id)
			}
		}

		delete(x.synced[n.name], shard)

		x.log = append(x.log, "purge:"+n.name+":"+strconv.Itoa(shard))

		return nil
	}

	return rb, x
}

// Every id sits on both of its owners.
func assertTwoCopies(t *testing.T, f *fakeStore, c *cluster, all []HazardEvent) {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()

	for _, h := range all {
		for _, o := range owners(shardOf(h.HazardID), c.live()) {
			if _, ok := f.rows[o.node.name][h.HazardID]; !ok {
				t.Fatalf("%s missing on owner %s", h.SourceRefID, o.node.name)
			}
		}
	}
}

func TestRebalanceRestoresWipedNode(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	all := seed(t, c, 60)
	rb, x := newRBFake(t, f, c, true)

	lost := len(f.rows["b"])
	f.rows["b"] = map[string]HazardEvent{}

	rb.pass(t.Context())
	assertTwoCopies(t, f, c, all)

	if got := rb.repairRows.Load(); got != int64(lost) {
		t.Fatalf("copied %d rows, wiped %d", got, lost)
	}

	if rb.under.Load() != 0 || rb.misplaced.Load() != 0 {
		t.Fatalf("under=%d misplaced=%d", rb.under.Load(), rb.misplaced.Load())
	}

	before, marks := rb.repairRows.Load(), len(x.log)

	rb.pass(t.Context())

	if rb.repairRows.Load() != before || len(x.log) != marks {
		t.Fatal("a second pass changed something")
	}
}

func TestRebalanceKeepsLiveWrite(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	seed(t, c, 40)
	rb, _ := newRBFake(t, f, c, false)

	f.rows["b"] = map[string]HazardEvent{}

	var once sync.Once

	inner := rb.copyTo
	area := "live"

	var target string

	rb.copyTo = func(ctx context.Context, n *node, rows []HazardEvent) error {
		once.Do(func() {
			v2 := rows[0]
			v2.AreaName = &area
			target = v2.HazardID

			f.mu.Lock()
			if f.rows[n.name] == nil {
				f.rows[n.name] = map[string]HazardEvent{}
			}

			f.rows[n.name][v2.HazardID] = v2
			f.mu.Unlock()
		})

		return inner(ctx, n, rows)
	}

	rb.pass(t.Context())

	found := false

	for _, nm := range []string{"a", "b", "c"} {
		if h, ok := f.rows[nm][target]; ok && h.AreaName != nil && *h.AreaName == "live" {
			found = true
		}
	}

	if !found {
		t.Fatal("live write was lost")
	}
}

func TestRebalanceSourceFailsThenAnotherPass(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	all := seed(t, c, 60)
	rb, _ := newRBFake(t, f, c, true)
	f.rows["b"] = map[string]HazardEvent{}

	inner := c.listShard
	broken := true
	c.listShard = func(ctx context.Context, n *node, shard int, after *listPos, limit int, f listFilter) ([]HazardEvent, error) {
		if broken && n.name == "a" {
			return nil, errDown
		}

		return inner(ctx, n, shard, after, limit, f)
	}

	rb.pass(t.Context())

	// Shards that b shares with c can still be fixed from c; shards whose only other copy is on a cannot.
	if rb.under.Load() == 0 {
		t.Fatal("expected shards left unsynced while the source is failing")
	}

	broken = false

	rb.pass(t.Context())
	assertTwoCopies(t, f, c, all)

	if rb.under.Load() != 0 {
		t.Fatalf("under=%d", rb.under.Load())
	}
}

func TestRebalanceNoHolderDeletesNothing(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c", "d")
	seed(t, c, 40)
	rb, x := newRBFake(t, f, c, true)

	f.down["c"] = true

	rb.pass(t.Context())

	for _, l := range x.log {
		if strings.HasPrefix(l, "purge:") {
			t.Fatalf("deleted while a node was down: %s", l)
		}
	}
}

func TestRebalanceStopsWhenLeaseLost(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	seed(t, c, 60)
	rb, _ := newRBFake(t, f, c, false)

	f.rows["b"] = map[string]HazardEvent{}

	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	inner := rb.copyTo
	rb.copyTo = func(ctx context.Context, n *node, rows []HazardEvent) error {
		calls++

		cancel()

		return inner(ctx, n, rows)
	}

	rb.pass(ctx)

	if calls != 1 {
		t.Fatalf("copyTo called %d times after the lease was lost", calls)
	}
}

//nolint:gocyclo // one table-like scenario, splitting it hides the order check
func TestRebalanceJoinDeletesOnlyAfterSynced(t *testing.T) {
	t.Parallel()

	f := newFake()
	three := f.listCluster("a", "b", "c")
	all := seed(t, three, 80)

	live := []string{"a", "b", "c", "d"}

	for _, del := range []bool{false, true} {
		g := newFake()
		g.rows = map[string]map[string]HazardEvent{}

		for k, v := range f.rows {
			g.rows[k] = copyRows(v)
		}

		cc := g.listCluster(live...)
		rb, x := newRBFake(t, g, cc, del)

		rb.pass(t.Context())
		assertTwoCopies(t, g, cc, all)

		synced := map[string]bool{}

		for _, l := range x.log {
			p := strings.Split(l, ":")
			if p[0] == "sync" {
				synced[p[1]+":"+p[2]] = true
			}

			if p[0] == "purge" {
				for _, o := range owners(mustInt(p[2]), cc.live()) {
					if !synced[o.node.name+":"+p[2]] {
						t.Fatalf("purge before owner %s synced", o.node.name)
					}
				}
			}
		}

		extra := 0

		for nm, rows := range g.rows {
			for id := range rows {
				if _, own := ownerIndex(owners(shardOf(id), cc.live()), nm); !own {
					extra++
				}
			}
		}

		if del && (extra != 0 || rb.misplaced.Load() != 0) {
			t.Fatalf("delete on: %d rows still on non-owners", extra)
		}

		if !del && extra == 0 {
			t.Fatal("delete off removed copies")
		}
	}
}

func copyRows(in map[string]HazardEvent) map[string]HazardEvent { return maps.Clone(in) }

func mustInt(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}

	return n
}

func TestRebalanceBootstrapMarksSyncedWithoutCopy(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	seed(t, c, 50)
	rb, x := newRBFake(t, f, c, true)

	rb.pass(t.Context())

	if rb.repairRows.Load() != 0 {
		t.Fatalf("copied %d rows although owners were equal", rb.repairRows.Load())
	}

	if n := len(x.synced["a"]) + len(x.synced["b"]) + len(x.synced["c"]); n != 2*numShards {
		t.Fatalf("synced marks = %d, want %d", n, 2*numShards)
	}
}

func TestRebalanceRunOnLiveSetChange(t *testing.T) {
	t.Parallel()

	f := newFake()

	var mu sync.Mutex

	names := []string{"a", "b"}
	c := f.listCluster("a", "b")
	c.live = func() []*node { mu.Lock(); defer mu.Unlock(); return nodes(names...) }
	seed(t, c, 20)
	rb, x := newRBFake(t, f, c, false)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	go rb.run(ctx, time.Hour)

	waitUntil(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(x.synced["a"]) == numShards })

	mu.Lock()
	names = []string{"a", "b", "c"}
	mu.Unlock()

	waitUntil(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(x.synced["c"]) > 0 })
}

// Prints the real placement of the 16 shards for the design figures: go test -run PlacementCounts -v.
func TestPlacementCounts(t *testing.T) {
	t.Parallel()

	group := func(names ...string) map[string][]int {
		out := map[string][]int{}

		for s := range numShards {
			var pair []string
			for _, o := range owners(s, nodes(names...)) {
				pair = append(pair, o.node.name)
			}

			sort.Strings(pair)

			k := strings.Join(pair, "+")
			out[k] = append(out[k], s)
		}

		return out
	}

	t.Logf("3 nodes: %v", group("node-1", "node-2", "node-3"))

	before, after := group("node-1", "node-3"), group("node-1", "node-2", "node-3")
	held := map[string]int{}

	for k, v := range after {
		for n := range strings.SplitSeq(k, "+") {
			held[n] += len(v)
		}
	}

	t.Logf("2 nodes: %v", before)
	t.Logf("join: shards held per node after: %v", held)
}

func waitUntil(t *testing.T, ok func() bool) {
	t.Helper()

	for range 400 {
		if ok() {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("condition not reached")
}
