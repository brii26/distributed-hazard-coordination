package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Opens the node databases from REBALANCE_TEST_URLS (comma separated, at least 4), applies the migrations to each.
func rebalanceNodes(t *testing.T) []*node {
	t.Helper()

	raw := os.Getenv("REBALANCE_TEST_URLS")
	if raw == "" {
		t.Skip("REBALANCE_TEST_URLS not set")
	}

	urls := strings.Split(raw, ",")
	if len(urls) < 4 {
		t.Fatal("need 4 urls")
	}

	var out []*node

	for i, u := range urls[:4] {
		s, err := openStore(u, 4)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() { _ = s.close() })

		out = append(out, &node{name: fmt.Sprintf("node-%d", i+1), db: s.db})
		resetNode(t, out[i])
	}

	return out
}

// Drops the data tables and applies every migration again: a node that came back with an empty disk.
func resetNode(t *testing.T, n *node) {
	t.Helper()

	ctx := context.Background()

	if _, err := n.db.ExecContext(ctx, `DROP TABLE IF EXISTS hazard_events, shard_state, source_lease, failed_messages CASCADE`); err != nil {
		t.Fatal(err)
	}

	files, _ := filepath.Glob("migrations/*.sql")
	sort.Strings(files)

	for _, f := range files {
		ddl, err := os.ReadFile(f) //nolint:gosec // test reads the project's own migration files
		if err != nil {
			t.Fatal(err)
		}

		if _, err := n.db.ExecContext(ctx, string(ddl)); err != nil {
			t.Fatal(err)
		}
	}
}

func dbHazard(i int) HazardEvent {
	ref := fmt.Sprintf("RB-%d", i)
	sev := "WASPADA"

	return HazardEvent{
		HazardID: hazardID("BMKG", ref), Source: "BMKG", SourceRefID: ref, HazardType: "SEISMIC", Severity: &sev,
		OccurredAt: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), IngestedAt: time.Date(2026, 11, 1, 0, i, 0, 0, time.UTC),
	}
}

func holds(t *testing.T, n *node, id string) bool {
	t.Helper()

	h, err := getOn(context.Background(), n.db, id)
	if err != nil {
		t.Fatal(err)
	}

	return h != nil
}

func rowCounts(t *testing.T, ns []*node) string {
	t.Helper()

	var parts []string

	for _, n := range ns {
		var c int
		if err := n.db.QueryRowContext(context.Background(), `SELECT count(*) FROM hazard_events`).Scan(&c); err != nil {
			t.Fatal(err)
		}

		parts = append(parts, fmt.Sprintf("%s=%d", n.name, c))
	}

	return strings.Join(parts, " ")
}

// Every id sits on both owners (and with del on, nowhere else) and owner digests match.
func assertPlacement(t *testing.T, c *cluster, ids []string, strict bool) {
	t.Helper()

	live := c.live()

	for _, id := range ids {
		own := owners(shardOf(id), live)
		for _, n := range live {
			_, isOwner := ownerIndex(own, n.name)
			if got := holds(t, n, id); isOwner && !got {
				t.Fatalf("%s missing on owner %s", id, n.name)
			} else if strict && !isOwner && got {
				t.Fatalf("%s still on non-owner %s", id, n.name)
			}
		}
	}
}

func TestRebalanceRealPostgres(t *testing.T) {
	ns := rebalanceNodes(t)
	ctx := t.Context()

	var mu sync.Mutex

	live := ns[:3]
	c := newCluster(func() []*node { mu.Lock(); defer mu.Unlock(); return append([]*node(nil), live...) })
	setLive := func(l []*node) { mu.Lock(); live = l; mu.Unlock() }

	var ids []string

	for i := range 500 {
		h := dbHazard(i)
		ids = append(ids, h.HazardID)

		if _, err := c.upsert(ctx, h); err != nil {
			t.Fatal(err)
		}
	}

	rb := newRebalancer(c, 0, true)
	rb.pageSize = 40

	rb.pass(ctx)
	t.Logf("bootstrap: copied=%d under=%d misplaced=%d rows: %s", rb.repairRows.Load(), rb.under.Load(), rb.misplaced.Load(), rowCounts(t, ns[:3]))

	if rb.repairRows.Load() != 0 || rb.under.Load() != 0 {
		t.Fatal("bootstrap must mark synced without copying")
	}

	// Scenario 1: node-2 comes back with an empty disk.
	resetNode(t, ns[1])
	t.Logf("wiped node-2: rows %s", rowCounts(t, ns[:3]))

	rb.pass(ctx)
	t.Logf("after re-replicate: copied=%d under=%d misplaced=%d rows: %s", rb.repairRows.Load(), rb.under.Load(), rb.misplaced.Load(), rowCounts(t, ns[:3]))
	assertPlacement(t, c, ids, true)

	if rb.under.Load() != 0 {
		t.Fatalf("under=%d", rb.under.Load())
	}

	// Scenario 2: a fourth node joins while a writer keeps going.
	setLive(ns)

	var (
		wg   sync.WaitGroup
		wmu  sync.Mutex
		more []string
	)

	wg.Go(func() {
		for i := 500; i < 600; i++ {
			h := dbHazard(i)

			if _, err := c.upsert(ctx, h); err != nil {
				t.Error(err)

				return
			}

			wmu.Lock()
			more = append(more, h.HazardID)
			wmu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	})

	rb.pass(ctx)
	wg.Wait()
	rb.pass(ctx)

	ids = append(ids, more...)
	t.Logf("after join: copied=%d under=%d misplaced=%d rows: %s", rb.repairRows.Load(), rb.under.Load(), rb.misplaced.Load(), rowCounts(t, ns))
	assertPlacement(t, c, ids, true)

	var total int

	for _, n := range ns {
		var cnt int
		_ = n.db.QueryRowContext(ctx, `SELECT count(*) FROM hazard_events`).Scan(&cnt)
		total += cnt
	}

	if total != 2*len(ids) {
		t.Fatalf("total rows %d, want %d (two copies of %d records)", total, 2*len(ids), len(ids))
	}

	var onNew int
	_ = ns[3].db.QueryRowContext(ctx, `SELECT count(*) FROM hazard_events`).Scan(&onNew)

	if onNew == 0 {
		t.Fatal("node-4 holds nothing")
	}

	// Scenario 3: lose node-1 at once (no repair yet), then node-3 after repair. Every id stays readable.
	setLive([]*node{ns[1], ns[2], ns[3]})
	readAll(t, c, ids)

	rb.pass(ctx)
	t.Logf("after first loss: under=%d misplaced=%d rows: %s", rb.under.Load(), rb.misplaced.Load(), rowCounts(t, ns[1:]))

	setLive([]*node{ns[1], ns[3]})
	readAll(t, c, ids)

	rb.pass(ctx)
	t.Logf("after second loss: under=%d misplaced=%d rows: %s", rb.under.Load(), rb.misplaced.Load(), rowCounts(t, []*node{ns[1], ns[3]}))
	assertPlacement(t, c, ids, true)
}

func readAll(t *testing.T, c *cluster, ids []string) {
	t.Helper()

	for _, id := range ids {
		h, _, err := c.get(context.Background(), id)
		if err != nil || h == nil {
			t.Fatalf("id %s unreadable: h=%v err=%v", id, h, err)
		}
	}
}
