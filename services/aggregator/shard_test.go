package main

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"
)

func nodes(names ...string) []*node {
	out := make([]*node, len(names))
	for i, n := range names {
		out[i] = &node{name: n}
	}

	return out
}

func TestHazardIDIsStableUUIDv5(t *testing.T) {
	t.Parallel()

	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

	a := hazardID("BMKG", "E1")
	if !re.MatchString(a) || a != hazardID("BMKG", "E1") {
		t.Fatalf("not a stable v5 id: %q", a)
	}

	if a == hazardID("PVMBG", "E1") || a == hazardID("BMKG", "E2") {
		t.Fatal("different records must get different ids")
	}

	// Pinned so a namespace or format change is noticed: it would re-id every record.
	if a != "3d851cfc-2c5f-5cc7-804a-73261879473a" {
		t.Fatalf("hazard id changed: %s", a)
	}

	// RFC 4122 DNS namespace test vector, proves uuidV5 is a real v5.
	dns := [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	if got := uuidV5(dns, "www.example.com"); got != "2ed6657d-e927-568b-95e1-2665a8aea6a2" {
		t.Fatalf("not RFC v5: %s", got)
	}
}

func TestShardOfDeterministicAndSpread(t *testing.T) {
	t.Parallel()

	counts := make([]int, numShards)

	for i := range 1600 {
		id := hazardID("BMKG", strconv.Itoa(i))
		s := shardOf(id)

		if s < 0 || s >= numShards || s != shardOf(id) {
			t.Fatalf("bad shard %d for %s", s, id)
		}

		counts[s]++
	}

	for s, c := range counts {
		if c == 0 || c > 200 {
			t.Fatalf("shard %d holds %d of 1600, spread is off: %v", s, c, counts)
		}
	}
}

func TestOwnersTopTwoDeterministic(t *testing.T) {
	t.Parallel()

	live := nodes("a", "b", "c")
	rev := nodes("c", "b", "a")

	for s := range numShards {
		o, r := owners(s, live), owners(s, rev)

		if len(o) != 2 || o[0].node.name == o[1].node.name {
			t.Fatalf("shard %d: want 2 distinct owners, got %+v", s, o)
		}

		if o[0].node.name != r[0].node.name || o[1].node.name != r[1].node.name {
			t.Fatalf("shard %d: owners depend on input order", s)
		}

		if o[0].unit != fmt.Sprintf("shard-%d", s) || o[1].unit != fmt.Sprintf("shard-%d-r1", s) {
			t.Fatalf("bad unit names %q %q", o[0].unit, o[1].unit)
		}
	}

	if got := owners(3, nodes("a")); len(got) != 1 || got[0].role != "primary" {
		t.Fatalf("one live node: want one primary, got %+v", got)
	}
}

// How many of the 16 shards change their owner pair, and how many copies move.
func moved(from, to []*node) (shards, copies int) {
	for s := range numShards {
		before, after := map[string]bool{}, map[string]bool{}

		for _, c := range owners(s, from) {
			before[c.node.name] = true
		}

		for _, c := range owners(s, to) {
			after[c.node.name] = true
		}

		n := 0

		for k := range after {
			if !before[k] {
				n++
			}
		}

		if n > 0 {
			shards++
		}

		copies += n
	}

	return shards, copies
}

func TestOwnersMoveLittleWhenNodesChange(t *testing.T) {
	t.Parallel()

	three := nodes("node-a", "node-b", "node-c")

	// Join: only copies placed on the new node move. 2 copies x 16 shards / 4 nodes = 8 expected, allow slack.
	shards, copies := moved(three, nodes("node-a", "node-b", "node-c", "node-d"))
	t.Logf("3->4 nodes: %d of 16 shards changed, %d of 32 copies moved", shards, copies)

	if copies > 14 {
		t.Fatalf("3->4 moved %d copies, want about 8", copies)
	}

	// Leave: only copies held by the lost node need a new home. Every copy not on it stays.
	shards, copies = moved(three, nodes("node-a", "node-b"))
	t.Logf("3->2 nodes: %d of 16 shards changed, %d copies moved", shards, copies)

	if copies > 16 {
		t.Fatalf("3->2 moved %d copies, want at most 16", copies)
	}
}

func TestMembershipGraceKeepsQuietNodeThenDropsIt(t *testing.T) {
	t.Parallel()

	ips := []string{"1", "2"}
	now := time.Now()
	m := newMembership(
		func(context.Context) ([]string, error) { return ips, nil },
		func(_ context.Context, ip string) (*node, error) { return &node{name: "n" + ip}, nil },
		30*time.Second,
	)
	m.now = func() time.Time { return now }

	m.refresh(t.Context())

	if len(m.live()) != 2 {
		t.Fatalf("want 2 live, got %d", len(m.live()))
	}

	ips, now = []string{"1"}, now.Add(10*time.Second)

	m.refresh(t.Context())

	if len(m.live()) != 2 {
		t.Fatal("n2 must stay live inside the grace period")
	}

	now = now.Add(31 * time.Second)

	m.refresh(t.Context())

	if l := m.live(); len(l) != 1 || l[0].name != "n1" {
		t.Fatalf("n2 should be dropped after grace, got %v", l)
	}
}
