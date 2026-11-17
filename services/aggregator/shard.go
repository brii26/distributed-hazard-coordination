package main

import (
	"crypto/sha1" //nolint:gosec // UUID v5 is defined with SHA-1, it is an id and not a security hash
	"database/sql"
	"fmt"
	"hash/fnv"
	"sort"
)

// How many fixed shards the data is cut into. A record's shard never changes.
const numShards = 16

// Random-looking but fixed namespace for hazard ids. Changing it changes every id.
var hazardNamespace = [16]byte{0x6b, 0x4e, 0x1d, 0x2a, 0x91, 0x3c, 0x4f, 0x57, 0x8a, 0x0e, 0x5d, 0x7c, 0x21, 0xb8, 0x39, 0xf4}

// UUID v5 of source|source_ref_id, so the same record gets the same id on every Aggregator and every restart.
func hazardID(source, ref string) string {
	return uuidV5(hazardNamespace, source+"|"+ref)
}

// RFC 4122 UUID v5: SHA-1 of namespace then name, with version and variant bits set.
func uuidV5(ns [16]byte, name string) string {
	h := sha1.New() //nolint:gosec // see import

	_, _ = h.Write(ns[:])
	_, _ = h.Write([]byte(name))

	b := h.Sum(nil)[:16]
	b[6] = b[6]&0x0f | 0x50
	b[8] = b[8]&0x3f | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func fnv32(s string) uint32 {
	h := fnv.New32a()

	_, _ = h.Write([]byte(s))

	return h.Sum32()
}

// The shard that owns a hazard id, fnv32a mod 16.
func shardOf(id string) int {
	return int(fnv32(id) % numShards)
}

// One store node: its name (from bnpb.node_name) and its Postgres pool.
type node struct {
	name string
	ip   string // pod IP the pool was opened against, set by membership
	db   *sql.DB
}

// One copy of a shard: which node holds it and under which unit name ("shard-2", "shard-2-r1").
type copyRef struct {
	node       *node
	unit, role string
}

// Rendezvous hashing: score every live node with fnv32a(shard|node), highest two own the shard, first is the primary. One live node gives one owner.
func owners(shard int, live []*node) []copyRef {
	ranked := append([]*node(nil), live...)
	score := func(n *node) uint32 { return fnv32(fmt.Sprintf("%d|%s", shard, n.name)) }

	sort.Slice(ranked, func(i, j int) bool {
		if si, sj := score(ranked[i]), score(ranked[j]); si != sj {
			return si > sj
		}

		return ranked[i].name < ranked[j].name
	})

	out := make([]copyRef, 0, 2)

	for i, n := range ranked {
		if i == 2 {
			break
		}

		unit, role := fmt.Sprintf("shard-%d", shard), "primary"
		if i == 1 {
			unit, role = unit+"-r1", "replica"
		}

		out = append(out, copyRef{node: n, unit: unit, role: role})
	}

	return out
}
