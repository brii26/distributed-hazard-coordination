package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
)

// With fake hooks: the pages are walked in order and every report reaches the destination once.
func TestCopyReportsWalksEveryPage(t *testing.T) {
	t.Parallel()

	var all []reportRow
	for i := range 25 {
		all = append(all, reportRow{ReportID: fmt.Sprintf("r-%02d", i), Shard: 3})
	}

	var (
		mu  sync.Mutex
		got []string
	)

	r := &rebalancer{pageSize: 10, pause: 0}
	r.listReports = func(_ context.Context, _ *node, _ int, after string, limit int) ([]reportRow, error) {
		var out []reportRow

		for _, x := range all {
			if x.ReportID > after && len(out) < limit {
				out = append(out, x)
			}
		}

		return out, nil
	}
	r.copyReportsTo = func(_ context.Context, _ *node, rows []reportRow) error {
		mu.Lock()
		defer mu.Unlock()

		for _, x := range rows {
			got = append(got, x.ReportID)
		}

		return nil
	}

	if err := r.copyReports(t.Context(), 3, &node{name: "a"}, &node{name: "b"}); err != nil {
		t.Fatal(err)
	}

	if len(got) != 25 || got[0] != "r-00" || got[24] != "r-24" {
		t.Fatalf("copied %d reports, first %q last %q", len(got), got[0], got[len(got)-1])
	}

	if r.repairRows.Load() != 25 {
		t.Fatalf("repairRows %d, want 25", r.repairRows.Load())
	}
}

// Without the hooks nothing happens, which is what the older fake based tests rely on.
func TestCopyReportsWithoutHooksIsANoOp(t *testing.T) {
	t.Parallel()

	r := &rebalancer{pageSize: 10}
	if err := r.copyReports(t.Context(), 0, &node{}, &node{}); err != nil {
		t.Fatal(err)
	}
}

func reportCount(t *testing.T, db *sql.DB) int {
	t.Helper()

	var c int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM field_reports`).Scan(&c); err != nil {
		t.Fatal(err)
	}

	return c
}

// A report committed on one owner only gets its second copy in one pass, a repeated copy adds nothing,
// and after the first owner is gone the report is still on the second.
//
//nolint:paralleltest // the nodes are real databases
func TestRebalanceCopiesFieldReportsPostgres(t *testing.T) {
	ns := rebalanceNodes(t)
	ctx := t.Context()

	live := ns[:3]
	c := newCluster(func() []*node { return append([]*node(nil), live...) })

	// One hazard so the bootstrap pass marks the shards synced.
	if _, err := c.upsert(ctx, dbHazard(1)); err != nil {
		t.Fatal(err)
	}

	rb := newRebalancer(c, 0, true)
	rb.pass(ctx)

	rep := fieldReport{ReportID: "report-one-owner", Description: "banjir", ReporterID: "a@b.c", ReporterRole: "Lapangan"}
	shard := shardOf(rep.ReportID)
	own := owners(shard, live)

	if len(own) < 2 {
		t.Fatalf("need two owners, got %d", len(own))
	}

	firstNode, secondNode := own[0].node, own[1].node

	if _, err := insertFieldReportOn(ctx, firstNode.db, rep, "c1"); err != nil {
		t.Fatal(err)
	}

	if got := reportCount(t, secondNode.db); got != 0 {
		t.Fatalf("second owner already has %d reports", got)
	}

	rb.pass(ctx)

	if got := reportCount(t, secondNode.db); got != 1 {
		t.Fatalf("after one pass the second owner has %d reports, want 1", got)
	}

	// Repeated copy: nothing changes.
	rows, err := listReportsOn(ctx, firstNode.db, shard, "", 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list: %v, %d rows", err, len(rows))
	}

	if err := copyReportsOn(ctx, secondNode.db, rows); err != nil {
		t.Fatal(err)
	}

	if got := reportCount(t, secondNode.db); got != 1 {
		t.Fatalf("repeated copy left %d reports, want 1", got)
	}

	// The first owner loses its disk: the report is still on the second.
	resetNode(t, firstNode)

	if got := reportCount(t, secondNode.db); got != 1 {
		t.Fatalf("second owner has %d reports after the first was wiped, want 1", got)
	}

	rb.pass(ctx)

	if got := reportCount(t, firstNode.db); got != 1 {
		t.Fatalf("after re-replication the wiped owner has %d reports, want 1", got)
	}

}
