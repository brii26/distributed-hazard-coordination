package main

import (
	"context"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/aggregator/aggregatorpb"
)

// A cluster over fake nodes whose per-shard list reads the fake rows in client order.
func (f *fakeStore) listCluster(names ...string) *cluster {
	c := f.cluster(names...)
	c.listShard = func(ctx context.Context, n *node, shard int, after *listPos, limit int) ([]HazardEvent, error) {
		if err := f.gate(ctx, n); err != nil {
			return nil, err
		}

		f.mu.Lock()
		defer f.mu.Unlock()

		var rows []HazardEvent

		for _, h := range f.rows[n.name] {
			if shardOf(h.HazardID) != shard {
				continue
			}

			if after != nil && !newer(HazardEvent{IngestedAt: after.T, HazardID: after.ID}, h) {
				continue
			}

			rows = append(rows, h)
		}

		sort.Slice(rows, func(i, j int) bool { return newer(rows[i], rows[j]) })

		if len(rows) > limit {
			rows = rows[:limit]
		}

		return rows, nil
	}

	return c
}

// Writes n records through the cluster. Every third one shares an ingested_at with its neighbour to exercise the hazard_id tie-break.
func seed(t *testing.T, c *cluster, n int) []HazardEvent {
	t.Helper()

	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	all := make([]HazardEvent, 0, n)

	for i := range n {
		h := hz("r" + strconv.Itoa(i))
		h.IngestedAt = base.Add(time.Duration(i-i%3) * time.Minute)

		if _, err := c.upsert(context.Background(), h); err != nil {
			t.Fatal(err)
		}

		all = append(all, h)
	}

	sort.Slice(all, func(i, j int) bool { return newer(all[i], all[j]) })

	return all
}

func apiOf(c *cluster) *readAPI {
	return &readAPI{cl: c}
}

// Walks every page and returns the ids in order plus the last response.
func walk(t *testing.T, a *readAPI, limit int32) ([]string, *aggregatorpb.ListHazardsResponse) {
	t.Helper()

	var (
		ids    []string
		cursor string
		resp   *aggregatorpb.ListHazardsResponse
	)

	for range 100 {
		var err error

		resp, err = a.ListHazards(context.Background(), &aggregatorpb.ListHazardsRequest{Limit: limit, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}

		for _, e := range resp.GetData() {
			ids = append(ids, e.GetHazardId())
		}

		if !resp.GetHasMore() {
			return ids, resp
		}

		cursor = resp.GetNextCursor()
	}

	t.Fatal("paging did not end")

	return nil, nil
}

func TestListMergesThreeNodesInOrderWithoutRepeatOrSkip(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	want := seed(t, c, 60)

	ids, resp := walk(t, apiOf(c), 7)
	if resp.GetPartial() {
		t.Fatalf("partial with all nodes up: %v", resp.GetUnavailableUnits())
	}

	if len(ids) != len(want) {
		t.Fatalf("got %d records, want %d", len(ids), len(want))
	}

	for i, h := range want {
		if ids[i] != h.HazardID {
			t.Fatalf("position %d: got %s want %s", i, ids[i], h.HazardID)
		}
	}
}

func TestListOneNodeDownIsServedFromReplicasNotPartial(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	want := seed(t, c, 40)
	f.down["a"] = true

	ids, resp := walk(t, apiOf(c), 9)
	if resp.GetPartial() || len(ids) != len(want) {
		t.Fatalf("partial=%v got %d want %d", resp.GetPartial(), len(ids), len(want))
	}
}

func TestListTwoNodesDownIsPartialAndNamesUnits(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	seed(t, c, 40)

	f.down["a"], f.down["b"] = true, true

	resp, err := apiOf(c).ListHazards(context.Background(), &aggregatorpb.ListHazardsRequest{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}

	if !resp.GetPartial() || len(resp.GetUnavailableUnits()) == 0 {
		t.Fatalf("want partial with units, got %v %v", resp.GetPartial(), resp.GetUnavailableUnits())
	}
}

func TestListSlowNodeAnswersWithinTheTimeout(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	want := seed(t, c, 20)
	f.slow["a"] = true

	start := time.Now()
	ids, _ := walk(t, apiOf(c), 50)

	if d := time.Since(start); d > 2*copyTimeout {
		t.Fatalf("took %v", d)
	}

	if len(ids) != len(want) {
		t.Fatalf("got %d want %d", len(ids), len(want))
	}
}

func TestListNoNodesIsPartialNot500(t *testing.T) {
	t.Parallel()

	c := newFake().listCluster()

	resp, err := apiOf(c).ListHazards(context.Background(), &aggregatorpb.ListHazardsRequest{})
	if err != nil || !resp.GetPartial() || len(resp.GetUnavailableUnits()) != numShards {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
}

func TestListRejectsBadInput(t *testing.T) {
	t.Parallel()

	a := apiOf(newFake().listCluster("a"))

	for _, req := range []*aggregatorpb.ListHazardsRequest{{Limit: -1}, {Cursor: "!!"}} {
		if _, err := a.ListHazards(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%v: got %v", req, err)
		}
	}

	if _, err := (&readAPI{}).ListHazards(context.Background(), &aggregatorpb.ListHazardsRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("no cluster: %v", err)
	}
}

func TestGetByIDReturnsPlacementAndFallsBackToReplica(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	h := seed(t, c, 5)[0]
	a := apiOf(c)

	resp, err := a.GetHazard(context.Background(), &aggregatorpb.GetHazardRequest{HazardId: h.HazardID})
	if err != nil || len(resp.GetPlacement()) != 2 || resp.GetEvent().GetHazardId() != h.HazardID {
		t.Fatalf("err=%v resp=%v", err, resp)
	}

	primary := owners(shardOf(h.HazardID), c.live())[0].node.name
	f.down[primary] = true

	if resp, err = a.GetHazard(context.Background(), &aggregatorpb.GetHazardRequest{HazardId: h.HazardID}); err != nil || resp.GetEvent() == nil {
		t.Fatalf("replica fallback failed: %v", err)
	}
}

func TestGetByIDNotFoundInvalidAndUnavailable(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b")
	a := apiOf(c)

	if _, err := a.GetHazard(context.Background(), &aggregatorpb.GetHazardRequest{HazardId: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("not found: %v", err)
	}

	if _, err := a.GetHazard(context.Background(), &aggregatorpb.GetHazardRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty id: %v", err)
	}

	f.down["a"], f.down["b"] = true, true

	if _, err := a.GetHazard(context.Background(), &aggregatorpb.GetHazardRequest{HazardId: "x"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("all down: %v", err)
	}
}

// Real SQL: equal ingested_at rows are ordered by hazard_id and a cursor resumes without repeat or skip. Skips without CANONICAL_DB_TEST_URL.
func TestListShardOnPagesEqualTimestampsOnRealDB(t *testing.T) { //nolint:paralleltest // shared database, fixed far-future timestamp
	s := testStore(t, 2)
	ctx := context.Background()
	at := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)

	var (
		want  []string
		shard int
	)

	for len(want) < 5 {
		h := hazard(sourcePVMBG, ref(t, s))
		h.IngestedAt = at

		if len(want) == 0 {
			shard = shardOf(h.HazardID)
		} else if shardOf(h.HazardID) != shard {
			continue
		}

		if _, err := s.upsert(ctx, h); err != nil {
			t.Fatal(err)
		}

		want = append(want, h.HazardID)
	}

	sort.Sort(sort.Reverse(sort.StringSlice(want)))

	first, err := listShardOn(ctx, s.db, shard, nil, 3)
	if err != nil {
		t.Fatal(err)
	}

	if len(first) != 3 {
		t.Fatalf("first page has %d rows, want 3", len(first))
	}

	last := first[2]

	rest, err := listShardOn(ctx, s.db, shard, &listPos{T: last.IngestedAt, ID: last.HazardID}, 3)
	if err != nil {
		t.Fatal(err)
	}

	var got []string

	for _, h := range append(first, rest...) {
		if h.IngestedAt.Equal(at) {
			got = append(got, h.HazardID)
		}
	}

	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
