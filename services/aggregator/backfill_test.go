package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
)

const quakeRecord = `{"event_id":"%s","magnitude":5.1,"depth_km":10,"epicenter_lat":-6.2,"epicenter_lon":106.8,"region_name":"Jawa","occurred_at":"%s","potential_tsunami":false}`

func quakeJSON(id, at string) string { return fmt.Sprintf(quakeRecord, id, at) }

func TestAdminIngest(t *testing.T) {
	t.Parallel()

	var (
		mu  sync.Mutex
		got []string
	)

	api := &readAPI{adminTok: adminTestToken, replay: &replayer{}, backfill: &backfill{m: newMapper(), upsert: func(_ context.Context, h HazardEvent) (bool, error) {
		mu.Lock()
		defer mu.Unlock()

		got = append(got, h.SourceRefID)

		return true, nil
	}}}
	mux := newMux(api)

	post := func(body, token string) int {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/internal/admin/ingest", strings.NewReader(body))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}

		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		return rec.Code
	}

	good := "[" + quakeJSON("E1", "2004-12-26T00:58:53Z") + "," + quakeJSON("E2", "2011-03-11T05:46:23Z") + "]"

	for _, c := range []struct {
		name, body, token string
		want              int
	}{
		{"no token", good, "", http.StatusUnauthorized},
		{"not an array", `{"event_id":"E9"}`, adminTestToken, http.StatusBadRequest},
		{"empty", `[]`, adminTestToken, http.StatusBadRequest},
		{"too many", "[" + strings.Repeat("{},", backfillMaxRecords) + "{}]", adminTestToken, http.StatusBadRequest},
	} {
		if code := post(c.body, c.token); code != c.want {
			t.Errorf("%s: %d, want %d", c.name, code, c.want)
		}
	}

	if len(got) != 0 {
		t.Fatalf("refused requests wrote %v", got)
	}

	if code := post(good, adminTestToken); code != http.StatusOK {
		t.Fatalf("good batch: %d", code)
	}

	slices.Sort(got)

	if !slices.Equal(got, []string{"E1", "E2"}) {
		t.Fatalf("stored %v", got)
	}

	got = nil

	badTime := "[" + quakeJSON("E3", "yesterday") + "," + quakeJSON("E4", "2020-01-01T00:00:00Z") + "]"
	if code := post(badTime, adminTestToken); code != http.StatusOK {
		t.Fatalf("mixed batch: %d", code)
	}

	if !slices.Equal(got, []string{"E4"}) {
		t.Fatalf("a bad time must be skipped, stored %v", got)
	}
}

func TestUpsertCountsWritesPerNode(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.cluster("a", "b", "c")
	h := hz("E1")

	if _, err := c.upsert(t.Context(), h); err != nil {
		t.Fatal(err)
	}

	var b strings.Builder

	c.writeMetrics(&b)

	for _, o := range owners(shardOf(h.HazardID), c.live()) {
		if want := `store_node_writes_total{node="` + o.node.name + `"} 1`; !strings.Contains(b.String(), want) {
			t.Errorf("missing %q in\n%s", want, b.String())
		}
	}
}

func TestShardOwnerMetricSpreadsPrimaries(t *testing.T) {
	t.Parallel()

	r := &rebalancer{c: newFake().cluster("a", "b", "c")}

	var b strings.Builder

	r.writeMetrics(&b, true)

	primaryNodes := map[string]bool{}
	primaries, replicas := 0, 0

	for line := range strings.SplitSeq(b.String(), "\n") {
		if !strings.HasPrefix(line, "store_shard_owner{") {
			continue
		}

		switch {
		case strings.Contains(line, `role="primary"`):
			primaries++

			_, rest, _ := strings.Cut(line, `node="`)
			name, _, _ := strings.Cut(rest, `"`)
			primaryNodes[name] = true
		case strings.Contains(line, `role="replica"`):
			replicas++
		}
	}

	if primaries != numShards || replicas != numShards {
		t.Fatalf("primaries=%d replicas=%d, want %d each", primaries, replicas, numShards)
	}

	if len(primaryNodes) < 2 {
		t.Fatalf("every primary on one node: %v", primaryNodes)
	}
}
