package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const testAuditToken = "tok"

func auditAPIOf(c *cluster) *readAPI {
	return &readAPI{cl: c, auditTok: testAuditToken}
}

// Calls a route with the right token unless token is "-".
func auditGet(t *testing.T, a *readAPI, path, token string) (int, string) {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
	if token != "-" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	rec := httptest.NewRecorder()
	newMux(a).ServeHTTP(rec, req)

	return rec.Code, rec.Body.String()
}

// Walks every events page and returns the items plus the last page.
func auditWalk(t *testing.T, a *readAPI, query string) ([]HazardEvent, auditEventsResponse) {
	t.Helper()

	var (
		all    []HazardEvent
		cursor string
		resp   auditEventsResponse
	)

	for range 200 {
		code, body := auditGet(t, a, "/internal/audit/events?"+query+"&cursor="+url.QueryEscape(cursor), testAuditToken)
		if code != http.StatusOK {
			t.Fatalf("status %d: %s", code, body)
		}

		resp = auditEventsResponse{}

		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			t.Fatal(err)
		}

		all = append(all, resp.Items...)

		if resp.NextCursor == nil {
			return all, resp
		}

		cursor = *resp.NextCursor
	}

	t.Fatal("paging did not end")

	return nil, resp
}

func sumOf(ids []string) string {
	s := sha256.Sum256([]byte(strings.Join(ids, "\n")))

	return hex.EncodeToString(s[:])
}

func TestAuditRejectsBadTokenWithExactBody(t *testing.T) {
	t.Parallel()

	a := auditAPIOf(newFake().listCluster("a", "b", "c"))
	want := `{"error":{"code":"unauthorized","message":"token tidak valid"}}` + "\n"

	for _, tok := range []string{"-", "wrong", ""} {
		for _, p := range []string{"/internal/audit/events", "/internal/audit/checksum?source=BMKG"} {
			code, body := auditGet(t, a, p, tok)
			if code != http.StatusUnauthorized || body != want {
				t.Fatalf("%s token %q: %d %q", p, tok, code, body)
			}
		}
	}

	if code, _ := auditGet(t, a, "/internal/audit/events", testAuditToken); code != http.StatusOK {
		t.Fatalf("right token: %d", code)
	}
}

func TestAuditEventsPagesAscendingExactlyOnce(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	want := seed(t, c, 60)

	slices.Reverse(want)

	got, resp := auditWalk(t, auditAPIOf(c), "limit=7")
	if len(got) != len(want) {
		t.Fatalf("got %d items, want %d", len(got), len(want))
	}

	for i := range want {
		if got[i].HazardID != want[i].HazardID {
			t.Fatalf("item %d: got %s want %s", i, got[i].HazardID, want[i].HazardID)
		}
	}

	if resp.NextCursor != nil || resp.Partial || resp.UnavailableUnits == nil || len(resp.UnavailableUnits) != 0 {
		t.Fatalf("last page: %+v", resp)
	}

	if loc := got[0].IngestedAt.Location(); loc != time.UTC {
		t.Fatalf("time not UTC: %v", loc)
	}
}

func TestAuditEventsLastPageCursorIsJSONNull(t *testing.T) {
	t.Parallel()

	c := newFake().listCluster("a", "b", "c")
	seed(t, c, 3)

	_, body := auditGet(t, auditAPIOf(c), "/internal/audit/events", testAuditToken)
	if !strings.Contains(body, `"next_cursor":null`) || !strings.Contains(body, `"unavailable_units":[]`) {
		t.Fatal(body)
	}

	_, body = auditGet(t, auditAPIOf(newFake().listCluster("a", "b", "c")), "/internal/audit/events", testAuditToken)
	if !strings.Contains(body, `"items":[]`) {
		t.Fatal(body)
	}
}

func TestAuditEventsFilters(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	all := seed(t, c, 30)

	for i := range 5 {
		h := HazardEvent{HazardID: hazardID(sourcePVMBG, "p"+strconv.Itoa(i)), Source: sourcePVMBG, SourceRefID: "p" + strconv.Itoa(i), IngestedAt: all[0].IngestedAt.Add(time.Hour)}
		if _, err := c.upsert(context.Background(), h); err != nil {
			t.Fatal(err)
		}
	}

	a := auditAPIOf(c)

	pv, _ := auditWalk(t, a, "source=PVMBG&limit=2")
	if len(pv) != 5 {
		t.Fatalf("source filter: %d", len(pv))
	}

	since := all[0].IngestedAt.Add(time.Hour).Format(time.RFC3339)

	late, _ := auditWalk(t, a, "since="+url.QueryEscape(since))
	if len(late) != 5 {
		t.Fatalf("since is inclusive and filters: %d", len(late))
	}
}

func TestAuditEventsRejectsBadInput(t *testing.T) {
	t.Parallel()

	a := auditAPIOf(newFake().listCluster("a", "b", "c"))

	for _, q := range []string{"limit=0", "limit=1001", "limit=abc", "source=SATELIT", "since=yesterday", "cursor=%25%25"} {
		code, body := auditGet(t, a, "/internal/audit/events?"+q, testAuditToken)
		if code != http.StatusBadRequest || !strings.Contains(body, `"code":"bad_request"`) {
			t.Fatalf("%s: %d %s", q, code, body)
		}
	}

	code, _ := auditGet(t, a, "/internal/audit/checksum", testAuditToken)
	if code != http.StatusBadRequest {
		t.Fatalf("checksum without source: %d", code)
	}
}

func TestAuditNeedsCluster(t *testing.T) {
	t.Parallel()

	a := &readAPI{auditTok: testAuditToken}

	for _, p := range []string{"/internal/audit/events", "/internal/audit/checksum?source=BMKG"} {
		code, body := auditGet(t, a, p, testAuditToken)
		if code != http.StatusServiceUnavailable || !strings.Contains(body, `"error"`) {
			t.Fatalf("%s: %d %s", p, code, body)
		}
	}
}

type checksumResp struct {
	Source, Checksum, Algorithm string
	AsOf                        string `json:"as_of"`
	Count                       int
	Partial                     bool
	UnavailableUnits            []string `json:"unavailable_units"`
}

func checksumOf(t *testing.T, a *readAPI, source string) checksumResp {
	t.Helper()

	code, body := auditGet(t, a, "/internal/audit/checksum?source="+source, testAuditToken)
	if code != http.StatusOK {
		t.Fatalf("status %d: %s", code, body)
	}

	var r checksumResp
	if err := json.Unmarshal([]byte(body), &r); err != nil {
		t.Fatal(err)
	}

	return r
}

func TestAuditChecksumMatchesIndependentHashAndCountsOnce(t *testing.T) {
	t.Parallel()

	c := newFake().listCluster("a", "b", "c")
	all := seed(t, c, 60)

	ids := make([]string, 0, len(all))
	for _, h := range all {
		ids = append(ids, h.SourceRefID)
	}

	sort.Strings(ids)

	r := checksumOf(t, auditAPIOf(c), sourceBMKG)
	if r.Count != 60 || r.Checksum != sumOf(ids) || r.Algorithm != "sha256" || r.Partial || r.Source != sourceBMKG {
		t.Fatalf("%+v", r)
	}

	if _, err := time.Parse(time.RFC3339, r.AsOf); err != nil || !strings.HasSuffix(r.AsOf, "Z") {
		t.Fatalf("as_of %q", r.AsOf)
	}

	empty := checksumOf(t, auditAPIOf(c), sourcePVMBG)
	if empty.Count != 0 || empty.Checksum != sumOf(nil) {
		t.Fatalf("known empty source: %+v", empty)
	}
}

func TestAuditChecksumOneNodeDownIsNotPartialTwoDownIs(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	seed(t, c, 60)

	full := checksumOf(t, auditAPIOf(c), sourceBMKG)

	f.down["a"] = true

	one := checksumOf(t, auditAPIOf(c), sourceBMKG)
	if one.Partial || one.Checksum != full.Checksum || one.Count != 60 {
		t.Fatalf("one down: %+v", one)
	}

	f.down["b"] = true

	two := checksumOf(t, auditAPIOf(c), sourceBMKG)
	if !two.Partial || len(two.UnavailableUnits) == 0 || two.Checksum == full.Checksum || two.Count >= 60 {
		t.Fatalf("two down: %+v", two)
	}
}

func TestAuditSlowNodeStaysUnderTimeout(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.listCluster("a", "b", "c")
	seed(t, c, 20)

	f.slow["a"] = true

	start := time.Now()

	checksumOf(t, auditAPIOf(c), sourceBMKG)

	if d := time.Since(start); d > 2*copyTimeout {
		t.Fatalf("took %v", d)
	}
}

// Real SQL: ascending order, cursor, source and since filters, and byte-order ids. Skips without CANONICAL_DB_TEST_URL.
func TestAuditSQLOnRealDB(t *testing.T) { //nolint:paralleltest // shared database, fixed far-future timestamps
	s := testStore(t, 2)
	ctx := context.Background()
	at := time.Date(2098, 1, 1, 0, 0, 0, 0, time.UTC)

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

	sort.Strings(want)

	asc := listFilter{Asc: true, Source: sourcePVMBG, Since: &at}

	first, err := listShardOn(ctx, s.db, shard, nil, 3, asc)
	if err != nil || len(first) != 3 {
		t.Fatalf("first: %d %v", len(first), err)
	}

	rest, err := listShardOn(ctx, s.db, shard, &listPos{T: first[2].IngestedAt, ID: first[2].HazardID}, 10, asc)
	if err != nil {
		t.Fatal(err)
	}

	var got []string
	for _, h := range append(first, rest...) {
		got = append(got, h.HazardID)
	}

	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	after := at.Add(time.Second)

	for name, lf := range map[string]listFilter{"source": {Source: sourceBMKG, Since: &at}, "since": {Since: &after}} {
		if none, err := listShardOn(ctx, s.db, shard, nil, 10, lf); err != nil || len(none) != 0 {
			t.Fatalf("%s filter: %d %v", name, len(none), err)
		}
	}
}

func TestAuditIDsAreByteOrderedOnRealDB(t *testing.T) { //nolint:paralleltest // shared database
	s := testStore(t, 2)
	ctx := context.Background()
	tag := ref(t, s)

	t.Cleanup(func() {
		_, _ = s.db.ExecContext(ctx, `DELETE FROM hazard_events WHERE source_ref_id LIKE $1`, tag+"-%")
	})

	for _, r := range []string{"a", "B", "_x", "10", "9", "Z", "b"} {
		h := hazard(sourceBMKG, tag+"-"+r)
		if _, err := s.upsert(ctx, h); err != nil {
			t.Fatal(err)
		}
	}

	for shard := range numShards {
		ids, err := idsShardOn(ctx, s.db, shard, sourceBMKG)
		if err != nil {
			t.Fatal(err)
		}

		if !sort.StringsAreSorted(ids) {
			t.Fatalf("shard %d not in byte order: %v", shard, ids)
		}
	}
}
