package main

import (
	"bytes"
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
func auditWalk(t *testing.T, a *readAPI, query string) ([]auditItem, auditEventsResponse) {
	t.Helper()

	var (
		all    []auditItem
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

func TestAuditEmptyTokenRefusesEverything(t *testing.T) {
	t.Parallel()

	a := &readAPI{cl: newFake().listCluster("a", "b", "c")}

	for _, tok := range []string{"-", "", "anything"} {
		for _, p := range []string{"/internal/audit/events", "/internal/audit/checksum?source=BMKG"} {
			if code, body := auditGet(t, a, p, tok); code != http.StatusServiceUnavailable || !strings.Contains(body, "audit not configured") {
				t.Fatalf("%s token %q: %d", p, tok, code)
			}
		}
	}
}

// Real SQL: attributes come back exactly as stored, 5.0 stays 5.0 and an integer above 2^53 stays exact. Skips without CANONICAL_DB_TEST_URL.
func TestAuditAttributesAreRawOnRealDB(t *testing.T) { //nolint:paralleltest // shared database
	s := testStore(t, 2)
	ctx := context.Background()
	h := hazard(sourcePVMBG, ref(t, s))

	if _, err := s.upsert(ctx, h); err != nil {
		t.Fatal(err)
	}

	if _, err := s.db.ExecContext(ctx, `UPDATE hazard_events SET attributes = '{"x": 5.0, "big": 9007199254740993}'::jsonb WHERE hazard_id = $1::uuid`, h.HazardID); err != nil {
		t.Fatal(err)
	}

	var stored string
	if err := s.db.QueryRowContext(ctx, `SELECT attributes::text FROM hazard_events WHERE hazard_id = $1::uuid`, h.HazardID).Scan(&stored); err != nil {
		t.Fatal(err)
	}

	rows, err := listShardOn(ctx, s.db, shardOf(h.HazardID), nil, 1000, listFilter{Source: sourcePVMBG})
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range rows {
		if e.HazardID != h.HazardID {
			continue
		}

		out, err := json.Marshal(newAuditItem(e))
		if err != nil {
			t.Fatal(err)
		}

		var back struct {
			Attributes json.RawMessage `json:"attributes"`
		}

		if err := json.Unmarshal(out, &back); err != nil {
			t.Fatal(err)
		}

		var compact bytes.Buffer
		if err := json.Compact(&compact, []byte(stored)); err != nil {
			t.Fatal(err)
		}

		if string(back.Attributes) != compact.String() || !strings.Contains(stored, "5.0") || !strings.Contains(stored, "9007199254740993") {
			t.Fatalf("got %s, stored %s", back.Attributes, stored)
		}

		return
	}

	t.Fatal("row not listed")
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

	for _, q := range []string{"limit=0", "limit=1001", "limit=abc", "source=WILDFIRE", "since=yesterday", "cursor=%25%25"} {
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
}

func TestAuditKnownEmptySourcesAreEmptyNot400(t *testing.T) {
	t.Parallel()

	c := newFake().listCluster("a", "b", "c")
	seed(t, c, 5)

	for _, src := range []string{sourcePVMBG, sourceSatelit, sourceSensorPantai} {
		empty := checksumOf(t, auditAPIOf(c), src)
		if empty.Count != 0 || empty.Checksum != sumOf(nil) {
			t.Fatalf("known empty source %s: %+v", src, empty)
		}

		if code, body := auditGet(t, auditAPIOf(c), "/internal/audit/events?source="+src, testAuditToken); code != http.StatusOK || !strings.Contains(body, `"items":[]`) {
			t.Fatalf("events for empty source %s: %d %s", src, code, body)
		}
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

const locPrimary = "primary"

type locResp struct {
	HazardID  string          `json:"hazard_id"`
	Locations []auditLocation `json:"locations"`
	Partial   bool            `json:"partial"`

	UnavailableUnits []string `json:"unavailable_units"`
}

// Seeds one hazard on a 3 node fake and returns it with its two owners.
func locSetup(t *testing.T) (*fakeStore, *readAPI, string, []copyRef) {
	t.Helper()

	f := newFake()
	c := f.cluster("a", "b", "c")
	h := hz("LOC1")

	if _, err := c.upsert(t.Context(), h); err != nil {
		t.Fatal(err)
	}

	return f, auditAPIOf(c), h.HazardID, owners(shardOf(h.HazardID), c.live())
}

func locGet(t *testing.T, a *readAPI, id string) (int, locResp, string) {
	t.Helper()

	code, body := auditGet(t, a, "/internal/audit/location?hazard_id="+url.QueryEscape(id), testAuditToken)

	var r locResp

	if code == http.StatusOK {
		if err := json.Unmarshal([]byte(body), &r); err != nil {
			t.Fatal(err)
		}
	}

	return code, r, body
}

func TestAuditLocationBothCopies(t *testing.T) {
	t.Parallel()

	_, a, id, own := locSetup(t)

	code, r, body := locGet(t, a, id)
	if code != http.StatusOK || r.Partial || r.HazardID != id || len(r.Locations) != 2 {
		t.Fatalf("%d %s", code, body)
	}

	shard := shardOf(id)
	want := []auditLocation{
		{Node: own[0].node.name, Unit: "shard-" + strconv.Itoa(shard), Role: locPrimary},
		{Node: own[1].node.name, Unit: "shard-" + strconv.Itoa(shard) + "-r1", Role: "replica"},
	}

	if !slices.Equal(r.Locations, want) || r.Locations[0].Node == r.Locations[1].Node {
		t.Fatalf("got %+v want %+v", r.Locations, want)
	}

	if !strings.Contains(body, `"locations":[{"node":`) || !strings.Contains(body, `"partial":false`) || !strings.Contains(body, `"unavailable_units":[]`) {
		t.Fatalf("shape: %s", body)
	}
}

func TestAuditLocationUnknownAndBadIDs(t *testing.T) {
	t.Parallel()

	f, a, _, _ := locSetup(t)
	want := `{"error":{"code":"not_found","message":"hazard_id tidak ditemukan"}}` + "\n"

	code, _, body := locGet(t, a, hazardID("BMKG", "nothing"))
	if code != http.StatusNotFound || body != want {
		t.Fatalf("unknown uuid: %d %q", code, body)
	}

	before := f.hits["a"] + f.hits["b"] + f.hits["c"]

	for _, id := range []string{"hz-7f3a91", "not-a-uuid", "'; drop table x;--"} {
		if code, _, body := locGet(t, a, id); code != http.StatusNotFound || body != want {
			t.Fatalf("%q: %d %q", id, code, body)
		}
	}

	if after := f.hits["a"] + f.hits["b"] + f.hits["c"]; after != before {
		t.Fatalf("bad ids touched nodes: %d -> %d", before, after)
	}

	if code, _ := auditGet(t, a, "/internal/audit/location", testAuditToken); code != http.StatusBadRequest {
		t.Fatalf("missing param: %d", code)
	}
}

func TestAuditLocationSilentCopiesArePartialNeverAnError(t *testing.T) {
	t.Parallel()

	f, a, id, own := locSetup(t)
	shard := "shard-" + strconv.Itoa(shardOf(id))

	f.down[own[1].node.name] = true

	code, r, body := locGet(t, a, id)
	if code != http.StatusOK || !r.Partial || len(r.Locations) != 1 || r.Locations[0].Role != locPrimary || !slices.Equal(r.UnavailableUnits, []string{shard + "-r1"}) {
		t.Fatalf("replica down: %d %s", code, body)
	}

	f.down[own[1].node.name], f.down[own[0].node.name] = false, true

	code, r, body = locGet(t, a, id)
	if code != http.StatusOK || !r.Partial || len(r.Locations) != 1 || r.Locations[0].Role != "replica" || !slices.Equal(r.UnavailableUnits, []string{shard}) {
		t.Fatalf("primary down: %d %s", code, body)
	}
}

func TestAuditLocationBothSilentIsEmptyPartialNotAnError(t *testing.T) {
	t.Parallel()

	f, a, id, own := locSetup(t)
	shard := "shard-" + strconv.Itoa(shardOf(id))

	f.down[own[0].node.name], f.down[own[1].node.name] = true, true

	code, r, body := locGet(t, a, id)
	if code != http.StatusOK || !r.Partial || len(r.Locations) != 0 || !slices.Equal(r.UnavailableUnits, []string{shard, shard + "-r1"}) ||
		!strings.Contains(body, `"locations":[]`) {
		t.Fatalf("both down: %d %s", code, body)
	}
}

func TestAuditLocationUnknownIDWithOneSilentCopyIsPartial(t *testing.T) {
	t.Parallel()

	f, a, _, _ := locSetup(t)
	other := hazardID("BMKG", "other")
	oown := owners(shardOf(other), a.cl.live())
	f.down[oown[0].node.name] = true

	code, r, body := locGet(t, a, other)
	if code != http.StatusOK || !r.Partial || len(r.Locations) != 0 || len(r.UnavailableUnits) != 1 || r.UnavailableUnits[0] != oown[0].unit {
		t.Fatalf("unknown with one silent: %d %s", code, body)
	}
}

func TestAuditLocationCopyMissingRowIsOmittedNotPartial(t *testing.T) {
	t.Parallel()

	f, a, id, own := locSetup(t)

	delete(f.rows[own[1].node.name], id)

	code, r, body := locGet(t, a, id)
	if code != http.StatusOK || r.Partial || len(r.Locations) != 1 || r.Locations[0].Node != own[0].node.name {
		t.Fatalf("%d %s", code, body)
	}
}

func TestAuditLocationOneLiveNodeAndSlowNode(t *testing.T) {
	t.Parallel()

	f := newFake()
	one := f.cluster("a")
	h := hz("LOC2")

	if _, err := one.upsert(t.Context(), h); err != nil {
		t.Fatal(err)
	}

	if code, r, body := locGet(t, auditAPIOf(one), h.HazardID); code != http.StatusOK || r.Partial || len(r.Locations) != 1 || r.Locations[0].Role != locPrimary {
		t.Fatalf("one node: %d %s", code, body)
	}

	f2, a, id, own := locSetup(t)
	f2.slow[own[0].node.name] = true

	start := time.Now()

	code, r, _ := locGet(t, a, id)
	if code != http.StatusOK || !r.Partial || time.Since(start) > 2*copyTimeout {
		t.Fatalf("slow: %d partial=%v took %v", code, r.Partial, time.Since(start))
	}
}

func TestAuditLocationTokenRules(t *testing.T) {
	t.Parallel()

	_, a, id, _ := locSetup(t)
	p := "/internal/audit/location?hazard_id=" + id
	want := `{"error":{"code":"unauthorized","message":"token tidak valid"}}` + "\n"

	for _, tok := range []string{"-", "wrong", ""} {
		if code, body := auditGet(t, a, p, tok); code != http.StatusUnauthorized || body != want {
			t.Fatalf("token %q: %d %q", tok, code, body)
		}
	}

	empty := &readAPI{cl: a.cl}
	if code, body := auditGet(t, empty, p, "anything"); code != http.StatusServiceUnavailable || !strings.Contains(body, "audit not configured") {
		t.Fatalf("empty token: %d %s", code, body)
	}

	if code, body := auditGet(t, &readAPI{auditTok: testAuditToken}, p, testAuditToken); code != http.StatusServiceUnavailable {
		t.Fatalf("no cluster: %d %s", code, body)
	}
}

// Real SQL: the location route finds a row through the real read on two nodes sharing one database, and a non-uuid string would be a Postgres error. Skips without CANONICAL_DB_TEST_URL.
func TestAuditLocationOnRealDB(t *testing.T) { //nolint:paralleltest // shared database
	s := testStore(t, 2)
	h := hazard(sourcePVMBG, ref(t, s))

	if _, err := s.upsert(t.Context(), h); err != nil {
		t.Fatal(err)
	}

	c := newCluster(func() []*node { return []*node{{name: "a", db: s.db}, {name: "b", db: s.db}} })
	a := auditAPIOf(c)

	if code, r, body := locGet(t, a, h.HazardID); code != http.StatusOK || r.Partial || len(r.Locations) != 2 {
		t.Fatalf("known: %d %s", code, body)
	}

	if code, _, body := locGet(t, a, hazardID("BMKG", "real-db-missing")); code != http.StatusNotFound {
		t.Fatalf("unknown uuid: %d %s", code, body)
	}

	if _, err := getOn(t.Context(), s.db, "hz-7f3a91"); err == nil {
		t.Fatal("a non-uuid id should be a database error, which is why the handler checks the format first")
	}

	if code, _, _ := locGet(t, a, "hz-7f3a91"); code != http.StatusNotFound {
		t.Fatalf("non-uuid: %d", code)
	}
}
