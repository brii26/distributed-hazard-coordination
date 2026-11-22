package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const burstFlag = "--burst"

var testNow = time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

func writeFile(t *testing.T, name, content string) string {
	t.Helper()

	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	return p
}

func TestLoadCSVWithMappingAndRejects(t *testing.T) {
	t.Parallel()

	csv := "Date,Time,Latitude,Longitude,Depth,Mag,Place\n" +
		"2004-12-26,00:58:53,3.3,95.9,30,9.1,Aceh\n" +
		"2004-12-26,00:58:53,3.3,95.9,30,9.1,Aceh again\n" + // same quake twice
		"2010-10-25,14:42:22,-3.5,100.1,20,7.8,Mentawai\n" +
		"yesterday,10:00:00,1,1,1,5,bad time\n" +
		"2011-03-11,05:46:23,95,100,10,9.0,bad latitude\n" +
		"2999-01-01,00:00:00,1,1,1,5,future\n"

	cols, err := parseColumns("time=Date+Time,lat=Latitude,lon=Longitude,depth=Depth,mag=Mag,region=Place")
	if err != nil {
		t.Fatal(err)
	}

	evs, rep, err := load(writeFile(t, "q.csv", csv), cols, testNow)
	if err != nil {
		t.Fatal(err)
	}

	if rep.Read != 6 || rep.Kept != 2 || rep.Duplicates != 1 || total(rep.Rejected) != 3 {
		t.Fatalf("report %+v", rep)
	}

	if evs[0].OccurredAt != "2004-12-26T00:58:53Z" || evs[0].RegionName != "Aceh" || !strings.HasPrefix(evs[0].EventID, "q-") {
		t.Fatalf("first event %+v", evs[0])
	}

	again, _, _ := load(writeFile(t, "q2.csv", csv), cols, testNow)
	if again[0].EventID != evs[0].EventID {
		t.Fatal("the derived id must be the same for the same quake")
	}
}

func TestLoadJSONKeepsGivenID(t *testing.T) {
	t.Parallel()

	js := `[{"event_id":"E7","occurred_at":"2020-05-05T05:05:05Z","epicenter_lat":-6.2,"epicenter_lon":106.8,"magnitude":5.5}]`

	evs, rep, err := load(writeFile(t, "q.json", js), defaultColumns(), testNow)
	if err != nil || rep.Kept != 1 || evs[0].EventID != "E7" {
		t.Fatalf("%v %+v %+v", err, rep, evs)
	}
}

func TestMissingColumnIsNamed(t *testing.T) {
	t.Parallel()

	_, _, err := load(writeFile(t, "q.csv", "when,lat,lon,mag\n2020-01-01,1,1,5\n"), defaultColumns(), testNow)
	if err == nil || !strings.Contains(err.Error(), `"occurred_at"`) {
		t.Fatalf("want the missing column named, got %v", err)
	}

	if _, err := parseColumns("colour=red"); err == nil {
		t.Fatal("an unknown field in --map must be refused")
	}
}

func TestBurstStaysInTheLast24Hours(t *testing.T) {
	t.Parallel()

	box := [4]float64{-10, 2, 94, 112}
	evs := burst(200, box, testNow, rand.New(rand.NewPCG(1, 2))) //nolint:gosec // a fixed seed keeps the test repeatable
	ids := map[string]bool{}

	for _, e := range evs {
		at, ok := parseTime(e.OccurredAt)
		if !ok || at.After(testNow.Add(-time.Minute)) || at.Before(testNow.Add(-24*time.Hour)) {
			t.Fatalf("time %s is outside the 24 hours before now", e.OccurredAt)
		}

		if e.EpicenterLat < box[0] || e.EpicenterLat > box[1] || e.EpicenterLon < box[2] || e.EpicenterLon > box[3] {
			t.Fatalf("position outside the box: %+v", e)
		}

		if !strings.HasPrefix(e.EventID, "burst-") || ids[e.EventID] {
			t.Fatalf("id %q must start with burst- and be unique", e.EventID)
		}

		ids[e.EventID] = true
	}
}

func TestSendBatchesWithTokenAndStopsOnRefusal(t *testing.T) {
	t.Parallel()

	var sizes []int

	refuse := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, "no", http.StatusUnauthorized)

			return
		}

		var evs []event

		_ = json.NewDecoder(r.Body).Decode(&evs)

		sizes = append(sizes, len(evs))

		if refuse && len(sizes) == 2 {
			http.Error(w, "busy", http.StatusServiceUnavailable)

			return
		}

		_, _ = io.WriteString(w, `{"received":1}`)
	}))
	defer srv.Close()

	evs := burst(25, [4]float64{-10, 2, 94, 112}, testNow, rand.New(rand.NewPCG(3, 4))) //nolint:gosec // a fixed seed keeps the test repeatable
	s := &sender{url: srv.URL, token: "tok", client: srv.Client(), batch: 10, out: io.Discard}

	if err := s.send(context.Background(), evs); err != nil {
		t.Fatal(err)
	}

	if len(sizes) != 3 || sizes[0] != 10 || sizes[2] != 5 {
		t.Fatalf("batches %v, want 10, 10, 5", sizes)
	}

	sizes, refuse = nil, true

	if err := s.send(context.Background(), evs); err == nil || !strings.Contains(err.Error(), "503") || len(sizes) != 2 {
		t.Fatalf("must stop at the refused batch: err=%v sizes=%v", err, sizes)
	}

	bad := &sender{url: srv.URL, token: "wrong", client: srv.Client(), batch: 10, out: io.Discard}
	if err := bad.send(context.Background(), evs); err == nil {
		t.Fatal("a wrong token must fail")
	}
}

// fakeAudit is an Aggregator that stores what it is sent and lists it, with live events beside it, in pages of seven.
type fakeAudit struct {
	stored     []string
	live       []string
	drop       bool // accept batches but store nothing
	hideRecent bool // answer an empty list when the request has a since
}

func (f *fakeAudit) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/internal/audit/events" {
		var evs []event

		_ = json.NewDecoder(r.Body).Decode(&evs)

		if !f.drop {
			for _, e := range evs {
				f.stored = append(f.stored, e.EventID)
			}
		}

		_, _ = io.WriteString(w, `{"received":1}`)

		return
	}

	if r.URL.Query().Get("source") != "BMKG" || r.Header.Get("Authorization") != "Bearer aud" {
		http.Error(w, "no", http.StatusBadRequest)

		return
	}

	all := append(append([]string{}, f.live...), f.stored...)
	if f.hideRecent && r.URL.Query().Get("since") != "" {
		all = nil
	}

	from, _ := strconv.Atoi(r.URL.Query().Get("cursor"))
	to := min(from+7, len(all))

	var items []string
	for _, id := range all[from:to] {
		items = append(items, `{"source_ref_id":"`+id+`"}`)
	}

	next := "null"
	if to < len(all) {
		next = `"` + strconv.Itoa(to) + `"`
	}

	_, _ = io.WriteString(w, `{"items":[`+strings.Join(items, ",")+`],"next_cursor":`+next+`,"partial":false}`)
}

func TestRunChecksEverySentID(t *testing.T) {
	f := &fakeAudit{live: []string{"live-1", "live-2"}}
	srv := httptest.NewServer(f)

	defer srv.Close()

	t.Setenv("ADMIN_TOKEN", "adm")
	t.Setenv("AUDIT_TOKEN", "aud")

	burstRun := func(n string) (int, string) {
		var out, errOut bytes.Buffer

		code := run([]string{burstFlag, n, "--batch", "10", "--url", srv.URL}, &out, &errOut)

		return code, out.String() + errOut.String()
	}

	// Thirty ids over several pages, with live events listed beside them.
	if code, out := burstRun("30"); code != 0 || !strings.Contains(out, "missing=0") {
		t.Fatalf("exit %d\n%s", code, out)
	}

	// The recent window shows nothing, so the full read has to find the ids.
	f.hideRecent = true

	if code, out := burstRun("10"); code != 0 || !strings.Contains(out, "PASS") {
		t.Fatalf("the full read must back up the recent window, exit %d\n%s", code, out)
	}

	f.hideRecent, f.drop = false, true

	if code, out := burstRun("30"); code != 1 || !strings.Contains(out, "missing=30") {
		t.Fatalf("lost ids must fail even with live events listed, exit %d\n%s", code, out)
	}
}

func TestRunDryRunSendsNothing(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer

	p := writeFile(t, "q.json", `[{"event_id":"E1","occurred_at":"2020-05-05T05:05:05Z","epicenter_lat":1,"epicenter_lon":1,"magnitude":5}]`)

	if code := run([]string{"--file", p, burstFlag, "5", "--dry-run"}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}

	if !strings.Contains(out.String(), "unique_rows=1") || !strings.Contains(out.String(), "5 burst events") {
		t.Fatalf("output: %s", out.String())
	}

	if code := run([]string{"--file", p}, &out, &errOut); code != 2 {
		t.Fatalf("sending without --url must exit 2, got %d", code)
	}

	if code := run(nil, &out, &errOut); code != 2 {
		t.Fatalf("no input must exit 2, got %d", code)
	}
}
