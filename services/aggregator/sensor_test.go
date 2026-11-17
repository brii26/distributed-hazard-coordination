package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const testToken = "test-token"

// A fake sensor that serves its items in pages of pageSize and speaks the offset cursor of the real one.
type fakeSensor struct {
	mu       sync.Mutex
	items    []map[string]any
	pageSize int
	status   int // non-zero answers every observations call with this status
	calls    []string
	auth     []string
	srv      *httptest.Server
}

func newFakeSensor(t *testing.T, items ...map[string]any) *fakeSensor {
	t.Helper()

	f := &fakeSensor{items: items, pageSize: 1000}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)

	return f
}

func (f *fakeSensor) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.auth = append(f.auth, r.Header.Get("Authorization"))

	if r.Header.Get("Authorization") != "Bearer "+testToken {
		http.Error(w, `{"error":{"code":"unauthorized","message":"token tidak valid"}}`, http.StatusUnauthorized)

		return
	}

	if r.URL.Path == "/v1/stations" {
		_, _ = fmt.Fprint(w, `{"items":[
			{"station_id":"s-1","name":"Padang","province":"Sumatera Barat","latitude":-0.9492,"longitude":100.3543},
			{"station_id":"s-2","name":"Cilacap","province":"Jawa Tengah","latitude":-7.7,"longitude":109.0}]}`)

		return
	}

	f.calls = append(f.calls, r.URL.RawQuery)

	if f.status != 0 {
		http.Error(w, `{"error":{"code":"unavailable","message":"sensor sedang tidak melayani"}}`, f.status)

		return
	}

	off := 0

	if c := r.URL.Query().Get("cursor"); c != "" {
		b, _ := base64.StdEncoding.DecodeString(c)

		var cur struct{ O int }
		_ = json.Unmarshal(b, &cur)
		off = cur.O
	}

	end := min(off+f.pageSize, len(f.items))
	body := map[string]any{"generated_at": "2026-01-01T12:00:00Z", "items": f.items[off:end], "next_cursor": nil}

	if end > off {
		body["next_cursor"] = base64.StdEncoding.EncodeToString([]byte(`{"o":` + strconv.Itoa(end) + `}`))
	}

	_ = json.NewEncoder(w).Encode(body)
}

func (f *fakeSensor) add(items ...map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.items = append(f.items, items...)
}

func obs(id, station string, mutate ...func(map[string]any)) map[string]any {
	m := map[string]any{
		"observation_id": id, "station_id": station, "observed_at": "2026-01-01T11:00:00Z",
		"sea_level_anomaly_m": 0.4, "wave_height_m": 1.2, "quality": "ok", "critical": false,
	}
	for _, f := range mutate {
		f(m)
	}

	return m
}

func critical(m map[string]any) { m["critical"] = true }
func suspect(m map[string]any)  { m["quality"] = "suspect" }

// The saved cursor and quarantine of a test, standing in for the store. It outlives a client, like the database outlives a restart.
type memState struct {
	mu          sync.Mutex
	cursor      string
	saves       int
	quarantined []failedMessage
}

func (m *memState) client(url, token string) *sensorClient {
	return &sensorClient{
		http: &sourceClient{
			baseURL: url, timeout: 2 * time.Second, client: &http.Client{},
			auth: func(r *http.Request) error { r.Header.Set("Authorization", "Bearer "+token); return nil },
		},
		validator: newSensorValidator(nil),
		rejects:   newRejectCounter(),
		mapper:    &mapper{now: func() time.Time { return testGenerated }, newID: hazardID},
		loadCursor: func(context.Context) (string, error) {
			m.mu.Lock()
			defer m.mu.Unlock()

			return m.cursor, nil
		},
		saveCursor: func(_ context.Context, c string) error {
			m.mu.Lock()
			defer m.mu.Unlock()

			m.cursor = c
			m.saves++

			return nil
		},
		quarantine: func(_ context.Context, f failedMessage) error {
			m.mu.Lock()
			defer m.mu.Unlock()

			m.quarantined = append(m.quarantined, f)

			return nil
		},
	}
}

// What one ingest of the sensor pipeline did.
type sensorRun struct {
	stored    map[string]HazardEvent
	published []string
}

// Runs the real ingest stage with an in-memory upsert that reports a change only for new or different rows.
func (r *sensorRun) ingest(s *sensorClient) *ingest {
	return &ingest{
		name: sourceSensorPantai, mapBatch: s.mapBatch, afterBatch: s.afterBatch,
		upsert: func(_ context.Context, h HazardEvent) (bool, error) {
			old, ok := r.stored[h.SourceRefID]
			r.stored[h.SourceRefID] = h

			return !ok || fmt.Sprint(old.Attributes, *old.Severity) != fmt.Sprint(h.Attributes, *h.Severity), nil
		},
		publish:   func(_ context.Context, h HazardEvent, _ string) { r.published = append(r.published, h.SourceRefID) },
		retryBase: time.Millisecond, retryMax: time.Millisecond,
	}
}

// One poll followed by its ingest, like the two stages do.
func (r *sensorRun) poll(t *testing.T, s *sensorClient) pollResult {
	t.Helper()

	res, err := s.fetch(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	r.ingest(s).handle(context.Background(), res)

	return res
}

func newSensorRun() *sensorRun { return &sensorRun{stored: map[string]HazardEvent{}} }

func ids(res pollResult) []string {
	var out []string
	for _, r := range res.records {
		id, _ := r["observation_id"].(string)
		out = append(out, id)
	}

	return out
}

func TestSensorFetchPagination(t *testing.T) {
	tests := []struct {
		name     string
		n        int
		pageSize int
		pages    int // observations calls in one poll
	}{
		{"one page", 3, 10, 2}, // the second call returns nothing and a null cursor
		{"several pages", 7, 3, 4},
		{"nothing yet", 0, 10, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var items []map[string]any
			for i := range tc.n {
				items = append(items, obs(fmt.Sprintf("o-%d", i), "s-1"))
			}

			f := newFakeSensor(t, items...)
			f.pageSize = tc.pageSize
			st := &memState{}

			res, err := st.client(f.srv.URL, testToken).fetch(context.Background(), time.Time{})
			if err != nil {
				t.Fatal(err)
			}

			if len(res.records) != tc.n || len(res.raws) != tc.n {
				t.Fatalf("records = %d, raws = %d, want %d", len(res.records), len(res.raws), tc.n)
			}

			if len(f.calls) != tc.pages {
				t.Fatalf("calls = %d (%v), want %d", len(f.calls), f.calls, tc.pages)
			}

			if !strings.HasPrefix(f.calls[0], "limit=1000") || strings.Contains(f.calls[0], "cursor") {
				t.Fatalf("first call = %q, want limit=1000 and no cursor", f.calls[0])
			}

			if (tc.n > 0) != (res.nextCursor != "") {
				t.Fatalf("nextCursor = %q for %d items", res.nextCursor, tc.n)
			}
		})
	}
}

func TestSensorNullCursorKeepsOldCursor(t *testing.T) {
	f := newFakeSensor(t, obs("o-1", "s-1"))
	st := &memState{}
	s := st.client(f.srv.URL, testToken)
	run := newSensorRun()

	first := run.poll(t, s)
	second := run.poll(t, s) // nothing new: next_cursor is null

	if first.nextCursor == "" || second.nextCursor != "" || len(second.records) != 0 {
		t.Fatalf("first %q, second %q with %d records", first.nextCursor, second.nextCursor, len(second.records))
	}

	if st.cursor != first.nextCursor || s.cursor != first.nextCursor {
		t.Fatalf("saved %q, memory %q, want %q", st.cursor, s.cursor, first.nextCursor)
	}

	if last := f.calls[len(f.calls)-1]; !strings.Contains(last, "cursor=") {
		t.Fatalf("last call %q did not send the cursor", last)
	}
}

func TestSensorAuthAndErrors(t *testing.T) {
	tests := []struct {
		name    string
		token   string
		status  int
		wantErr string
	}{
		{"good token", testToken, 0, ""},
		{"wrong token", "nope", 0, "status 401"},
		{"service unavailable", testToken, http.StatusServiceUnavailable, "status 503"},
		{"server error", testToken, http.StatusInternalServerError, "status 500"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeSensor(t, obs("o-1", "s-1"))
			f.status = tc.status
			st := &memState{}
			s := st.client(f.srv.URL, tc.token)

			res, err := s.fetch(context.Background(), time.Time{})

			if tc.wantErr == "" {
				if err != nil || len(res.records) != 1 {
					t.Fatalf("err = %v, records = %d", err, len(res.records))
				}

				if !slices.Contains(f.auth, "Bearer "+testToken) {
					t.Fatalf("auth headers = %v", f.auth)
				}

				return
			}

			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}

			if strings.Contains(err.Error(), tc.token) {
				t.Fatalf("error leaks the token: %v", err)
			}

			if s.cursor != "" || st.saves != 0 {
				t.Fatalf("a failed poll moved the cursor: %q, %d saves", s.cursor, st.saves)
			}
		})
	}
}

func TestSensorOutageThenRecovery(t *testing.T) {
	f := newFakeSensor(t, obs("o-1", "s-1"), obs("o-2", "s-1"))
	st := &memState{}
	s := st.client(f.srv.URL, testToken)
	run := newSensorRun()

	f.status = http.StatusServiceUnavailable

	if _, err := s.fetch(context.Background(), time.Time{}); err == nil {
		t.Fatal("want an error during the outage")
	}

	f.status = 0
	f.add(obs("o-3", "s-1"))
	run.poll(t, s)

	if len(run.stored) != 3 {
		t.Fatalf("stored %d after recovery, want 3 (no gap)", len(run.stored))
	}
}

func TestSensorStationsLoadedOnce(t *testing.T) {
	f := newFakeSensor(t, obs("o-1", "s-1"))
	st := &memState{}
	s := st.client(f.srv.URL, testToken)
	run := newSensorRun()

	run.poll(t, s)
	run.poll(t, s)

	n := len(f.auth)

	// 1 stations call and 3 observations calls (the first poll pages twice).
	if n != 4 {
		t.Fatalf("sensor calls = %d, want 4", n)
	}
}

func TestSensorMapBatch(t *testing.T) {
	f := newFakeSensor(t,
		obs("o-1", "s-1"),
		obs("o-2", "s-2", critical),
		obs("o-3", "s-1", suspect),           // suspect is kept, with critical false
		obs("o-4", "s-1", suspect, critical), // critical stays whatever the quality
		obs("o-1", "s-1"),                    // exact duplicate in the same batch
		obs("o-9", "stn-unknown-99"),         // unknown station
		obs("o-8", "s-1", func(m map[string]any) { m["wave_height_m"] = 99.0 }),
		obs("o-0", "s-2", func(m map[string]any) { m["observed_at"] = "2025-12-31T23:59:00Z" }), // older, after newer ones
	)
	st := &memState{}
	s := st.client(f.srv.URL, testToken)

	res, err := s.fetch(context.Background(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	got, err := s.mapBatch(res)
	if err != nil {
		t.Fatal(err)
	}

	var gotIDs []string

	by := map[string]HazardEvent{}

	for _, h := range got {
		gotIDs = append(gotIDs, h.SourceRefID)
		by[h.SourceRefID] = h
	}

	if want := []string{"o-1", "o-2", "o-3", "o-4", "o-0"}; !slices.Equal(gotIDs, want) {
		t.Fatalf("mapped %v, want %v", gotIDs, want)
	}

	t.Run("fields", func(t *testing.T) {
		h := by["o-2"]

		if h.Source != "SENSOR_PANTAI" || h.HazardType != "COASTAL" || h.HazardID != hazardID("SENSOR_PANTAI", "o-2") {
			t.Fatalf("identity: %+v", h)
		}

		if *h.Severity != severityAwas || *h.AreaName != "Jawa Tengah" || *h.Latitude != -7.7 || *h.Longitude != 109.0 {
			t.Fatalf("severity %s area %s lat %v lon %v", *h.Severity, *h.AreaName, *h.Latitude, *h.Longitude)
		}

		if !h.OccurredAt.Equal(time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)) {
			t.Fatalf("occurred_at = %v", h.OccurredAt)
		}

		want := map[string]any{"wave_height_m": 1.2, "sea_level_anomaly_m": 0.4, "quality": "ok", "critical": true, "station_id": "s-2"}
		if fmt.Sprint(h.Attributes) != fmt.Sprint(want) {
			t.Fatalf("attributes = %v, want %v", h.Attributes, want)
		}
	})

	t.Run("severity follows the critical flag only", func(t *testing.T) {
		for id, want := range map[string]string{"o-1": severityNormal, "o-3": severityNormal, "o-4": severityAwas} {
			if sev := *by[id].Severity; sev != want {
				t.Errorf("%s severity = %s, want %s", id, sev, want)
			}
		}

		if by["o-3"].Attributes["quality"] != "suspect" {
			t.Errorf("suspect quality was not kept: %v", by["o-3"].Attributes)
		}
	})

	t.Run("quarantine", func(t *testing.T) {
		if len(st.quarantined) != 2 {
			t.Fatalf("quarantined %d, want 2", len(st.quarantined))
		}

		q := st.quarantined[0]
		if q.Source != sourceSensorPantai || q.Retryable || q.Status != failedParked || q.SourceRefID != "o-9" ||
			!strings.HasPrefix(q.Reason, reasonUnknownStation+": ") || !strings.Contains(string(q.Payload), "stn-unknown-99") {
			t.Fatalf("quarantine row: %+v", q)
		}

		snap := s.rejects.snapshot()
		if snap[reasonUnknownStation] != 1 || snap[reasonOutOfRange] != 1 {
			t.Fatalf("reject counter = %v", snap)
		}
	})
}

func TestSensorMapBatchNotAnObject(t *testing.T) {
	st := &memState{}
	s := st.client("http://unused", testToken)
	s.validator.setStations([]string{"s-1"})

	raw := []byte(`"surprise"`)
	got, err := s.mapBatch(pollResult{records: []map[string]any{decodeItem(raw)}, raws: [][]byte{raw}})

	if err != nil || len(got) != 0 || len(st.quarantined) != 1 || s.rejects.snapshot()[reasonBadType] != 1 {
		t.Fatalf("got %v, err %v, quarantined %d", got, err, len(st.quarantined))
	}
}

func TestSensorDuplicatesPublishOnce(t *testing.T) {
	f := newFakeSensor(t, obs("o-1", "s-1", critical), obs("o-1", "s-1", critical), obs("o-2", "s-1"))
	st := &memState{}
	s := st.client(f.srv.URL, testToken)
	run := newSensorRun()

	run.poll(t, s)

	// The sensor repeats o-1 in a later batch, and the whole first batch is read again after a reset.
	f.add(obs("o-1", "s-1", critical), obs("o-3", "s-1"))
	run.poll(t, s)

	st.cursor = ""
	run.poll(t, st.client(f.srv.URL, testToken))

	slices.Sort(run.published)

	if want := []string{"o-1", "o-2", "o-3"}; !slices.Equal(run.published, want) {
		t.Fatalf("published %v, want %v", run.published, want)
	}

	if len(run.stored) != 3 {
		t.Fatalf("stored %d, want 3", len(run.stored))
	}
}

func TestSensorRestartKeepsCursor(t *testing.T) {
	var items []map[string]any
	for i := range 5 {
		items = append(items, obs(fmt.Sprintf("o-%d", i), "s-1"))
	}

	f := newFakeSensor(t, items...)
	f.pageSize = 2
	st := &memState{}
	run := newSensorRun()

	first := run.poll(t, st.client(f.srv.URL, testToken))

	f.add(obs("o-5", "s-1"), obs("o-6", "s-2", critical))

	// A new client with empty memory, the same saved cursor: a restart in the middle of the stream.
	second := run.poll(t, st.client(f.srv.URL, testToken))

	if got := ids(second); !slices.Equal(got, []string{"o-5", "o-6"}) {
		t.Fatalf("after restart read %v (first read %v), want only the new items", got, ids(first))
	}

	if len(run.stored) != 7 {
		t.Fatalf("stored %d, want 7 with no gap and no duplicate", len(run.stored))
	}

	// 4 calls for the first client (three pages and the empty one), then the restarted one.
	if len(f.calls) < 5 || !strings.Contains(f.calls[4], "cursor=") {
		t.Fatalf("restarted client did not send the saved cursor: %v", f.calls)
	}
}

func TestSensorCursorOnlyAfterBatchStored(t *testing.T) {
	f := newFakeSensor(t, obs("o-1", "s-1"), obs("o-2", "s-1"))
	st := &memState{}
	s := st.client(f.srv.URL, testToken)

	res, err := s.fetch(context.Background(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	if st.saves != 0 {
		t.Fatal("cursor saved before the batch was stored")
	}

	s.afterBatch(context.Background(), res, false)

	if st.saves != 0 {
		t.Fatal("cursor saved for a batch that was not stored")
	}

	f.add(obs("o-3", "s-1"))

	res2, err := s.fetch(context.Background(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	// The failed batch is read again from the stored cursor, so nothing is skipped.
	if got := ids(res2); !slices.Equal(got, []string{"o-1", "o-2", "o-3"}) {
		t.Fatalf("after a failed batch read %v, want the whole range again", got)
	}

	s.afterBatch(context.Background(), res2, true)

	if st.saves != 1 {
		t.Fatalf("saves = %d, want the good batch saved once", st.saves)
	}
}

func TestSensorStaleBatchDoesNotSaveItsCursor(t *testing.T) {
	f := newFakeSensor(t, obs("o-1", "s-1"), obs("o-2", "s-1"))
	st := &memState{}
	s := st.client(f.srv.URL, testToken)

	queued, err := s.fetch(context.Background(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	s.newTerm()
	s.afterBatch(context.Background(), queued, true)

	if st.saves != 0 {
		t.Fatal("a batch queued before a new lease term saved its cursor")
	}

	next, err := s.fetch(context.Background(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	if got := ids(next); !slices.Equal(got, []string{"o-1", "o-2"}) {
		t.Fatalf("new term read %v, want a read from the stored cursor", got)
	}

	s.afterBatch(context.Background(), next, true)

	if st.saves != 1 {
		t.Fatalf("saves = %d, want 1", st.saves)
	}
}

func TestSensorNewTermReloadsTheStoredCursor(t *testing.T) {
	f := newFakeSensor(t, obs("o-1", "s-1"), obs("o-2", "s-1"))
	st := &memState{}
	s := st.client(f.srv.URL, testToken)

	first, _ := s.fetch(context.Background(), time.Time{})
	s.afterBatch(context.Background(), first, true)

	// Another instance leads for a while and moves the stored cursor on.
	st.cursor = first.nextCursor
	f.add(obs("o-3", "s-1"))

	s.newTerm()

	res, err := s.fetch(context.Background(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	if got := ids(res); !slices.Equal(got, []string{"o-3"}) {
		t.Fatalf("new term read %v, want only the item after the stored cursor", got)
	}
}

func TestSensorStationsFailureIsFailedPoll(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/stations" {
			_, _ = fmt.Fprint(w, `{"items":[]}`)

			return
		}

		t.Error("observations asked without stations")
	}))
	defer srv.Close()

	if _, err := (&memState{}).client(srv.URL, testToken).fetch(context.Background(), time.Time{}); err == nil {
		t.Fatal("want an error for an empty station list")
	}
}

func TestPantaiConfigPair(t *testing.T) {
	tests := []struct {
		url, token string
		wantErr    bool
	}{
		{"", "", false},
		{"http://s:8080", "t", false},
		{"http://s:8080", "", true},
		{"", "t", true},
	}

	for _, tc := range tests {
		t.Setenv("BMKG_BASE_URL", "http://bmkg")
		t.Setenv("BMKG_API_KEY", "k")
		t.Setenv("PANTAI_BASE_URL", tc.url)
		t.Setenv("PANTAI_AUTH_TOKEN", tc.token)

		_, err := loadPollConfig(newTokenManager("http://pvmbg", "id", "secret", "", "/tmp/x"))
		if (err != nil) != tc.wantErr {
			t.Errorf("url %q token %q: err = %v", tc.url, tc.token, err)
		}
	}
}

func TestSensorPipelineRegistered(t *testing.T) {
	st := &memState{}
	c := pollConfig{interval: time.Second, timeout: time.Second, cbThreshold: 3, cbOpenFor: time.Second, bmkgURL: "http://b", pvmbgURL: "http://p", pantai: st.client("http://s", "t")}

	names := func(ps []pipeline) []string {
		var out []string
		for _, p := range ps {
			out = append(out, p.poller.name)
		}

		return out
	}

	up := func(context.Context, HazardEvent) (bool, error) { return true, nil }
	pub := func(context.Context, HazardEvent, string) {}

	if got := names(newPipelines(c, up, pub)); !slices.Equal(got, []string{sourceBMKG, sourcePVMBG, sourceSensorPantai}) {
		t.Fatalf("pipelines = %v", got)
	}

	c.pantai = nil

	if got := names(newPipelines(c, up, pub)); !slices.Equal(got, []string{sourceBMKG, sourcePVMBG}) {
		t.Fatalf("pipelines without sensor = %v", got)
	}
}
