package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type schemaRig struct {
	t      *testing.T
	tokens *tokenStore
	store  *reportStore
	mux    http.Handler
	token  string
}

func newSchemaRig(t *testing.T) *schemaRig {
	t.Helper()
	resetSwitches(t)

	tokens := newTestStore()
	store := &reportStore{}

	pair, errCode := tokens.exchange(grantAuthCode, testCode, "")
	if errCode != "" {
		t.Fatal(errCode)
	}

	return &schemaRig{t: t, tokens: tokens, store: store, mux: newMux(tokens, store, 0, 0), token: pair.access}
}

func (g *schemaRig) post(body string) *httptest.ResponseRecorder {
	g.t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/schema-version", strings.NewReader(body))
	rec := httptest.NewRecorder()
	g.mux.ServeHTTP(rec, req)

	return rec
}

// Adds one report in the mode the admin calls have set so far.
func (g *schemaRig) generate() VolcanicReport {
	return g.store.add(time.Now(), currentSchema())
}

// The JSON of one report as a generic map, to check which fields are present and their type.
func asMap(t *testing.T, r VolcanicReport) map[string]any {
	t.Helper()

	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}

	m := map[string]any{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}

	return m
}

func has(m map[string]any, k string) bool { _, ok := m[k]; return ok }

//nolint:paralleltest // flips the shared switch state
func TestIncompatibleChanges(t *testing.T) {
	cases := []struct {
		body                    string
		rename, retype, missing bool
	}{
		{`{"version":2,"change":"rename"}`, true, false, false},
		{`{"version":2,"change":"type"}`, false, true, false},
		{`{"version":2,"change":"missing"}`, false, false, true},
		{`{"version":2}`, true, true, true},
		{`{"version":2,"change":"all"}`, true, true, true},
		{`{"version":2,"change":null}`, true, true, true},
		{`{"version":3,"change":"type"}`, false, true, false},
		{`{"version":3}`, true, true, true},
		{`{"version":2,"change":"Rename"}`, true, false, false},
		{`{"version":2,"change":"  TYPE "}`, false, true, false},
		{`{"version":2,"change":""}`, true, true, true},
		{`{"version":2,"change":"bogus"}`, true, true, true},
		{`{"version":2,"change":5}`, true, true, true},
		{`{"version":2,"change":["rename"]}`, true, true, true},
	}

	for _, tc := range cases {
		g := newSchemaRig(t)

		if rec := g.post(tc.body); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"incompatible"`) {
			t.Fatalf("%s: got %d %s", tc.body, rec.Code, rec.Body.String())
		}

		r := g.generate()
		m := asMap(t, r)

		if has(m, "eruption_count") != tc.rename || has(m, "eruption_count_24h") == tc.rename {
			t.Fatalf("%s: rename wrong: %v", tc.body, m)
		}

		if _, isString := m["ash_column_height_m"].(string); isString != tc.retype {
			t.Fatalf("%s: type wrong: %v", tc.body, m)
		}

		if has(m, "volcano_id") == tc.missing {
			t.Fatalf("%s: missing wrong: %v", tc.body, m)
		}

		if has(m, "confidence_level") || !has(m, "report_id") || !has(m, "alert_level") || !has(m, "reported_at") {
			t.Fatalf("%s: other fields must be untouched: %v", tc.body, m)
		}
	}
}

//nolint:paralleltest // flips the shared switch state
func TestIncompatibleStringValue(t *testing.T) {
	g := newSchemaRig(t)
	g.post(`{"version":3,"change":"type"}`)

	r := g.store.add(time.Now(), currentSchema())
	r.AshColumnHeightM = 1200.5

	if got := asMap(t, r)["ash_column_height_m"]; got != "1200.5" {
		t.Fatalf("want \"1200.5\", got %v", got)
	}

	r.AshColumnHeightM = 1200

	if got := asMap(t, r)["ash_column_height_m"]; got != "1200" {
		t.Fatalf("want \"1200\", got %v", got)
	}
}

//nolint:paralleltest // flips the shared switch state
func TestSwitchKeepsOldReportsAndGoesBack(t *testing.T) {
	g := newSchemaRig(t)

	old := g.generate()
	g.post(`{"version":3}`)
	broken := g.generate()
	g.post(`{"version":1}`)
	fresh := g.generate()

	if m := asMap(t, old); !has(m, "volcano_id") || !has(m, "eruption_count_24h") {
		t.Fatalf("report from before the switch changed shape: %v", m)
	}

	if m := asMap(t, broken); has(m, "volcano_id") || !has(m, "eruption_count") {
		t.Fatalf("report made while incompatible changed shape back: %v", m)
	}

	if m := asMap(t, fresh); !has(m, "volcano_id") || !has(m, "eruption_count_24h") || has(m, "eruption_count") {
		t.Fatalf("version 1 must restore the legacy shape: %v", m)
	}
}

//nolint:paralleltest // flips the shared switch state
func TestVersionTwoIsIncompatibleAndEmptyBodyStaysExtended(t *testing.T) {
	g := newSchemaRig(t)

	if rec := g.post(""); !strings.Contains(rec.Body.String(), wantExtended) || strings.Contains(rec.Body.String(), "change") {
		t.Fatalf("empty body must give extended: %s", rec.Body.String())
	}

	if m := asMap(t, g.generate()); !has(m, "confidence_level") || !has(m, "volcano_id") {
		t.Fatalf("extended report: %v", m)
	}

	if rec := g.post(bodyVersion2); !strings.Contains(rec.Body.String(), `"incompatible"`) {
		t.Fatalf("version 2 must be incompatible: %s", rec.Body.String())
	}

	if m := asMap(t, g.generate()); has(m, "confidence_level") || has(m, "volcano_id") || !has(m, "eruption_count") {
		t.Fatalf("incompatible report: %v", m)
	}
}

//nolint:paralleltest // flips the shared switch state
func TestEmptyBodyToggle(t *testing.T) {
	g := newSchemaRig(t)

	for _, want := range []string{wantExtended, wantLegacy} {
		if rec := g.post(""); !strings.Contains(rec.Body.String(), `"`+want+`"`) {
			t.Fatalf("want %s, got %s", want, rec.Body.String())
		}
	}

	g.post(`{"version":3,"change":"rename"}`)

	if rec := g.post(""); !strings.Contains(rec.Body.String(), `"`+wantLegacy+`"`) {
		t.Fatalf("empty body from incompatible must go legacy: %s", rec.Body.String())
	}
}

//nolint:paralleltest // flips the shared switch state
//nolint:paralleltest // flips the shared switch state
func TestBadChangeFallsBackToAllAndWarns(t *testing.T) {
	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	g := newSchemaRig(t)

	for _, body := range []string{`{"version":2,"change":"bogus"}`, `{"version":2,"change":5}`, `{"version":2,"change":["rename"]}`, `{"version":2,"change":""}`} {
		buf.Reset()

		rec := g.post(body)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"change":"all"`) {
			t.Fatalf("%s: want 200 with all, got %d %s", body, rec.Code, rec.Body.String())
		}

		if !strings.Contains(buf.String(), `"level":"warn"`) || !strings.Contains(buf.String(), "using all") {
			t.Fatalf("%s: want a warning, got %s", body, buf.String())
		}
	}

	if m := asMap(t, g.generate()); has(m, "volcano_id") || has(m, "eruption_count_24h") {
		t.Fatalf("fallback must apply all three: %v", m)
	}
}

// A change on a body that does not switch to incompatible is ignored and logged, whatever the version.
//
//nolint:paralleltest // flips the shared switch state
func TestChangeOnOtherVersionsIsLogged(t *testing.T) {
	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	g := newSchemaRig(t)

	for _, body := range []string{`{"version":1,"change":"rename"}`, `{"version":0,"change":"rename"}`, `{"change":"rename"}`} {
		buf.Reset()

		if rec := g.post(body); rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), wantBroken) {
			t.Fatalf("%s: %d %s", body, rec.Code, rec.Body.String())
		}

		if !strings.Contains(buf.String(), "schema-version change ignored") {
			t.Fatalf("%s: want an ignored line, got %s", body, buf.String())
		}
	}
}

//nolint:paralleltest // flips the shared switch state
func TestResponseNamesTheChange(t *testing.T) {
	g := newSchemaRig(t)

	for body, want := range map[string]string{
		`{"version":3,"change":"type"}`: "type",
		`{"version":3}`:                 "all",
	} {
		rec := g.post(body)

		var got map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got["change"] != want || got["schema_version"] != wantBroken {
			t.Fatalf("%s: got %s", body, rec.Body.String())
		}
	}
}

// The whole path over HTTP: switch, then read the reports with a bearer token, no restart.
//
//nolint:paralleltest // flips the shared switch state
func TestIncompatibleOverHTTP(t *testing.T) {
	g := newSchemaRig(t)
	oldID := g.generate().ReportID
	g.post(`{"version":3}`)
	g.generate()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+g.token)

	rec := httptest.NewRecorder()
	g.mux.ServeHTTP(rec, req)

	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || rec.Code != http.StatusOK || len(got) != 2 {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}

	for _, m := range got {
		broken := !has(m, "volcano_id")
		if (m["report_id"] == oldID) == broken {
			t.Fatalf("old report must keep its shape and the new one must not: %v", m)
		}

		if broken != has(m, "eruption_count") {
			t.Fatalf("new report must carry all three changes: %v", m)
		}
	}
}
