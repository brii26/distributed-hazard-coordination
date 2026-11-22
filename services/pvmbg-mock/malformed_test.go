package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

const (
	goodReports = 4
	kindDeep    = "deep"
	kindUTF8    = "utf8"
	kindOff     = "off"
	bodyDeep    = `{"kind":"deep"}`
)

func (g *schemaRig) postMalformed(body string) (int, string) {
	g.t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/malformed", strings.NewReader(body))
	rec := httptest.NewRecorder()
	g.mux.ServeHTTP(rec, req)

	return rec.Code, strings.TrimSpace(rec.Body.String())
}

func (g *schemaRig) page() []byte {
	g.t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+g.token)

	rec := httptest.NewRecorder()
	g.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		g.t.Fatalf("status %d", rec.Code)
	}

	return rec.Body.Bytes()
}

func (g *schemaRig) fill(n int) {
	for range n {
		g.store.add(time.Now(), modeLegacy)
	}
}

// Calls fn for every byte of a JSON text that is outside a string.
func eachStructural(b []byte, fn func(i int, c byte)) {
	inString, escaped := false, false

	for i, c := range b {
		switch {
		case escaped:
			escaped = false
		case inString && c == '\\':
			escaped = true
		case c == '"':
			inString = !inString
		case !inString:
			fn(i, c)
		}
	}
}

func jsonDepth(b []byte) int {
	depth, deepest := 0, 0

	eachStructural(b, func(_ int, c byte) {
		switch c {
		case '[', '{':
			depth++
			deepest = max(deepest, depth)
		case ']', '}':
			depth--
		}
	})

	return deepest
}

// Splits an array of objects into its objects without decoding, so bodies with invalid UTF-8 can be split too.
func splitObjects(body []byte) [][]byte {
	var out [][]byte

	depth, start := 0, 0

	eachStructural(body, func(i int, c byte) {
		switch c {
		case '[', '{':
			if depth == 1 && c == '{' {
				start = i
			}

			depth++
		case ']', '}':
			depth--

			if depth == 1 {
				out = append(out, body[start:i+1])
			}
		}
	})

	return out
}

//nolint:paralleltest // flips the shared switch state
func TestMalformedAdminRoute(t *testing.T) {
	g := newSchemaRig(t)

	cases := []struct{ body, want string }{
		{bodyDeep, kindDeep},
		{`{"kind":" UTF8 "}`, kindUTF8},
		{`{"kind":"truncated"}`, "truncated"},
		{`{"kind":"huge"}`, "huge"},
		{`{"kind":"off"}`, kindOff},
		{bodyDeep, kindDeep},
		{``, kindOff},
		{bodyDeep, kindDeep},
		{`{}`, kindOff},
		{bodyDeep, kindDeep},
		{`garbage`, kindOff},
		{bodyDeep, kindDeep},
		{`{"kind":"nope"}`, kindOff},
	}

	for _, c := range cases {
		code, got := g.postMalformed(c.body)
		if want := `{"malformed":"` + c.want + `"}`; code != http.StatusOK || got != want || currentMalformed().name() != c.want {
			t.Errorf("body %q: %d %s, state %s, want %s", c.body, code, got, currentMalformed().name(), c.want)
		}
	}
}

//nolint:paralleltest // flips the shared switch state
func TestMalformedOffIsTheNormalBody(t *testing.T) {
	g := newSchemaRig(t)
	g.fill(goodReports)

	normal := g.page()

	g.postMalformed(bodyDeep)
	g.postMalformed(`{"kind":"off"}`)

	if !bytes.Equal(g.page(), normal) {
		t.Error("off must serve exactly the normal body")
	}
}

//nolint:paralleltest // flips the shared switch state
func TestMalformedTruncatedKeepsTheEarlierRecordsWhole(t *testing.T) {
	g := newSchemaRig(t)
	g.fill(goodReports)
	g.postMalformed(`{"kind":"truncated"}`)

	body := g.page()
	if json.Valid(body) || body[0] != '[' {
		t.Fatalf("want a cut array, got valid=%v start=%q", json.Valid(body), body[:1])
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	if _, err := dec.Token(); err != nil {
		t.Fatal(err)
	}

	for i := range goodReports - 1 {
		var r VolcanicReport
		if err := dec.Decode(&r); err != nil || r.ReportID == "" {
			t.Fatalf("record %d should be whole: %v", i, err)
		}
	}

	var last VolcanicReport
	if dec.Decode(&last) == nil {
		t.Error("the last record should be cut")
	}
}

//nolint:paralleltest // flips the shared switch state
func TestMalformedOneBadRecordAmongGoodOnes(t *testing.T) {
	cases := []struct {
		kind, id string
		wantBad  func(body, bad []byte) string
	}{
		{kindDeep, "MALFORMED-deep", func(body, bad []byte) string {
			if !json.Valid(body) || jsonDepth(bad) <= 64 {
				return "want valid JSON with a record deeper than 64"
			}

			return ""
		}},
		{kindUTF8, "MALFORMED-utf8", func(body, _ []byte) string {
			if utf8.Valid(body) {
				return "want invalid UTF-8 in the body"
			}

			return ""
		}},
		{"huge", "MALFORMED-huge", func(body, bad []byte) string {
			if len(bad) <= 1<<20 || len(body) >= 8<<20 {
				return "want one record above 1 MiB in a body under 8 MiB"
			}

			return ""
		}},
	}

	for _, c := range cases {
		g := newSchemaRig(t)
		g.fill(goodReports)
		g.postMalformed(`{"kind":"` + c.kind + `"}`)

		body := g.page()
		items := splitObjects(body)

		if len(items) != goodReports+1 {
			t.Fatalf("%s: want %d records, got %d", c.kind, goodReports+1, len(items))
		}

		if !bytes.Contains(items[goodReports/2], []byte(c.id)) {
			t.Fatalf("%s: the bad record should sit after the first half", c.kind)
		}

		for i, it := range items {
			if i != goodReports/2 && !bytes.Contains(it, []byte(`"volcano_id"`)) {
				t.Errorf("%s: record %d should be a normal report", c.kind, i)
			}
		}

		if msg := c.wantBad(body, items[goodReports/2]); msg != "" {
			t.Errorf("%s: %s", c.kind, msg)
		}
	}
}

//nolint:paralleltest // flips the shared switch state
func TestMalformedWithNoReportsStillServesSomethingBad(t *testing.T) {
	for _, kind := range []string{"truncated", kindDeep, kindUTF8, "huge"} {
		g := newSchemaRig(t)
		g.postMalformed(`{"kind":"` + kind + `"}`)

		if body := g.page(); len(body) == 0 || body[0] != '[' {
			t.Errorf("%s: empty page gave %q", kind, body)
		}
	}
}
