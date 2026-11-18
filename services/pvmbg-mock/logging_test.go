package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

//nolint:paralleltest // swaps the global logger
func TestPlainLogLinesAreJSONWithTheRequiredFields(t *testing.T) {
	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	log.Printf("pvmbg-mock listening on :%d", 1234)

	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("log line is not one JSON object: %q", buf.String())
	}

	for _, f := range []string{"timestamp", "level", "service", "correlation_id", "message"} {
		if _, ok := line[f]; !ok {
			t.Fatalf("log line %v has no %q", line, f)
		}
	}

	if line["service"] != serviceName || line["correlation_id"] != "-" || line["message"] != "pvmbg-mock listening on :1234" || line["level"] != "info" {
		t.Fatalf("unexpected line %v", line)
	}
}

//nolint:paralleltest // swaps the global logger and flips the schema switch
func TestAdminCallIsLoggedWithItsBody(t *testing.T) {
	resetSwitches(t)

	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	long := strings.Repeat("a", 2048)
	cases := []struct {
		body, wantBody string
		wantVersion    any
		wantCut        bool
	}{
		{"", "", nil, false},
		{bodyVersion2, bodyVersion2, float64(2), false},
		{"not json\nfake line", "not json\nfake line", nil, false},
		{long, long[:maxAdminBody], nil, true},
	}

	for _, tc := range cases {
		buf.Reset()

		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/schema-version", strings.NewReader(tc.body))
		req.Header.Set(correlationHeader, "t-277")
		newMux(newTestStore(), &reportStore{}, 0, 0).ServeHTTP(httptest.NewRecorder(), req)

		// The admin line comes first, the request line after it. A newline in the body must stay inside the field.
		lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
		if len(lines) != 2 {
			t.Fatalf("expected 2 log lines, got %d: %q", len(lines), buf.String())
		}

		var line map[string]any
		if err := json.Unmarshal(lines[0], &line); err != nil {
			t.Fatalf("not JSON: %q", lines[0])
		}

		if line["message"] != "schema-version changed" || line["correlation_id"] != "t-277" ||
			line["method"] != http.MethodPost || line["path"] != "/admin/schema-version" ||
			line["body"] != tc.wantBody || line["body_truncated"] != tc.wantCut || line["version"] != tc.wantVersion {
			t.Fatalf("unexpected admin line %v", line)
		}
	}
}

//nolint:paralleltest // swaps the global logger
func TestRequestLogCarriesTheAggregatorsCorrelationID(t *testing.T) {
	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	h := withRequestLog(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }))

	for _, id := range []string{"cycle-1", ""} {
		buf.Reset()

		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/volcanic-reports", nil)
		if id != "" {
			req.Header.Set(correlationHeader, id)
		}

		h.ServeHTTP(httptest.NewRecorder(), req)

		var line map[string]any
		if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
			t.Fatalf("not JSON: %q", buf.String())
		}

		want := id
		if want == "" {
			want = "-"
		}

		if line["message"] != "request" || line["correlation_id"] != want || line["status"] != float64(http.StatusTeapot) {
			t.Fatalf("expected a request line with correlation_id %q, got %v", want, line)
		}
	}
}

//nolint:paralleltest // swaps the global logger
func TestRequestLogCarriesTheCallerAddress(t *testing.T) {
	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	h := withRequestLog(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/volcanic-reports", nil)
	req.RemoteAddr = "10.244.2.9:5555"
	h.ServeHTTP(httptest.NewRecorder(), req)

	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("not JSON: %q", buf.String())
	}

	if line["remote_addr"] != "10.244.2.9" {
		t.Fatalf("expected remote_addr 10.244.2.9 without the port, got %v", line["remote_addr"])
	}
}
