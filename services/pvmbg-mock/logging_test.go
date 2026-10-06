package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

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
