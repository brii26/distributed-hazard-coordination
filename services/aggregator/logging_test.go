package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a bytes.Buffer safe to write from several goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// captureLogs sends all logging to a buffer for the test and back to stdout after.
func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()

	var buf syncBuffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	return &buf
}

// jsonLines parses every line, failing on anything that is not JSON with the
// doc/TRD_M1.md 0.3 fields.
func jsonLines(t *testing.T, out string) []map[string]any {
	t.Helper()

	var lines []map[string]any

	sc := bufio.NewScanner(bytes.NewBufferString(out))
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", sc.Text())
		}

		for _, f := range []string{"timestamp", "level", "service", "correlation_id", "message"} {
			if _, ok := m[f]; !ok {
				t.Fatalf("log line %q has no %q", sc.Text(), f)
			}
		}

		if m["service"] != serviceName {
			t.Fatalf("service must be %q, got %v", serviceName, m["service"])
		}

		lines = append(lines, m)
	}

	return lines
}

func find(lines []map[string]any, message string) map[string]any {
	for _, l := range lines {
		if l["message"] == message {
			return l
		}
	}

	return nil
}

func TestPlainLogLinesAreJSONWithTheRequiredFields(t *testing.T) {
	buf := captureLogs(t)

	log.Printf("aggregator listening on :%d", 8091)

	l := find(jsonLines(t, buf.String()), "aggregator listening on :8091")
	if l == nil || l["correlation_id"] != "-" || l["level"] != "info" {
		t.Fatalf("expected the log.Printf line as JSON with correlation_id \"-\", got %v", buf.String())
	}
}

func TestOutboundCallsLogTargetDurationAndStatus(t *testing.T) {
	buf := captureLogs(t)
	ctx := withCorrelation(context.Background(), "cycle-1")
	start := time.Now().Add(-25 * time.Millisecond)

	logCall(ctx, "GET http://bmkg-mock:8081/seismic-events", start, nil)
	logCall(ctx, "canonical-store upsert", start, errors.New("connection refused"))
	logCall(ctx, "POST http://pvmbg-mock:8082/oauth/token", start, context.DeadlineExceeded)

	lines := jsonLines(t, buf.String())
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d: %s", len(lines), buf.String())
	}

	for i, want := range []string{"success", "failure", "timeout"} {
		l := lines[i]
		if l["message"] != "outbound call" || l["status"] != want || l["target"] == "" || l["correlation_id"] != "cycle-1" {
			t.Fatalf("line %d: expected an outbound call with status %s, got %v", i, want, l)
		}

		if d, ok := l["duration_ms"].(float64); !ok || d < 25 {
			t.Fatalf("line %d: duration_ms must be the call's duration, got %v", i, l["duration_ms"])
		}
	}

	if lines[1]["error"] != "connection refused" {
		t.Fatalf("a failed call must carry its cause, got %v", lines[1])
	}
}

func TestAPollCycleLogsItsCallsAndResultUnderOneCorrelationID(t *testing.T) {
	buf := captureLogs(t)

	c := &sourceClient{baseURL: "http://127.0.0.1:1", timeout: 200 * time.Millisecond, client: &http.Client{}, auth: func(*http.Request) error { return nil }}
	p := &poller{name: sourceBMKG, breaker: newBreaker(sourceBMKG, 3, time.Second), out: make(chan pollResult, 1),
		fetch: func(ctx context.Context, since time.Time) (pollResult, error) {
			_, err := c.records(ctx, "/seismic-events", since)

			return pollResult{}, err
		}}

	p.pollOnce(context.Background())

	lines := jsonLines(t, buf.String())
	call, poll := find(lines, "outbound call"), find(lines, "poll")

	if call == nil || poll == nil || call["correlation_id"] != poll["correlation_id"] || poll["correlation_id"] == "-" {
		t.Fatalf("the poll line and its call must share one correlation ID, got %s", buf.String())
	}

	if call["status"] != "failure" || poll["result"] != "failure" {
		t.Fatalf("expected a failed call and a failed poll, got %v / %v", call, poll)
	}
}

func TestPollsSendTheCycleIDToTheSource(t *testing.T) {
	got := make(chan string, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("X-Correlation-ID")

		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()

	c := &sourceClient{baseURL: srv.URL, timeout: time.Second, client: srv.Client(), auth: func(*http.Request) error { return nil }}
	if _, err := c.records(withCorrelation(context.Background(), "cycle-7"), "/seismic-events", time.Time{}); err != nil {
		t.Fatal(err)
	}

	if id := <-got; id != "cycle-7" {
		t.Fatalf("the source must receive the cycle's ID, got %q", id)
	}
}
