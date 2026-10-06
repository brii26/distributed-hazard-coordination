package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

//nolint:paralleltest // swaps the global logger
func TestPlainLogLinesAreJSONWithTheRequiredFields(t *testing.T) {
	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	log.Printf("auth-service listening on :%d", 1234)

	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("log line is not one JSON object: %q", buf.String())
	}

	for _, f := range []string{"timestamp", "level", "service", "correlation_id", "message"} {
		if _, ok := line[f]; !ok {
			t.Fatalf("log line %v has no %q", line, f)
		}
	}

	if line["service"] != serviceName || line["correlation_id"] != "-" || line["message"] != "auth-service listening on :1234" || line["level"] != "info" {
		t.Fatalf("unexpected line %v", line)
	}
}

//nolint:paralleltest // swaps the global logger
func TestDatabaseCallsLogTargetDurationAndStatus(t *testing.T) {
	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-correlation-id", "req-7"))
	start := time.Now().Add(-10 * time.Millisecond)

	logDB(ctx, "user-db select user", start, nil)
	logDB(ctx, "user-db select user", start, errNotFound) // an answer, not a failure
	logDB(ctx, "user-db insert user", start, errors.New("connection refused"))
	logCall(context.Background(), "user-db ping", start, context.DeadlineExceeded)

	var lines []map[string]any

	for raw := range bytes.SplitSeq(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("not JSON: %q", raw)
		}

		lines = append(lines, m)
	}

	want := []struct{ status, correlation string }{{"success", "req-7"}, {"success", "req-7"}, {"failure", "req-7"}, {"timeout", "-"}}
	if len(lines) != len(want) {
		t.Fatalf("expected %d lines, got %s", len(want), buf.String())
	}

	for i, w := range want {
		l := lines[i]
		if l["message"] != "outbound call" || l["status"] != w.status || l["correlation_id"] != w.correlation || l["target"] == "" || l["duration_ms"] == nil {
			t.Fatalf("line %d: expected status %s and correlation %s, got %v", i, w.status, w.correlation, l)
		}
	}

	if lines[2]["error"] != "connection refused" {
		t.Fatalf("a failed call must carry its cause, got %v", lines[2])
	}
}
