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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestPlainLogLinesAreJSONWithTheRequiredFields(t *testing.T) {
	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	log.Printf("client-facing-api listening on :%d", 1234)

	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("log line is not one JSON object: %q", buf.String())
	}

	for _, f := range []string{"timestamp", "level", "service", "correlation_id", "message"} {
		if _, ok := line[f]; !ok {
			t.Fatalf("log line %v has no %q", line, f)
		}
	}

	if line["service"] != serviceName || line["correlation_id"] != "-" || line["message"] != "client-facing-api listening on :1234" || line["level"] != "info" {
		t.Fatalf("unexpected line %v", line)
	}
}

func TestEveryGRPCCallIsLoggedWithTargetDurationAndStatus(t *testing.T) {
	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	intercept := logOutbound("auth-service:9092")
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-correlation-id", "req-9"))

	for _, err := range []error{
		nil,
		status.Error(codes.Unauthenticated, "invalid credentials"), // an answer, not a failed call
		status.Error(codes.Unavailable, "connection refused"),
		status.Error(codes.DeadlineExceeded, "too slow"),
	} {
		got := intercept(ctx, "/auth.v1.Auth/Login", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			time.Sleep(2 * time.Millisecond)

			return err
		})
		if !errors.Is(got, err) {
			t.Fatalf("the interceptor must return the call's own error, got %v", got)
		}
	}

	var lines []map[string]any

	for _, raw := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("not JSON: %q", raw)
		}

		lines = append(lines, m)
	}

	want := []string{"success", "success", "failure", "timeout"}
	if len(lines) != len(want) {
		t.Fatalf("expected one line per call, got %s", buf.String())
	}

	for i, w := range want {
		l := lines[i]
		if l["message"] != "outbound call" || l["status"] != w || l["target"] != "grpc auth-service:9092/auth.v1.Auth/Login" || l["correlation_id"] != "req-9" {
			t.Fatalf("call %d: expected status %s, got %v", i, w, l)
		}

		if d, _ := l["duration_ms"].(float64); d < 2 {
			t.Fatalf("call %d: duration_ms must be measured, got %v", i, l["duration_ms"])
		}
	}
}
