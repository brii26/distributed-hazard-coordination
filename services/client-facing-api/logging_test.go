package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

// serveWithCorrelation runs one request through the correlation middleware and
// returns the response and the log lines it produced.
func serveWithCorrelation(t *testing.T, header string, inner http.Handler) (*httptest.ResponseRecorder, []map[string]any) {
	t.Helper()

	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/hazards", nil)
	if header != "" {
		req.Header.Set(correlationHeader, header)
	}

	rec := httptest.NewRecorder()
	withCorrelation(inner).ServeHTTP(rec, req)

	var lines []map[string]any

	for _, raw := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("not JSON: %q", raw)
		}

		lines = append(lines, m)
	}

	return rec, lines
}

func TestAClientCorrelationIDIsReusedReturnedAndLogged(t *testing.T) {
	var seen string

	rec, lines := serveWithCorrelation(t, "client-abc-123", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = requestID(r.Context())
		logFor(r.Context()).Info("inside the handler")
	}))

	if seen != "client-abc-123" || rec.Header().Get(correlationHeader) != "client-abc-123" {
		t.Fatalf("the client's ID must be used and returned, got %q and header %q", seen, rec.Header().Get(correlationHeader))
	}

	if len(lines) != 2 || lines[0]["correlation_id"] != "client-abc-123" || lines[1]["message"] != "request" || lines[1]["correlation_id"] != "client-abc-123" {
		t.Fatalf("every line of the request must carry its ID, got %v", lines)
	}
}

func TestAMissingOrUnusableCorrelationIDIsReplaced(t *testing.T) {
	for _, header := range []string{"", "has spaces", "line\nbreak", strings.Repeat("x", 129)} {
		var seen string

		rec, _ := serveWithCorrelation(t, header, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { seen = requestID(r.Context()) }))

		if seen == "" || seen == header || !validCorrelation.MatchString(seen) || rec.Header().Get(correlationHeader) != seen {
			t.Fatalf("header %q: expected a fresh ID in the context and the response, got %q", header, seen)
		}
	}
}

func TestTheRequestIDIsForwardedAsGRPCMetadata(t *testing.T) {
	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	ctx := context.WithValue(context.Background(), requestIDKey{}, "req-42")

	var sent []string

	_ = logOutbound("aggregator:9091")(ctx, "/aggregator.v1.Aggregator/GetHazards", nil, nil, nil,
		func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			md, _ := metadata.FromOutgoingContext(ctx)
			sent = md.Get("x-correlation-id")

			return nil
		})

	if len(sent) != 1 || sent[0] != "req-42" {
		t.Fatalf("the call must carry the request's ID once, got %v", sent)
	}

	if !strings.Contains(buf.String(), `"correlation_id":"req-42"`) {
		t.Fatalf("the outbound call line must carry the request's ID, got %s", buf.String())
	}
}
