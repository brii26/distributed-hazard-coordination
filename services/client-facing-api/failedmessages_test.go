package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/aggregatorpb"
)

const (
	testNextCursor = "next"
	queryLimitZero = "limit=0"
)

// Stands in for the Aggregator's ListFailedMessages and records what it was asked.
type fakeFailed struct {
	aggregatorpb.AggregatorClient

	resp *aggregatorpb.ListFailedMessagesResponse
	err  error

	calls *int
	got   **aggregatorpb.ListFailedMessagesRequest
}

func (f fakeFailed) ListFailedMessages(_ context.Context, in *aggregatorpb.ListFailedMessagesRequest, _ ...grpc.CallOption) (*aggregatorpb.ListFailedMessagesResponse, error) {
	if f.calls != nil {
		*f.calls++
	}

	if f.got != nil {
		*f.got = in
	}

	return f.resp, f.err
}

const failedTestSource = "BMKG"

var oddPayload = []byte{0xff, 0xfe, 0x00, 'a', 0x80}

func oneFailed() *aggregatorpb.ListFailedMessagesResponse {
	sum := sha256.Sum256(oddPayload)

	return &aggregatorpb.ListFailedMessagesResponse{
		Data: []*aggregatorpb.FailedMessage{{
			MessageId: "BMKG:ab", Source: failedTestSource, ReceivedAt: timestamppb.New(occurred), Reason: "bad json", Attempts: 1,
			Status: "parked", LastErrorAt: timestamppb.New(occurred), Payload: oddPayload, PayloadSha256: sum[:], PayloadSize: 5,
		}},
		NextCursor: testNextCursor,
	}
}

func getFailed(t *testing.T, f fakeFailed, role, query string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/failed-messages?"+query, nil)
	if role != "" {
		req.AddCookie(sessionCookie(cookieJWT, validToken(t, role)))
	}

	rec := httptest.NewRecorder()
	newVerifier().require(requireOperator(failedMessagesAPI{agg: f, timeout: time.Second})).ServeHTTP(rec, req)

	var body map[string]any

	_ = json.Unmarshal(rec.Body.Bytes(), &body)

	return rec, body
}

func TestFailedMessagesAuthz(t *testing.T) {
	t.Parallel()

	calls := 0
	f := fakeFailed{resp: oneFailed(), calls: &calls}

	for role, want := range map[string]int{"": 401, roleMedia: 403, roleLapangan: 403, roleOperator: 200} {
		if rec, _ := getFailed(t, f, role, ""); rec.Code != want {
			t.Fatalf("role %q: %d, want %d", role, rec.Code, want)
		}
	}

	if calls != 1 {
		t.Fatalf("only the Operator may reach the Aggregator, got %d calls", calls)
	}

	// the role is the verified one, whatever the caller puts in the query
	if rec, _ := getFailed(t, f, roleMedia, "role=Operator"); rec.Code != http.StatusForbidden {
		t.Fatalf("query role: %d", rec.Code)
	}

	// and in the body
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/failed-messages", strings.NewReader(`{"role":"Operator"}`))
	req.AddCookie(sessionCookie(cookieJWT, validToken(t, roleMedia)))

	rec := httptest.NewRecorder()
	newVerifier().require(requireOperator(failedMessagesAPI{agg: f, timeout: time.Second})).ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("body role: %d", rec.Code)
	}
}

func TestFailedMessagesOperatorGetsPayload(t *testing.T) {
	t.Parallel()

	rec, body := getFailed(t, fakeFailed{resp: oneFailed()}, roleOperator, "")
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("%d %v", rec.Code, rec.Header())
	}

	data, _ := body["data"].([]any)
	row, _ := data[0].(map[string]any)
	enc, _ := row["payload"].(string)

	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil || !bytes.Equal(raw, oddPayload) {
		t.Fatalf("payload must round trip byte for byte: %v %v", raw, err)
	}

	if row["payload_encoding"] != "base64" || row["oversize"] != false || row["payload_size"] != float64(5) || row["status"] != "parked" {
		t.Fatalf("row: %v", row)
	}

	if _, has := row["source_ref_id"]; has {
		t.Fatal("empty source_ref_id must be left out")
	}

	if _, has := row["mapped_event"]; has {
		t.Fatal("internal fields must not leak")
	}

	if body["next_cursor"] != testNextCursor {
		t.Fatalf("cursor: %v", body["next_cursor"])
	}

	last := &aggregatorpb.ListFailedMessagesResponse{Data: oneFailed().Data}
	if _, b := getFailed(t, fakeFailed{resp: last}, roleOperator, ""); b["next_cursor"] != nil {
		t.Fatalf("last page cursor must be null: %v", b["next_cursor"])
	}
}

func TestFailedMessagesQueryChecks(t *testing.T) {
	t.Parallel()

	var got *aggregatorpb.ListFailedMessagesRequest

	f := fakeFailed{resp: oneFailed(), got: &got}

	if rec, _ := getFailed(t, f, roleOperator, "status=parked&limit=7&cursor=abc"); rec.Code != 200 ||
		got.GetStatus() != "parked" || got.GetLimit() != 7 || got.GetCursor() != "abc" {
		t.Fatalf("not passed on: %d %v", rec.Code, got)
	}

	if getFailed(t, f, roleOperator, ""); got.GetLimit() != failedDefaultLimit {
		t.Fatalf("default limit %d", got.GetLimit())
	}

	calls := 0
	f.calls = &calls

	for _, q := range []string{"status=bogus", "limit=101", queryLimitZero, "limit=x", "limit=-3"} {
		if rec, _ := getFailed(t, f, roleOperator, q); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: %d", q, rec.Code)
		}
	}

	if calls != 0 {
		t.Fatal("bad input must not reach the Aggregator")
	}
}

func TestFailedMessagesUpstreamErrors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		err  error
		want int
	}{
		{status.Error(codes.Unavailable, "db down"), http.StatusBadGateway},
		{status.Error(codes.DeadlineExceeded, "slow"), http.StatusGatewayTimeout},
		{status.Error(codes.InvalidArgument, "cursor tidak valid"), http.StatusBadRequest},
		{status.Error(codes.Internal, "x"), http.StatusBadGateway},
	} {
		rec, _ := getFailed(t, fakeFailed{err: tc.err}, roleOperator, "")
		if rec.Code != tc.want {
			t.Fatalf("%v: %d, want %d", tc.err, rec.Code, tc.want)
		}
	}
}

func TestFailedMessagesSendsOperatorRole(t *testing.T) {
	t.Parallel()

	var role string

	f := &roleSpy{role: &role}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/failed-messages", nil)
	req.AddCookie(sessionCookie(cookieJWT, validToken(t, roleOperator)))
	newVerifier().require(requireOperator(failedMessagesAPI{agg: f, timeout: time.Second})).ServeHTTP(httptest.NewRecorder(), req)

	if role != roleOperator {
		t.Fatalf("x-client-role %q", role)
	}
}

type roleSpy struct {
	aggregatorpb.AggregatorClient

	role *string
}

func (s *roleSpy) ListFailedMessages(ctx context.Context, _ *aggregatorpb.ListFailedMessagesRequest, _ ...grpc.CallOption) (*aggregatorpb.ListFailedMessagesResponse, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	if v := md.Get("x-client-role"); len(v) > 0 {
		*s.role = v[0]
	}

	return &aggregatorpb.ListFailedMessagesResponse{}, nil
}
