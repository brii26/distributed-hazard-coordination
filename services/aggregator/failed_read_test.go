package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/aggregator/aggregatorpb"
)

func only(status string) failedFilter { return failedFilter{Statuses: []string{status}} }

// An in-memory stand-in for listFailed with the same filter, order and cursor rules.
func fakeFailed(rows []failedMessage) func(context.Context, failedFilter, string, int) ([]failedMessage, string, error) {
	return func(_ context.Context, f failedFilter, cursor string, limit int) ([]failedMessage, string, error) {
		var after time.Time

		afterID := ""

		if cursor != "" {
			var err error
			if after, afterID, err = decodeFailedCursor(cursor); err != nil {
				return nil, "", err
			}
		}

		var out []failedMessage

		for _, m := range rows {
			later := cursor == "" || m.ReceivedAt.After(after) || (m.ReceivedAt.Equal(after) && m.MessageID > afterID)
			if slices.Contains(f.Statuses, m.Status) && (f.Source == "" || f.Source == m.Source) && later {
				out = append(out, m)
			}
		}

		next := ""

		if len(out) > limit {
			out = out[:limit]
			next = encodeFailedCursor(out[limit-1].ReceivedAt, out[limit-1].MessageID)
		}

		return out, next, nil
	}
}

var failedBase = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

// n rows in time order: statuses cycle retrying, parked, recovered; one in four is a field report.
func failedRows(n int) []failedMessage {
	var rows []failedMessage

	for i := range n {
		m := newFailedMessage(sourceBMKG, fmt.Appendf(nil, "p-%d", i), "bad", false, failedBase.Add(time.Duration(i)*time.Second))
		m.Status = []string{failedRetrying, failedParked, failedRecovered}[i%3]
		m.Retryable = m.Status == failedRetrying

		if i%4 == 0 {
			m.Source = sourceFieldReport
		}

		rows = append(rows, m)
	}

	return rows
}

func unprocessedAPI(rows []failedMessage) *readAPI {
	return &readAPI{auditTok: testAuditToken, failed: fakeFailed(rows)}
}

func TestUnprocessedTokenGate(t *testing.T) {
	t.Parallel()

	want := `{"error":{"code":"unauthorized","message":"token tidak valid"}}` + "\n"
	a := unprocessedAPI(failedRows(3))

	for _, tok := range []string{"-", "wrong", ""} {
		if code, body := auditGet(t, a, "/internal/audit/unprocessed", tok); code != http.StatusUnauthorized || body != want {
			t.Fatalf("token %q: %d %q", tok, code, body)
		}
	}

	off := &readAPI{failed: fakeFailed(nil)}
	if code, body := auditGet(t, off, "/internal/audit/unprocessed", testAuditToken); code != http.StatusServiceUnavailable || !strings.Contains(body, "audit not configured") {
		t.Fatalf("unset token: %d %q", code, body)
	}
}

func TestUnprocessedFieldsAndRecoveredHidden(t *testing.T) {
	t.Parallel()

	// works with no cluster configured: the route reads the canonical database only
	code, body := auditGet(t, unprocessedAPI(failedRows(6)), "/internal/audit/unprocessed", testAuditToken)
	if code != http.StatusOK {
		t.Fatalf("%d %s", code, body)
	}

	var got struct {
		Items      []map[string]any `json:"items"`
		NextCursor *string          `json:"next_cursor"`
	}

	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}

	if len(got.Items) != 4 || got.NextCursor != nil {
		t.Fatalf("want 4 unrecovered rows and a null cursor: %s", body)
	}

	want := []string{"attempts", "message_id", "reason", "received_at", "retryable", "source", "status"}

	for _, it := range got.Items {
		keys := make([]string, 0, len(it))
		for k := range it {
			keys = append(keys, k)
		}

		slices.Sort(keys)

		if !slices.Equal(keys, want) {
			t.Fatalf("keys %v", keys)
		}

		if it["status"] == failedRecovered {
			t.Fatal("recovered row listed")
		}
	}

	if !strings.Contains(body, `"received_at":"2026-11-01T10:00:00Z"`) || strings.Contains(body, "payload") {
		t.Fatalf("time must be UTC Z and no payload: %s", body)
	}
}

func TestUnprocessedSourceAndPaging(t *testing.T) {
	t.Parallel()

	a := unprocessedAPI(failedRows(250))

	_, body := auditGet(t, a, "/internal/audit/unprocessed?source=FIELD_REPORT&limit=1000", testAuditToken)
	if !strings.Contains(body, `"source":"FIELD_REPORT"`) || strings.Contains(body, `"source":"BMKG"`) {
		t.Fatalf("source filter: %s", body)
	}

	seen, cursor := map[string]bool{}, ""

	for range 50 {
		_, body := auditGet(t, a, "/internal/audit/unprocessed?limit=40&cursor="+cursor, testAuditToken)

		var page unprocessedResponse
		if err := json.Unmarshal([]byte(body), &page); err != nil {
			t.Fatal(err)
		}

		for _, it := range page.Items {
			if seen[it.MessageID] {
				t.Fatalf("twice: %s", it.MessageID)
			}

			seen[it.MessageID] = true
		}

		if page.NextCursor == nil {
			break
		}

		cursor = *page.NextCursor
	}

	if len(seen) != 167 { // 250 rows, 83 of them recovered
		t.Fatalf("saw %d rows", len(seen))
	}
}

func TestUnprocessedBadInput(t *testing.T) {
	t.Parallel()

	a := unprocessedAPI(failedRows(3))

	for _, q := range []string{"source=X", "limit=0", "limit=1001", "limit=abc", "cursor=!!!", "cursor=" + "bm90LWEtY3Vyc29y"} {
		if code, body := auditGet(t, a, "/internal/audit/unprocessed?"+q, testAuditToken); code != http.StatusBadRequest {
			t.Fatalf("%s: %d %s", q, code, body)
		}
	}

	down := &readAPI{auditTok: testAuditToken, failed: func(context.Context, failedFilter, string, int) ([]failedMessage, string, error) {
		return nil, "", errors.New("boom")
	}}
	if code, body := auditGet(t, down, "/internal/audit/unprocessed", testAuditToken); code != http.StatusServiceUnavailable || strings.Contains(body, "boom") {
		t.Fatalf("db down: %d %s", code, body)
	}
}

func listFailedRPC(t *testing.T, rows []failedMessage, req *aggregatorpb.ListFailedMessagesRequest) (*aggregatorpb.ListFailedMessagesResponse, error) {
	t.Helper()

	return (&readAPI{failed: fakeFailed(rows)}).ListFailedMessages(roleCtx(t, roleOperator), req)
}

func roleCtx(t *testing.T, role string) context.Context {
	t.Helper()

	return metadata.NewIncomingContext(t.Context(), metadata.Pairs(metaClientRole, role))
}

func TestListFailedMessagesRequiresOperator(t *testing.T) {
	t.Parallel()

	calls := 0
	api := &readAPI{failed: func(context.Context, failedFilter, string, int) ([]failedMessage, string, error) {
		calls++

		return nil, "", nil
	}}
	req := &aggregatorpb.ListFailedMessagesRequest{}

	for _, ctx := range map[string]context.Context{
		"no metadata": t.Context(),
		"empty role":  roleCtx(t, ""),
		"Media":       roleCtx(t, "Media"),
		"Lapangan":    roleCtx(t, "Tim Lapangan"),
		"lowercase":   roleCtx(t, "operator"),
	} {
		if _, err := api.ListFailedMessages(ctx, req); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("want PermissionDenied, got %v", err)
		}
	}

	if calls != 0 {
		t.Fatalf("the store must not be read for a refused caller, got %d calls", calls)
	}

	if _, err := api.ListFailedMessages(roleCtx(t, roleOperator), req); err != nil || calls != 1 {
		t.Fatalf("Operator must pass: %v, calls %d", err, calls)
	}
}

func TestListFailedMessagesRPC(t *testing.T) { //nolint:gocyclo // one table of RPC cases, splitting it hides the shared rows
	t.Parallel()

	rows := failedRows(9)

	all, err := listFailedRPC(t, rows, &aggregatorpb.ListFailedMessagesRequest{})
	if err != nil || len(all.GetData()) != 9 {
		t.Fatalf("no status means all three: %v %v", all, err)
	}

	parked, err := listFailedRPC(t, rows, &aggregatorpb.ListFailedMessagesRequest{Status: failedParked})
	if err != nil || len(parked.GetData()) != 3 || parked.GetData()[0].GetStatus() != failedParked {
		t.Fatalf("status filter: %v %v", parked, err)
	}

	m := all.GetData()[0]
	if string(m.GetPayload()) != "p-0" || m.GetPayloadSize() != 3 || len(m.GetPayloadSha256()) != 32 || m.GetSource() != sourceFieldReport {
		t.Fatalf("fields not carried: %v", m)
	}

	for _, req := range []*aggregatorpb.ListFailedMessagesRequest{{Status: "bogus"}, {Limit: -1}, {Cursor: "!!"}} {
		if _, err := listFailedRPC(t, rows, req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%v: %v", req, err)
		}
	}

	page, _ := listFailedRPC(t, rows, &aggregatorpb.ListFailedMessagesRequest{Limit: 4})
	if len(page.GetData()) != 4 || page.GetNextCursor() == "" {
		t.Fatalf("limit and cursor: %v", page)
	}

	down := &readAPI{failed: func(context.Context, failedFilter, string, int) ([]failedMessage, string, error) {
		return nil, "", errors.New("boom")
	}}
	if _, err := down.ListFailedMessages(roleCtx(t, roleOperator), &aggregatorpb.ListFailedMessagesRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("db down: %v", err)
	}
}

func TestListFailedMessagesByteBudget(t *testing.T) {
	t.Parallel()

	big := bytes.Repeat([]byte("a"), maxFailedPayload)

	var rows []failedMessage

	for i := range 5 {
		rows = append(rows, newFailedMessage(sourcePVMBG, append([]byte{byte(i)}, big[1:]...), "bad", false, failedBase.Add(time.Duration(i)*time.Second)))
	}

	first, err := listFailedRPC(t, rows, &aggregatorpb.ListFailedMessagesRequest{Limit: 100})
	if err != nil || len(first.GetData()) != 2 || first.GetNextCursor() == "" {
		t.Fatalf("2 MiB budget must cut after 2 rows of 1 MiB: %d %v", len(first.GetData()), err)
	}

	second, _ := listFailedRPC(t, rows, &aggregatorpb.ListFailedMessagesRequest{Limit: 100, Cursor: first.GetNextCursor()})
	if second.GetData()[0].GetMessageId() != rows[2].MessageID {
		t.Fatal("the next page must continue right after the cut")
	}

	// one row bigger than the budget still goes out alone
	huge, _ := trimToBudget([]failedMessage{{Payload: make([]byte, failedPageBudget+1)}, {Payload: []byte("x")}}, "")
	if len(huge) != 1 {
		t.Fatalf("first row is always kept: %d", len(huge))
	}
}

// Real Postgres: filters, byte-exact payloads and exactly-once paging through the filtered query.
func TestListFailedFilterOnRealDB(t *testing.T) { //nolint:paralleltest,gocyclo // shares one truncated table; one scenario over one data set
	s := testFailedStore(t)
	ctx := t.Context()

	for i, m := range failedRows(250) {
		m.ReceivedAt = failedBase.Add(time.Duration(i/2) * time.Millisecond)

		if m.Source == sourceFieldReport {
			m.Source = sourceSatelit // the CHECK on source does not know field reports until their migration lands
		}

		if i%3 == 2 {
			m.Status = failedParked
		}

		if err := s.insertFailed(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := s.db.ExecContext(ctx, `UPDATE failed_messages SET status = 'recovered' WHERE message_id IN (SELECT message_id FROM failed_messages ORDER BY message_id LIMIT 10)`); err != nil {
		t.Fatal(err)
	}

	open := failedFilter{Statuses: []string{failedRetrying, failedParked}}
	seen, cursor := map[string]bool{}, ""

	for {
		rows, next, err := s.listFailed(ctx, open, cursor, 60)
		if err != nil {
			t.Fatal(err)
		}

		for _, r := range rows {
			if r.Status == failedRecovered || seen[r.MessageID] {
				t.Fatalf("recovered or repeated row %s", r.MessageID)
			}

			seen[r.MessageID] = true
		}

		if next == "" {
			break
		}

		cursor = next
	}

	if len(seen) != 240 {
		t.Fatalf("saw %d, want 240", len(seen))
	}

	rows, _, err := s.listFailed(ctx, failedFilter{Statuses: open.Statuses, Source: sourceSatelit}, "", 500)
	if err != nil || len(rows) == 0 || len(rows) >= 240 {
		t.Fatalf("source filter: %d %v", len(rows), err)
	}

	for _, r := range rows {
		if r.Source != sourceSatelit {
			t.Fatalf("wrong source %s", r.Source)
		}
	}
}
