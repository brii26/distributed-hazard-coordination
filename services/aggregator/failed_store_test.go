package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestFailedMessageIDAndOversize(t *testing.T) {
	t.Parallel()

	now := time.Now()
	a := newFailedMessage("PVMBG", []byte("x"), "r", false, now)
	b := newFailedMessage("PVMBG", []byte("x"), "other", false, now.Add(time.Hour))

	if a.MessageID != b.MessageID || a.Status != failedParked {
		t.Fatalf("same bytes must give the same id: %s %s", a.MessageID, b.MessageID)
	}

	full := newFailedMessage("BMKG", make([]byte, maxFailedPayload), "r", false, now)
	if full.Oversize || len(full.Payload) != maxFailedPayload {
		t.Fatalf("exactly 1 MiB stays whole: oversize=%v len=%d", full.Oversize, len(full.Payload))
	}

	cut := newFailedMessage("BMKG", make([]byte, maxFailedPayload+1), "r", false, now)
	if !cut.Oversize || len(cut.Payload) != failedPayloadHead || cut.PayloadSize != maxFailedPayload+1 {
		t.Fatalf("1 MiB + 1 must be cut: oversize=%v len=%d size=%d", cut.Oversize, len(cut.Payload), cut.PayloadSize)
	}

	if newFailedMessage("BMKG", nil, "r", true, now).Status != failedRetrying {
		t.Fatal("retryable must start as retrying")
	}
}

func TestFailedCursorRoundTrip(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 11, 1, 3, 4, 5, 123456000, time.UTC)

	gotT, gotID, err := decodeFailedCursor(encodeFailedCursor(ts, "PVMBG:ab|c"))
	if err != nil || !gotT.Equal(ts) || gotID != "PVMBG:ab|c" {
		t.Fatalf("got %v %q %v", gotT, gotID, err)
	}

	if _, _, err := decodeFailedCursor("!!"); err == nil {
		t.Fatal("garbage cursor must fail")
	}
}

// Store on a clean failed_messages table, skips without a DB.
func testFailedStore(t *testing.T) *store {
	t.Helper()

	s := testStore(t, 25)

	ddl, err := os.ReadFile("migrations/003_failed_messages.sql")
	if err != nil {
		t.Fatal(err)
	}

	for _, q := range []string{string(ddl), `TRUNCATE failed_messages`} {
		if _, err := s.db.ExecContext(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}

	return s
}

func TestFailedStoreRoundTripBytes(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()
	big := bytes.Repeat([]byte("a"), maxFailedPayload+1)

	for i, raw := range [][]byte{[]byte(`{"a":1, "b" :2}`), {0xff, 0xfe, 0x80}, {'a', 0, 'b'}, {}, big} {
		m := newFailedMessage("PVMBG", raw, "bad", false, time.Now().Add(time.Duration(i)*time.Second))
		if err := s.insertFailed(ctx, m); err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
	}

	rows, _, err := s.listFailed(ctx, failedParked, "", 10)
	if err != nil || len(rows) != 5 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}

	if !bytes.Equal(rows[0].Payload, []byte(`{"a":1, "b" :2}`)) || !bytes.Equal(rows[1].Payload, []byte{0xff, 0xfe, 0x80}) ||
		!bytes.Equal(rows[2].Payload, []byte{'a', 0, 'b'}) || len(rows[3].Payload) != 0 {
		t.Fatal("payload bytes changed in storage")
	}

	if !rows[4].Oversize || len(rows[4].Payload) != failedPayloadHead || rows[4].PayloadSize != maxFailedPayload+1 {
		t.Fatalf("oversize row wrong: %v %d %d", rows[4].Oversize, len(rows[4].Payload), rows[4].PayloadSize)
	}
}

func TestFailedStoreIdempotentAndReopen(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()
	m := newFailedMessage("BMKG", []byte("same"), "bad", false, time.Now())

	for range 2 {
		if err := s.insertFailed(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	rows, _, _ := s.listFailed(ctx, failedParked, "", 10)
	if len(rows) != 1 || rows[0].Attempts != 1 {
		t.Fatalf("a re-poll must not duplicate or bump: rows=%d", len(rows))
	}

	if ok, _ := s.markRecovered(ctx, m.MessageID); ok {
		t.Fatal("a parked row must not turn recovered")
	}

	if _, err := s.db.ExecContext(ctx, `UPDATE failed_messages SET status = 'retrying'`); err != nil {
		t.Fatal(err)
	}

	if ok, err := s.markRecovered(ctx, m.MessageID); !ok || err != nil {
		t.Fatalf("recover: %v %v", ok, err)
	}

	if rows, _, _ = s.listFailed(ctx, failedParked, "", 10); len(rows) != 0 {
		t.Fatal("recovered row must leave the parked list")
	}

	m.LastErrorAt = time.Now().Add(time.Minute)
	if err := s.insertFailed(ctx, m); err != nil {
		t.Fatal(err)
	}

	rows, _, _ = s.listFailed(ctx, failedParked, "", 10)
	if len(rows) != 1 || rows[0].Attempts != 2 || !rows[0].LastErrorAt.Equal(m.LastErrorAt.Truncate(time.Microsecond)) {
		t.Fatalf("regression must reopen the row: %+v", rows)
	}
}

func TestFailedStorePagination(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()
	at := time.Now()

	for i := range 250 {
		// same received_at for pairs, so the message_id half of the cursor is exercised
		m := newFailedMessage("PVMBG", fmt.Appendf(nil, "msg-%d", i), "bad", false, at.Add(time.Duration(i/2)*time.Millisecond))
		if err := s.insertFailed(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	seen, pages, cursor := map[string]bool{}, 0, ""

	for {
		rows, next, err := s.listFailed(ctx, failedParked, cursor, 100)
		if err != nil {
			t.Fatal(err)
		}

		pages++

		for _, r := range rows {
			if seen[r.MessageID] {
				t.Fatalf("row listed twice: %s", r.MessageID)
			}

			seen[r.MessageID] = true
		}

		if next == "" {
			break
		}

		cursor = next
	}

	if pages != 3 || len(seen) != 250 {
		t.Fatalf("pages=%d rows=%d", pages, len(seen))
	}
}

func TestFailedStoreParallelAttempts(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()
	m := newFailedMessage("BMKG", []byte("p"), "bad", false, time.Now())

	if err := s.insertFailed(ctx, m); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	for range 20 {
		wg.Go(func() { _, _ = s.incrementAttempts(ctx, m.MessageID) })
	}

	wg.Wait()

	if rows, _, _ := s.listFailed(ctx, failedParked, "", 10); len(rows) != 1 || rows[0].Attempts != 21 {
		t.Fatalf("attempts must be 21 (1 + 20): %+v", rows)
	}
}

func TestListFailedRejectsNonPositiveLimit(t *testing.T) {
	t.Parallel()

	s := &store{} // validation runs before any DB access

	for _, limit := range []int{0, -1} {
		if _, _, err := s.listFailed(context.Background(), failedParked, "", limit); err == nil {
			t.Fatalf("limit %d must return an error", limit)
		}
	}
}
