package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func event(id, typ, severity string) HazardEvent {
	sev := severity

	return HazardEvent{HazardID: id, Source: "BMKG", SourceRefID: "ref-" + id, HazardType: typ, Severity: &sev,
		OccurredAt: time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC), IngestedAt: time.Date(2026, 10, 1, 10, 0, 1, 0, time.UTC)}
}

func msg(t *testing.T, e HazardEvent) message {
	t.Helper()

	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}

	return message{body: b, correlationID: "c-" + e.HazardID}
}

func TestTwoStreamsInSequenceBothLand(t *testing.T) {
	t.Parallel()

	s := newState()

	for i := range 3 {
		if out := s.handle(msg(t, event(fmt.Sprintf("seis-%d", i), "SEISMIC", "NORMAL"))); out != ack {
			t.Fatalf("seismic %d: expected ack, got %v", i, out)
		}
	}

	for i := range 2 {
		s.handle(msg(t, event(fmt.Sprintf("vol-%d", i), "VOLCANIC", "SIAGA")))
	}

	if got := len(s.snapshot()); got != 5 {
		t.Fatalf("expected both streams in the state, 5 events, got %d", got)
	}
}

func TestRedeliveryDoesNotDuplicate(t *testing.T) {
	t.Parallel()

	s := newState()
	m := msg(t, event("h1", "SEISMIC", "WASPADA"))

	for range 3 {
		if out := s.handle(m); out != ack {
			t.Fatalf("a redelivery must still be acked, got %v", out)
		}
	}

	if got := s.snapshot(); len(got) != 1 || *got[0].Severity != "WASPADA" {
		t.Fatalf("expected one unchanged entry, got %+v", got)
	}
}

func TestAChangedEventReplacesItsEntry(t *testing.T) {
	t.Parallel()

	s := newState()
	s.handle(msg(t, event("h1", "VOLCANIC", "SIAGA")))
	s.handle(msg(t, event("h1", "VOLCANIC", "AWAS")))

	if got := s.snapshot(); len(got) != 1 || *got[0].Severity != "AWAS" {
		t.Fatalf("expected the escalated entry only, got %+v", got)
	}
}

func TestMalformedMessagesAreDroppedNotRequeued(t *testing.T) {
	t.Parallel()

	s := newState()

	for _, body := range []string{"not json", `{"source":"BMKG"}`, ""} {
		if out := s.handle(message{body: []byte(body)}); out != drop {
			t.Fatalf("%q: a message that can never be processed must be dropped, got %v", body, out)
		}
	}

	if len(s.snapshot()) != 0 {
		t.Fatal("a malformed message must not change the state")
	}
}

func TestConcurrentHandleAndReads(t *testing.T) {
	t.Parallel()

	s := newState()

	var wg sync.WaitGroup

	for i := range 50 {
		wg.Go(func() { s.handle(msg(t, event(fmt.Sprintf("h%d", i%10), "SEISMIC", "NORMAL"))) })
		wg.Go(func() { _ = s.snapshot() })
	}

	wg.Wait()

	if got := len(s.snapshot()); got != 10 {
		t.Fatalf("expected 10 distinct entries, got %d", got)
	}
}
