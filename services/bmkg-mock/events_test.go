package main

import (
	"context"
	"testing"
	"time"
)

func TestSeedAndTsunamiLinkage(t *testing.T) {
	t.Parallel()

	now := time.Now()
	s := &eventStore{}
	s.seed(200, time.Second, now)

	events := s.eventsSince(time.Time{})
	if len(events) != 200 {
		t.Fatalf("expected 200 seeded events, got %d", len(events))
	}

	want := 0

	for _, e := range events {
		if e.EventID == "" || e.RegionName == "" || e.OccurredAt.After(now) {
			t.Fatalf("bad seeded event %+v", e)
		}

		if e.PotentialTsunami {
			want++
		}
	}

	warnings := s.warningsSince(time.Time{})
	if want == 0 || len(warnings) != want {
		t.Fatalf("expected one warning per tsunami event (%d), got %d", want, len(warnings))
	}
}

func TestGeneratorAddsEvents(t *testing.T) {
	t.Parallel()

	s := &eventStore{}
	s.seed(20, time.Second, time.Now())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		s.run(ctx, 5*time.Millisecond)
	}()

	// Read concurrently with the generator so -race exercises the lock.
	deadline := time.Now().Add(2 * time.Second)
	for len(s.eventsSince(time.Time{})) < 25 && time.Now().Before(deadline) {
		s.warningsSince(time.Time{})
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	<-done

	if n := len(s.eventsSince(time.Time{})); n < 25 {
		t.Fatalf("generator should keep adding events, got %d", n)
	}
}

func TestEventsSinceFilters(t *testing.T) {
	t.Parallel()

	s := &eventStore{}
	base := time.Now()
	s.seed(10, time.Minute, base) // oldest 10 min ago, newest 1 min ago

	if n := len(s.eventsSince(base.Add(-5*time.Minute - time.Second))); n != 5 {
		t.Fatalf("expected the 5 newest events, got %d", n)
	}
}
