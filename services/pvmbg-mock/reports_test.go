package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSeedAndSchemaFlag(t *testing.T) {
	t.Parallel()

	now := time.Now()
	legacy, extended := &reportStore{}, &reportStore{}
	legacy.seed(25, time.Second, now, false)
	extended.seed(25, time.Second, now, true)

	got := legacy.since(time.Time{})
	if len(got) != 25 {
		t.Fatalf("expected 25 seeded reports, got %d", len(got))
	}

	for _, r := range got {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}

		if strings.Contains(string(b), "confidence_level") {
			t.Fatalf("legacy report must not carry confidence_level: %s", b)
		}

		if r.ReportID == "" || r.VolcanoID == "" || r.AlertLevel == "" || r.ReportedAt.After(now) {
			t.Fatalf("bad seeded report %+v", r)
		}
	}

	for _, r := range extended.since(time.Time{}) {
		if r.ConfidenceLevel == nil || *r.ConfidenceLevel < 0 || *r.ConfidenceLevel > 1 {
			t.Fatalf("extended report needs confidence_level in [0,1]: %+v", r)
		}
	}
}

func TestGeneratorAddsReportsAndHonoursSchemaFlag(t *testing.T) {
	t.Parallel()

	s := &reportStore{}
	s.seed(20, time.Second, time.Now(), false)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		defer close(done)

		s.run(ctx, 5*time.Millisecond, func() bool { return true })
	}()

	// Read concurrently with the generator so -race exercises the lock.
	deadline := time.Now().Add(2 * time.Second)
	for len(s.since(time.Time{})) < 25 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	<-done

	all := s.since(time.Time{})
	if len(all) < 25 {
		t.Fatalf("generator should keep adding reports, got %d", len(all))
	}

	if last := all[len(all)-1]; last.ConfidenceLevel == nil {
		t.Fatalf("generated report should follow the extended flag: %+v", last)
	}

	if all[0].ConfidenceLevel != nil {
		t.Fatalf("seeded legacy report must stay without confidence_level: %+v", all[0])
	}
}

func TestSinceFilters(t *testing.T) {
	t.Parallel()

	s := &reportStore{}
	base := time.Now()
	s.seed(10, time.Minute, base, false) // oldest is 10 min ago, newest 1 min ago

	if n := len(s.since(base.Add(-5*time.Minute - time.Second))); n != 5 {
		t.Fatalf("expected the 5 newest reports, got %d", n)
	}
}
