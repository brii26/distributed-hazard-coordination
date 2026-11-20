package main

import (
	"testing"
	"time"
)

func testBreaker() (*breaker, *time.Time) {
	now := time.Unix(1000, 0)
	b := newBreaker(3, 10*time.Second)
	b.now = func() time.Time { return now }

	return b, &now
}

func fail(b *breaker, n int) {
	for range n {
		if b.allow() {
			b.done(false)
		}
	}
}

func TestBreakerOpensAfterThresholdFailuresInARow(t *testing.T) {
	t.Parallel()

	b, _ := testBreaker()
	fail(b, 2)

	if b.current() != closed || !b.allow() {
		t.Fatalf("state %s after 2 failures, want closed", b.current())
	}

	b.done(false)

	if b.current() != open || b.allow() {
		t.Fatalf("state %s, want open and refusing", b.current())
	}
}

func TestBreakerSuccessResetsTheFailureCount(t *testing.T) {
	t.Parallel()

	b, _ := testBreaker()
	fail(b, 2)
	b.allow()
	b.done(true)
	fail(b, 2)

	if b.current() != closed {
		t.Fatalf("state %s, want closed because the failures were not in a row", b.current())
	}
}

func TestBreakerLetsOneTrialThroughThenClosesOnSuccess(t *testing.T) {
	t.Parallel()

	b, now := testBreaker()
	fail(b, 3)
	*now = now.Add(11 * time.Second)

	if !b.allow() || b.current() != halfOpen {
		t.Fatal("first call after the open period must be the trial")
	}

	if b.allow() {
		t.Fatal("a second call must wait for the trial")
	}

	b.done(true)

	if b.current() != closed || !b.allow() {
		t.Fatalf("state %s, want closed after a good trial", b.current())
	}
}

func TestBreakerReopensWhenTheTrialFails(t *testing.T) {
	t.Parallel()

	b, now := testBreaker()
	fail(b, 3)
	*now = now.Add(11 * time.Second)
	b.allow()
	b.done(false)

	if b.current() != open || b.allow() {
		t.Fatalf("state %s, want open again", b.current())
	}

	*now = now.Add(11 * time.Second)

	if !b.allow() {
		t.Fatal("the next period must allow another trial")
	}
}
