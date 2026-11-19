package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const evAck = "ack"

// Records what the broker was told, in order.
type events struct {
	mu  sync.Mutex
	log []string
}

func (e *events) add(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.log = append(e.log, s)
}

func (e *events) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]string(nil), e.log...)
}

func testJob(ev *events, id string) job {
	return job{
		id: id, body: []byte("{}"), correlationID: "c1",
		ack: func() error { ev.add("ack"); return nil },
		nack: func(requeue bool) error {
			ev.add(map[bool]string{true: "nack-requeue", false: "nack"}[requeue])
			return nil
		},
	}
}

func TestAckOnlyAfterSaverReturns(t *testing.T) {
	t.Parallel()

	ev := &events{}
	release := make(chan struct{})
	p := newProcessor(func(context.Context, string, []byte, string, string) (string, error) {
		ev.add("call")
		<-release
		ev.add("saved")

		return "STORED", nil
	})

	done := make(chan struct{})

	go func() { p.handle(context.Background(), testJob(ev, "r1")); close(done) }()

	time.Sleep(50 * time.Millisecond)

	if got := ev.snapshot(); len(got) != 1 || got[0] != "call" {
		t.Fatalf("ack happened while the call was blocked: %v", got)
	}

	close(release)
	<-done

	if got := ev.snapshot(); len(got) != 3 || got[2] != "ack" {
		t.Fatalf("order: %v", got)
	}
}

func TestResultsAreAcked(t *testing.T) {
	t.Parallel()

	for _, res := range []string{"STORED", "DUPLICATE", "PARKED"} {
		ev := &events{}
		p := newProcessor(func(context.Context, string, []byte, string, string) (string, error) { return res, nil })
		p.handle(context.Background(), testJob(ev, "r1"))

		if got := ev.snapshot(); len(got) != 1 || got[0] != "ack" {
			t.Fatalf("%s: %v", res, got)
		}
	}
}

func TestRetryableErrorsRequeueWithBackoff(t *testing.T) {
	t.Parallel()

	ev := &events{}

	var delays []time.Duration

	p := newProcessor(func(context.Context, string, []byte, string, string) (string, error) {
		return "", status.Error(codes.Unavailable, "down")
	})
	p.sleep = func(_ context.Context, d time.Duration) { delays = append(delays, d) }

	for range 8 {
		p.handle(context.Background(), testJob(ev, "r1"))
	}

	for _, s := range ev.snapshot() {
		if s != "nack-requeue" {
			t.Fatalf("unexpected %s", s)
		}
	}

	want := []time.Duration{
		200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond,
		3200 * time.Millisecond, 6400 * time.Millisecond, 10 * time.Second, 10 * time.Second,
	}
	for i, w := range want {
		if delays[i] != w {
			t.Fatalf("delay %d = %v want %v", i, delays[i], w)
		}
	}

	if len(p.fails) != 0 {
		t.Fatal("retryable errors must not count toward parking")
	}
}

func TestDeadlineExceededAndExhaustedRetry(t *testing.T) {
	t.Parallel()

	for _, c := range []codes.Code{codes.DeadlineExceeded, codes.ResourceExhausted} {
		if !retryable(status.Error(c, "x")) {
			t.Fatalf("%v should retry", c)
		}
	}

	if retryable(errors.New("plain")) || retryable(status.Error(codes.Internal, "x")) {
		t.Fatal("others must not count as outage")
	}
}

func TestOversizedBodyIsParkedWithoutSendingItWhole(t *testing.T) {
	t.Parallel()

	ev := &events{}

	var gotLen int

	var gotReason string

	p := newProcessor(func(_ context.Context, _ string, body []byte, _, reason string) (string, error) {
		gotLen, gotReason = len(body), reason

		return resultParked, nil
	})

	j := testJob(ev, "big")
	j.body = make([]byte, 5<<20)
	p.handle(context.Background(), j)

	if gotLen != keepHead || gotReason == "" {
		t.Fatalf("sent %d bytes, reason %q; want %d bytes and a park reason", gotLen, gotReason, keepHead)
	}

	if got := ev.snapshot(); len(got) != 1 || got[0] != evAck {
		t.Fatalf("events: %v", got)
	}

	// exactly maxBody is still sent whole and not parked by size
	j.body = make([]byte, maxBody)
	gotReason = "unset"

	p.handle(context.Background(), j)

	if gotLen != maxBody || gotReason != "" {
		t.Fatalf("maxBody: sent %d bytes, reason %q", gotLen, gotReason)
	}
}

func TestRetryableFailureIsCappedOnlyWhileOthersSucceed(t *testing.T) {
	t.Parallel()

	var reasons []string

	down := func(context.Context, string, []byte, string, string) (string, error) {
		return "", status.Error(codes.Unavailable, "down")
	}

	p := newProcessor(down)
	p.sleep = func(context.Context, time.Duration) {}
	p.save = func(c context.Context, id string, b []byte, corr, reason string) (string, error) {
		reasons = append(reasons, reason)

		if reason != "" {
			return resultParked, nil
		}

		return down(c, id, b, corr, reason)
	}

	// A long outage: nothing succeeds, so nothing is ever parked.
	for range 3 * maxRetryable {
		p.handle(context.Background(), testJob(&events{}, "stuck"))
	}

	for _, r := range reasons {
		if r != "" {
			t.Fatalf("parked during an outage: %q", r)
		}
	}

	// The pod saves other reports again, so the same id is now a poison message.
	p.successes.Add(1)

	reasons = nil
	ev := &events{}

	p.handle(context.Background(), testJob(ev, "stuck"))

	if len(reasons) != 1 || reasons[0] == "" || ev.snapshot()[0] != evAck {
		t.Fatalf("reasons %q events %v, want one park call and an ack", reasons, ev.snapshot())
	}

	if len(p.slow) != 0 {
		t.Fatal("counter must reset after the park")
	}
}

func TestParkAfterFiveOtherErrors(t *testing.T) {
	t.Parallel()

	ev := &events{}

	var reasons []string

	p := newProcessor(func(_ context.Context, _ string, _ []byte, _, reason string) (string, error) {
		reasons = append(reasons, reason)

		if reason != "" {
			return "PARKED", nil
		}

		return "", status.Error(codes.Internal, "boom")
	})
	p.sleep = func(context.Context, time.Duration) {}

	for range 6 {
		p.handle(context.Background(), testJob(ev, "poison"))
	}

	for i, r := range reasons {
		if (i < 5) != (r == "") {
			t.Fatalf("call %d reason %q", i, r)
		}
	}

	got := ev.snapshot()
	if got[5] != "ack" || got[4] != "nack-requeue" {
		t.Fatalf("events: %v", got)
	}

	if len(p.fails) != 0 {
		t.Fatal("counter must reset after the park")
	}
}
