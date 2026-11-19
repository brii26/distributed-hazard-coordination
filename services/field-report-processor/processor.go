package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	callTimeout  = 5 * time.Second
	backoffBase  = 200 * time.Millisecond
	backoffMax   = 10 * time.Second
	parkAfter    = 5
	maxTrackedID = 10000

	// A body above maxBody is parked without sending it whole: the Aggregator accepts 4 MiB per call and would answer ResourceExhausted for ever. Only the first keepHead bytes go along.
	maxBody  = 1 << 20
	keepHead = 64 << 10

	// Retryable failures of one id before it is parked, but only while this pod keeps saving other reports. During an outage nothing succeeds, so nothing is parked.
	maxRetryable = 20
)

// Retryable failures of one message id on this pod, and the pod's success count when the first one happened.
type slowState struct {
	n    int
	okAt int64
}

// One delivery as the processor sees it. ack and nack wrap the broker calls.
type job struct {
	id, correlationID string
	body              []byte
	ack               func() error
	nack              func(requeue bool) error
}

// Calls the Aggregator. result is STORED, DUPLICATE or PARKED; an error means nothing was confirmed.
type saver func(ctx context.Context, id string, body []byte, correlationID, parkReason string) (string, error)

// Saves one delivery at a time and acks only after the Aggregator confirmed it.
type processor struct {
	save  saver
	sleep func(ctx context.Context, d time.Duration) // injected so tests do not wait

	streak atomic.Int64 // consecutive retryable failures of this pod

	mu    sync.Mutex
	fails map[string]int       // other errors per message id, in memory only
	slow  map[string]slowState // retryable errors per message id, in memory only

	stored, duplicate, parked, requeued atomic.Int64
	successes                           atomic.Int64 // reports this pod saved, any result
	nanos, calls                        atomic.Int64 // time spent per delivery, for the average
}

func newProcessor(s saver) *processor {
	return &processor{save: s, sleep: sleepCtx, fails: map[string]int{}, slow: map[string]slowState{}}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// Delay before a requeue after n consecutive failures: 200 ms doubling, at most 10 s.
func backoff(n int64) time.Duration {
	return min(backoffMax, backoffBase<<min(n, 10))
}

func retryable(err error) bool {
	switch status.Code(err) { //nolint:exhaustive // every other code counts as a plain failure
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// Remembers a failed save of id. Other errors count toward parkAfter; retryable ones toward maxRetryable.
func (p *processor) countFailure(id string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !retryable(err) {
		if len(p.fails) < maxTrackedID {
			p.fails[id]++
		}

		return
	}

	st, tracked := p.slow[id]
	if !tracked && len(p.slow) >= maxTrackedID {
		return
	}

	if st.n == 0 {
		st.okAt = p.successes.Load()
	}

	st.n++
	p.slow[id] = st
}

// Handles one delivery: save, then ack. A failure never acks; the message goes back to the queue after a delay. Five other errors for one id make the next call ask the Aggregator to park it.
func (p *processor) handle(ctx context.Context, j job) {
	log := slog.With("report_id", j.id, "correlation_id", j.correlationID)
	start := time.Now()

	defer func() { p.nanos.Add(int64(time.Since(start))); p.calls.Add(1) }()

	p.mu.Lock()
	reason := ""

	if p.fails[j.id] >= parkAfter {
		reason = "gave up after repeated errors"
	}

	if st := p.slow[j.id]; st.n >= maxRetryable && p.successes.Load() > st.okAt {
		reason = "gave up after repeated transient errors while other reports were saved"
	}

	p.mu.Unlock()

	body := j.body
	if len(body) > maxBody {
		reason = fmt.Sprintf("body too large: %d bytes, the first %d are kept", len(body), keepHead)
		body = body[:keepHead]
	}

	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	res, err := p.save(cctx, j.id, body, j.correlationID, reason)

	cancel()

	if err != nil {
		p.countFailure(j.id, err)

		p.requeued.Add(1)
		log.Warn("save failed, requeue", "code", status.Code(err).String())
		p.sleep(ctx, backoff(p.streak.Add(1)-1))

		if e := j.nack(true); e != nil {
			log.Warn("nack failed", "error", e.Error())
		}

		return
	}

	p.streak.Store(0)
	p.successes.Add(1)

	p.mu.Lock()
	delete(p.fails, j.id)
	delete(p.slow, j.id)
	p.mu.Unlock()

	switch res {
	case resultDuplicate:
		p.duplicate.Add(1)
	case resultParked:
		p.parked.Add(1)
	default:
		p.stored.Add(1)
	}

	if e := j.ack(); e != nil {
		log.Warn("ack failed, the broker will redeliver", "error", e.Error())

		return
	}

	log.Info("field report done", "result", res)
}

const (
	resultDuplicate = "DUPLICATE"
	resultParked    = "PARKED"
)
