package main

import (
	"sync"
	"time"
)

type breakerState int

const (
	closed breakerState = iota
	open
	halfOpen
)

func (s breakerState) String() string {
	return [...]string{"closed", "open", "half-open"}[s]
}

// Circuit breaker for the LLM calls: opens after threshold failures in a row, lets one trial call through after openFor, closes on its success.
type breaker struct {
	threshold int
	openFor   time.Duration
	now       func() time.Time

	mu       sync.Mutex
	state    breakerState
	failures int
	openedAt time.Time
	trial    bool // a half-open trial call is out
}

func newBreaker(threshold int, openFor time.Duration) *breaker {
	return &breaker{threshold: threshold, openFor: openFor, now: time.Now}
}

// Whether a call may go out now. Every true must be followed by done.
func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case closed:
		return true
	case open:
		if b.now().Sub(b.openedAt) < b.openFor {
			return false
		}

		b.state, b.trial = halfOpen, true

		return true
	case halfOpen:
		if b.trial {
			return false
		}

		b.trial = true

		return true
	}

	return false
}

// Records the result of an allowed call.
func (b *breaker) done(ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.trial = false

	if ok {
		b.state, b.failures = closed, 0

		return
	}

	b.failures++
	if b.state == halfOpen || b.failures >= b.threshold {
		b.state, b.openedAt = open, b.now()
	}
}

func (b *breaker) current() breakerState {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.state
}
