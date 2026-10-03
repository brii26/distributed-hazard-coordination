package main

import (
	"log"
	"sync"
	"time"
)

const (
	defaultCBFailureThreshold = 3
	defaultCBOpenDuration     = 10 * time.Second
)

const (
	statusUp       = "UP"
	statusDegraded = "DEGRADED"
	statusDown     = "DOWN"
)

type breakerState int

const (
	stateClosed breakerState = iota
	stateOpen
	stateHalfOpen
)

func (s breakerState) String() string {
	return [...]string{"CLOSED", "OPEN", "HALF-OPEN"}[s]
}

// sourceStatus is what the read API puts in its response meta (doc/TRD_M1.md
// 3.3). Times are pointers so "never" and "not stale" serialize as null.
type sourceStatus struct {
	Source        string     `json:"source"`
	Status        string     `json:"source_status"` // UP, DEGRADED or DOWN
	LastSuccessAt *time.Time `json:"last_success_at"`
	StaleSince    *time.Time `json:"stale_since"`

	ConsecutiveFail int `json:"-"`
}

// breaker is one source's circuit breaker plus the status tracked with it. The
// poller goroutine drives it (allow, success, failure) while other goroutines
// (the read API, /health) read snapshots, so all state sits behind the RWMutex.
// Each source has its own breaker, so one source being down never affects the
// other.
type breaker struct {
	name      string
	threshold int
	openFor   time.Duration
	now       func() time.Time

	mu              sync.RWMutex
	state           breakerState
	consecutiveFail int
	lastSuccess     time.Time
	staleSince      time.Time
	openedAt        time.Time
}

func newBreaker(name string, threshold int, openFor time.Duration) *breaker {
	return &breaker{name: name, threshold: threshold, openFor: openFor, now: time.Now}
}

// transition moves to state to and logs it; the caller holds the write lock.
func (b *breaker) transition(to breakerState) {
	log.Printf("circuit breaker source=%s transition=%s->%s consecutive_fail=%d", b.name, b.state, to, b.consecutiveFail)

	b.state = to
}

// open trips the breaker. stale_since is the last good poll; a source that never
// succeeded has no such time, so the moment it went down stands in for it.
func (b *breaker) open() {
	b.openedAt = b.now()

	b.staleSince = b.lastSuccess
	if b.staleSince.IsZero() {
		b.staleSince = b.openedAt
	}

	b.transition(stateOpen)
}

// allow reports whether the poller may call the source now. While OPEN it says
// no until openFor has passed, then lets exactly one trial call through
// (HALF-OPEN); until that trial reports back it says no again.
func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case stateOpen:
		if b.now().Sub(b.openedAt) < b.openFor {
			return false
		}

		b.transition(stateHalfOpen)

		return true
	case stateHalfOpen:
		return false
	case stateClosed:
		return true
	}

	return true
}

// success records a good poll: the breaker closes and the source is fresh again.
func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.consecutiveFail = 0
	b.lastSuccess = b.now()
	b.staleSince = time.Time{}

	if b.state != stateClosed {
		b.transition(stateClosed)
	}
}

// failure records a failed poll (an error or a timeout). A failed trial reopens
// the breaker; otherwise it opens once threshold failures happen in a row.
func (b *breaker) failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.consecutiveFail++

	if b.state == stateHalfOpen || (b.state == stateClosed && b.consecutiveFail >= b.threshold) {
		b.open()
	}
}

// snapshot is a consistent copy of the status for the read API and /health.
// DEGRADED means polls are failing but the breaker has not tripped yet.
func (b *breaker) snapshot() sourceStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()

	s := sourceStatus{Source: b.name, Status: statusUp, ConsecutiveFail: b.consecutiveFail}

	switch {
	case b.state != stateClosed:
		s.Status = statusDown
	case b.consecutiveFail > 0:
		s.Status = statusDegraded
	}

	if !b.lastSuccess.IsZero() {
		t := b.lastSuccess
		s.LastSuccessAt = &t
	}

	if !b.staleSince.IsZero() {
		t := b.staleSince
		s.StaleSince = &t
	}

	return s
}
