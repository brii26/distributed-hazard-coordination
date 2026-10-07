package main

import (
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

// Source state for the read API meta. Times are pointers so they can be null.
type sourceStatus struct {
	Source        string     `json:"source"`
	Status        string     `json:"source_status"` // UP, DEGRADED or DOWN
	LastSuccessAt *time.Time `json:"last_success_at"`
	StaleSince    *time.Time `json:"stale_since"`

	ConsecutiveFail int `json:"-"`
}

// One source's circuit breaker plus its status. The poller drives it, other goroutines read snapshots behind the RWMutex.
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

// Moves to a new state and logs it. The caller holds the write lock.
func (b *breaker) transition(to breakerState) {
	withID("").Info("circuit breaker transition", "source", b.name, "from", b.state.String(), "to", to.String(), "consecutive_fail", b.consecutiveFail)

	b.state = to
}

// Trips the breaker. stale_since is the last good poll, or the time it went down if it never succeeded.
func (b *breaker) open() {
	b.openedAt = b.now()

	b.staleSince = b.lastSuccess
	if b.staleSince.IsZero() {
		b.staleSince = b.openedAt
	}

	b.transition(stateOpen)
}

// Whether the poller may call the source now. While OPEN it says no until openFor passes, then lets one trial call through (HALF-OPEN).
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

// A good poll: the breaker closes and the source is fresh again.
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

// A failed poll (error or timeout). A failed trial reopens the breaker, otherwise it opens after threshold failures in a row.
func (b *breaker) failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.consecutiveFail++

	if b.state == stateHalfOpen || (b.state == stateClosed && b.consecutiveFail >= b.threshold) {
		b.open()
	}
}

// Consistent copy of the status for the read API and /health. DEGRADED means polls fail but the breaker hasn't tripped yet.
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
