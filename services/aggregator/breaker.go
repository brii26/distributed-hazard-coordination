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

// status source buat meta read API, waktu pakai pointer biar bisa null.
type sourceStatus struct {
	Source        string     `json:"source"`
	Status        string     `json:"source_status"` // UP, DEGRADED, atau DOWN
	LastSuccessAt *time.Time `json:"last_success_at"`
	StaleSince    *time.Time `json:"stale_since"`

	ConsecutiveFail int `json:"-"`
}

// circuit breaker satu source plus statusnya. Poller yang drive, goroutine lain baca snapshot lewat RWMutex.
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

// pindah state dan log, caller harus pegang write lock.
func (b *breaker) transition(to breakerState) {
	withID("").Info("circuit breaker transition", "source", b.name, "from", b.state.String(), "to", to.String(), "consecutive_fail", b.consecutiveFail)

	b.state = to
}

// trip breaker. stale_since diisi poll terakhir yang sukses, atau waktu down kalau belum pernah sukses.
func (b *breaker) open() {
	b.openedAt = b.now()

	b.staleSince = b.lastSuccess
	if b.staleSince.IsZero() {
		b.staleSince = b.openedAt
	}

	b.transition(stateOpen)
}

// boleh nggak poller manggil source sekarang. Saat OPEN nolak sampai openFor lewat, lalu loloskan satu trial call (HALF-OPEN).
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

// poll sukses: breaker close dan source fresh lagi.
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

// poll gagal (error atau timeout). Trial yang gagal reopen breaker, selain itu open abis threshold gagal berturut-turut.
func (b *breaker) failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.consecutiveFail++

	if b.state == stateHalfOpen || (b.state == stateClosed && b.consecutiveFail >= b.threshold) {
		b.open()
	}
}

// copy status yang konsisten buat read API dan /health. DEGRADED berarti poll gagal tapi breaker belum trip.
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
