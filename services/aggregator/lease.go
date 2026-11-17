package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

const (
	defaultLeaseTTL   = 15 * time.Second
	defaultLeaseRenew = 3 * time.Second
)

// Lease timing. A leader that cannot confirm a renew for 2 intervals stops working on its own.
type leaseConfig struct {
	ttl, renew time.Duration
}

func (c leaseConfig) deadline() time.Duration { return 2 * c.renew }

// Reads LEASE_TTL and LEASE_RENEW_INTERVAL and checks db timeout < renew < renew deadline < ttl.
func loadLeaseConfig() (leaseConfig, error) {
	ttl, err := envDuration("LEASE_TTL", defaultLeaseTTL)
	if err != nil {
		return leaseConfig{}, err
	}

	renew, err := envDuration("LEASE_RENEW_INTERVAL", defaultLeaseRenew)
	if err != nil {
		return leaseConfig{}, err
	}

	c := leaseConfig{ttl: ttl, renew: renew}

	if c.renew <= dbQueryTimeout || c.deadline() >= c.ttl {
		return leaseConfig{}, fmt.Errorf("LEASE_RENEW_INTERVAL %s must be above %s and twice it below LEASE_TTL %s", renew, dbQueryTimeout, ttl)
	}

	return c, nil
}

// Reads a positive Go duration env var, def if unset.
func envDuration(name string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid %s value: %q", name, raw)
	}

	return d, nil
}

// Where the lease lives. Expiry is judged by the database clock, never by the pod clock.
type leaseBackend interface {
	// ok is false while another holder's lease is still valid.
	acquire(ctx context.Context, source, holder string, ttl time.Duration) (epoch int64, ok bool, err error)
	// ok is false when the lease is gone: other holder, other epoch or expired.
	renew(ctx context.Context, source, holder string, epoch int64, ttl time.Duration) (ok bool, err error)
	release(ctx context.Context, source, holder string, epoch int64) error
}

// The lease in the canonical database. Each statement reads clock_timestamp() after it holds the row lock.
type pgLease struct{ db *sql.DB }

const (
	ensureLeaseSQL  = `INSERT INTO source_lease (source) VALUES ($1) ON CONFLICT (source) DO NOTHING`
	acquireLeaseSQL = `
UPDATE source_lease SET holder = $2::uuid, epoch = epoch + 1, expires_at = clock_timestamp() + make_interval(secs => $3::double precision)
WHERE source = $1 AND (holder IS NULL OR expires_at <= clock_timestamp())
RETURNING epoch`
	renewLeaseSQL = `
UPDATE source_lease SET expires_at = clock_timestamp() + make_interval(secs => $4::double precision)
WHERE source = $1 AND holder = $2::uuid AND epoch = $3 AND expires_at > clock_timestamp()`
	releaseLeaseSQL = `UPDATE source_lease SET holder = NULL, expires_at = NULL WHERE source = $1 AND holder = $2::uuid AND epoch = $3`
)

func (l pgLease) acquire(ctx context.Context, source, holder string, ttl time.Duration) (int64, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	if _, err := l.db.ExecContext(ctx, ensureLeaseSQL, source); err != nil {
		return 0, false, err
	}

	var epoch int64

	err := l.db.QueryRowContext(ctx, acquireLeaseSQL, source, holder, ttl.Seconds()).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}

	return epoch, err == nil, err
}

func (l pgLease) renew(ctx context.Context, source, holder string, epoch int64, ttl time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	res, err := l.db.ExecContext(ctx, renewLeaseSQL, source, holder, epoch, ttl.Seconds())
	if err != nil {
		return false, err
	}

	n, err := res.RowsAffected()

	return n == 1, err
}

func (l pgLease) release(ctx context.Context, source, holder string, epoch int64) error {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	_, err := l.db.ExecContext(ctx, releaseLeaseSQL, source, holder, epoch)

	return err
}

// Newest stored occurred_at of a source, zero if none. This is the poll cursor a new leader continues from.
func (s *store) lastOccurred(ctx context.Context, source string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	var t sql.NullTime
	if err := s.db.QueryRowContext(ctx, `SELECT max(occurred_at) FROM hazard_events WHERE source = $1`, source).Scan(&t); err != nil {
		return time.Time{}, err
	}

	return t.Time, nil
}

type leaseTransition int

const (
	leaseNoChange leaseTransition = iota
	leaseAcquired
	leaseLost
)

type epochKey struct{}

// Marks ctx as work of the lease with this epoch, the fencing token of every store write made with it.
func withLeaseEpoch(ctx context.Context, epoch int64) context.Context {
	return context.WithValue(ctx, epochKey{}, epoch)
}

// Epoch of the lease this context belongs to.
func leaseEpoch(ctx context.Context) (int64, bool) {
	e, ok := ctx.Value(epochKey{}).(int64)

	return e, ok
}

// Keeps one source's lease for this process: tries to acquire while a follower, renews while the leader.
type elector struct {
	source, holder string
	cfg            leaseConfig
	backend        leaseBackend
	now            func() time.Time

	// Only touched by the run goroutine.
	held   bool
	epoch  int64
	lastOK time.Time

	// Read by the metrics handler.
	isLeader  atomic.Int32
	token     atomic.Int64
	takeovers atomic.Int64
	fenced    atomic.Int64

	// Highest epoch a store rejected as stale, read by the run goroutine. wake nudges it.
	fencedEpoch atomic.Int64
	wake        chan struct{}
}

func newElector(source string, cfg leaseConfig, b leaseBackend) *elector {
	return &elector{source: source, holder: newHazardID(), cfg: cfg, backend: b, now: time.Now, wake: make(chan struct{}, 1)}
}

// Called when a store rejected a write made under this epoch. Counts it and, if the epoch is still the current term, makes the run loop drop the lease.
func (e *elector) fence(epoch int64) {
	e.fenced.Add(1)

	for {
		cur := e.fencedEpoch.Load()
		if epoch <= cur || e.fencedEpoch.CompareAndSwap(cur, epoch) {
			break
		}
	}

	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// One attempt: acquire as a follower, renew as the leader.
func (e *elector) step(ctx context.Context) leaseTransition {
	start := e.now()
	l := withID("-")

	if !e.held {
		epoch, ok, err := e.backend.acquire(ctx, e.source, e.holder, e.cfg.ttl)
		if err != nil {
			l.Warn("lease acquire failed", "source", e.source, "holder", e.holder, "error", err.Error())

			return leaseNoChange
		}

		if !ok {
			return leaseNoChange
		}

		e.held, e.epoch, e.lastOK = true, epoch, start
		e.isLeader.Store(1)
		e.token.Store(epoch)

		if epoch > 1 {
			e.takeovers.Add(1)
		}

		l.Info("lease acquired", "source", e.source, "holder", e.holder, "epoch", epoch, "takeover", epoch > 1)

		return leaseAcquired
	}

	if e.fencedEpoch.Load() >= e.epoch {
		l.Warn("lease lost", "source", e.source, "holder", e.holder, "epoch", e.epoch, "reason", "fenced")

		// Release so the next acquire gets a fresh epoch. It is conditional, so it does nothing if another holder owns the lease.
		if err := e.backend.release(context.WithoutCancel(ctx), e.source, e.holder, e.epoch); err != nil {
			l.Warn("lease release failed", "source", e.source, "epoch", e.epoch, "error", err.Error())
		}

		e.held = false
		e.isLeader.Store(0)

		return leaseLost
	}

	ok, err := e.backend.renew(ctx, e.source, e.holder, e.epoch, e.cfg.ttl)

	switch {
	case err == nil && ok:
		e.lastOK = start

		return leaseNoChange
	case err != nil && e.now().Sub(e.lastOK) < e.cfg.deadline():
		l.Warn("lease renew failed", "source", e.source, "holder", e.holder, "epoch", e.epoch, "error", err.Error())

		return leaseNoChange
	}

	reason := "rejected"
	if err != nil {
		reason = "renew deadline passed"
	}

	l.Warn("lease lost", "source", e.source, "holder", e.holder, "epoch", e.epoch, "reason", reason)

	e.held = false
	e.isLeader.Store(0)

	return leaseLost
}

// Runs work while the lease is held and releases the lease when ctx is done.
func (e *elector) run(ctx context.Context, work func(ctx context.Context)) {
	var (
		stopWork context.CancelFunc
		done     chan struct{}
	)

	stop := func() {
		if stopWork != nil {
			stopWork()
			<-done

			stopWork = nil
		}
	}

	t := time.NewTicker(e.cfg.renew)
	defer t.Stop()

	for {
		switch e.step(ctx) {
		case leaseAcquired:
			wctx, cancel := context.WithCancel(withLeaseEpoch(ctx, e.epoch))
			stopWork, done = cancel, make(chan struct{})

			go func() {
				defer close(done)

				work(wctx)
			}()
		case leaseLost:
			stop()
		case leaseNoChange:
		}

		select {
		case <-ctx.Done():
			stop()

			if e.held {
				rctx := context.WithoutCancel(ctx)
				if err := e.backend.release(rctx, e.source, e.holder, e.epoch); err != nil {
					withID("-").Warn("lease release failed", "source", e.source, "epoch", e.epoch, "error", err.Error())
				}

				e.held = false
				e.isLeader.Store(0)
			}

			return
		case <-t.C:
		case <-e.wake:
		}
	}
}

// Prometheus text for lease_is_leader, lease_token, lease_takeovers_total and fenced_writes_total, one series per source.
func leaseMetricsHandler(es []*elector) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		var b strings.Builder

		series := []struct {
			name, typ string
			val       func(*elector) int64
		}{
			{"lease_is_leader", "gauge", func(e *elector) int64 { return int64(e.isLeader.Load()) }},
			{"lease_token", "gauge", func(e *elector) int64 { return e.token.Load() }},
			{"lease_takeovers_total", "counter", func(e *elector) int64 { return e.takeovers.Load() }},
			{"fenced_writes_total", "counter", func(e *elector) int64 { return e.fenced.Load() }},
		}

		for _, s := range series {
			fmt.Fprintf(&b, "# TYPE %s %s\n", s.name, s.typ)

			for _, e := range es {
				fmt.Fprintf(&b, "%s{source=%q} %d\n", s.name, e.source, s.val(e))
			}
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")

		_, _ = io.WriteString(w, b.String())
	}
}
