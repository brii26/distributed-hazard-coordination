package main

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// Settings that keep an unhealthy Sensor Pantai from costing more than one request per poll interval, and from using more than
// its share of connections and time.
type sensorConfig struct {
	timeout       time.Duration // one call
	slowAfter     time.Duration // a call that answers 200 but takes longer counts as a failure
	cbThreshold   int
	cbOpenFor     time.Duration
	backoffBase   time.Duration
	backoffMax    time.Duration
	maxConcurrent int           // calls in flight at once
	maxPages      int           // pages read in one poll
	staleAfter    time.Duration // the sensor answers but its clock and newest observation have not moved for this long
}

const (
	defaultSensorTimeout       = time.Second
	defaultSensorSlow          = 800 * time.Millisecond
	defaultSensorCBOpenFor     = 10 * time.Second
	defaultSensorBackoffBase   = 3 * time.Second
	defaultSensorBackoffMax    = 30 * time.Second
	defaultSensorMaxConcurrent = 2
	defaultSensorStaleAfter    = 60 * time.Second
)

// Fills every field that is zero with its default.
func (c sensorConfig) orDefaults() sensorConfig {
	fill := func(d *time.Duration, def time.Duration) {
		if *d <= 0 {
			*d = def
		}
	}

	fill(&c.timeout, defaultSensorTimeout)
	fill(&c.slowAfter, min(defaultSensorSlow, c.timeout))
	fill(&c.cbOpenFor, defaultSensorCBOpenFor)
	fill(&c.backoffBase, defaultSensorBackoffBase)
	fill(&c.backoffMax, max(defaultSensorBackoffMax, c.backoffBase))
	fill(&c.staleAfter, defaultSensorStaleAfter)

	if c.cbThreshold <= 0 {
		c.cbThreshold = defaultCBFailureThreshold
	}

	if c.maxConcurrent <= 0 {
		c.maxConcurrent = defaultSensorMaxConcurrent
	}

	if c.maxPages <= 0 {
		c.maxPages = sensorMaxPages
	}

	return c
}

// Reads the sensor settings from env. Every value must be a positive integer.
func loadSensorConfig() (sensorConfig, error) {
	var (
		c     sensorConfig
		err   error
		first error
	)

	num := func(name string, def int) int {
		n, e := envPositiveInt(name, def)
		if e != nil && first == nil {
			first = e
		}

		return n
	}

	ms := func(name string, def time.Duration) time.Duration {
		return time.Duration(num(name, int(def/time.Millisecond))) * time.Millisecond
	}

	c.timeout = ms("SENSOR_TIMEOUT_MS", defaultSensorTimeout)
	c.slowAfter = ms("SENSOR_SLOW_MS", defaultSensorSlow)
	c.cbOpenFor = ms("SENSOR_CB_OPEN_MS", defaultSensorCBOpenFor)
	c.backoffBase = ms("SENSOR_BACKOFF_BASE_MS", defaultSensorBackoffBase)
	c.backoffMax = ms("SENSOR_BACKOFF_MAX_MS", defaultSensorBackoffMax)
	c.staleAfter = ms("SENSOR_STALE_AFTER_MS", defaultSensorStaleAfter)
	c.cbThreshold = num("SENSOR_CB_FAILURE_THRESHOLD", defaultCBFailureThreshold)
	c.maxConcurrent = num("SENSOR_MAX_CONCURRENT", defaultSensorMaxConcurrent)
	c.maxPages = num("SENSOR_MAX_PAGES", sensorMaxPages)

	if first != nil {
		return sensorConfig{}, first
	}

	if c.slowAfter > c.timeout {
		err = errors.New("SENSOR_SLOW_MS must not be above SENSOR_TIMEOUT_MS")
	} else if c.backoffMax < c.backoffBase {
		err = errors.New("SENSOR_BACKOFF_MAX_MS must not be below SENSOR_BACKOFF_BASE_MS")
	}

	return c, err
}

// Exponential delay with jitter between failed polls. The delay doubles with every failure in a row up to max, and the jitter
// takes off up to half of it, so the cap holds. Used by the poller goroutine only.
type backoff struct {
	base, max time.Duration
	rnd       func() float64 // in [0, 1)
	fails     int
	last      time.Duration
}

func newBackoff(base, max time.Duration) *backoff {
	return &backoff{base: base, max: max, rnd: rand.Float64}
}

// Counts one more failure and returns how long to wait.
func (b *backoff) failed() time.Duration {
	b.fails++

	d := b.max
	if shift := b.fails - 1; shift < 31 {
		d = min(b.base<<shift, b.max)
	}

	b.last = d - time.Duration(b.rnd()*float64(d)/2)

	return b.last
}

func (b *backoff) reset() {
	b.fails, b.last = 0, 0
}

func (s *sensorStats) setStale(v bool) {
	if s != nil {
		n := uint32(0)
		if v {
			n = 1
		}

		s.stale.Store(n)
	}
}

var errBulkheadFull = errors.New("sensor bulkhead full")

// Counters and gauges of the sensor path, for /metrics. Methods accept a nil receiver.
type sensorStats struct {
	requests, rejectedBreaker, rejectedBulkhead, slow atomic.Uint64
	stale                                             atomic.Uint32
	backoffNanos                                      atomic.Int64
	inFlight                                          func() int
	breaker                                           *breaker
}

func (s *sensorStats) request() {
	if s != nil {
		s.requests.Add(1)
	}
}

func (s *sensorStats) breakerRejected() {
	if s != nil {
		s.rejectedBreaker.Add(1)
	}
}

func (s *sensorStats) bulkheadRejected() {
	if s != nil {
		s.rejectedBulkhead.Add(1)
	}
}

func (s *sensorStats) slowAnswer() {
	if s != nil {
		s.slow.Add(1)
	}
}

func (s *sensorStats) setBackoff(d time.Duration) {
	if s != nil {
		s.backoffNanos.Store(int64(d))
	}
}

// Writes the series in the Prometheus text format.
func (s *sensorStats) writeMetrics(w io.Writer) {
	if s == nil {
		return
	}

	state := stateClosed
	if s.breaker != nil {
		state = s.breaker.currentState()
	}

	inFlight := 0
	if s.inFlight != nil {
		inFlight = s.inFlight()
	}

	_, _ = fmt.Fprintf(w, `# HELP aggregator_sensor_breaker_state Circuit breaker of the sensor: 0 closed, 1 open, 2 half open.
# TYPE aggregator_sensor_breaker_state gauge
aggregator_sensor_breaker_state %d
# HELP aggregator_sensor_requests_total Calls made to the sensor.
# TYPE aggregator_sensor_requests_total counter
aggregator_sensor_requests_total %d
# HELP aggregator_sensor_rejected_by_breaker_total Polls the open breaker turned away.
# TYPE aggregator_sensor_rejected_by_breaker_total counter
aggregator_sensor_rejected_by_breaker_total %d
# HELP aggregator_sensor_rejected_by_bulkhead_total Calls the full bulkhead turned away.
# TYPE aggregator_sensor_rejected_by_bulkhead_total counter
aggregator_sensor_rejected_by_bulkhead_total %d
# HELP aggregator_sensor_slow_responses_total Answers that took longer than the slow limit.
# TYPE aggregator_sensor_slow_responses_total counter
aggregator_sensor_slow_responses_total %d
# HELP aggregator_sensor_backoff_seconds Wait added after the last failed poll, 0 when healthy.
# TYPE aggregator_sensor_backoff_seconds gauge
aggregator_sensor_backoff_seconds %g
# HELP aggregator_sensor_stale 1 while the sensor answers but its data has stopped moving.
# TYPE aggregator_sensor_stale gauge
aggregator_sensor_stale %d
# HELP aggregator_sensor_in_flight Calls to the sensor running now.
# TYPE aggregator_sensor_in_flight gauge
aggregator_sensor_in_flight %d
`, int(state), s.requests.Load(), s.rejectedBreaker.Load(), s.rejectedBulkhead.Load(), s.slow.Load(),
		time.Duration(s.backoffNanos.Load()).Seconds(), s.stale.Load(), inFlight)
}
