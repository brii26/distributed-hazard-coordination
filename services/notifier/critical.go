package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultCriticalQueue = "critical-alerts"
	criticalRequeueWait  = 100 * time.Millisecond
	alertReplay          = 200 // alerts kept for clients that reconnect
	subBuffer            = 64
	streamPing           = 15 * time.Second
)

// Latency buckets in seconds, around the 5 second target.
var latencyBuckets = []float64{0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 30, 60}

// Counters and the latency histogram for critical alerts.
type criticalMetrics struct {
	seen, delivered, duplicates, malformed atomic.Int64

	mu      sync.Mutex
	buckets []int64 // cumulative counts per latencyBuckets, last is +Inf
	sum     float64
}

func newCriticalMetrics() *criticalMetrics {
	return &criticalMetrics{buckets: make([]int64, len(latencyBuckets)+1)}
}

func (m *criticalMetrics) observe(d time.Duration) {
	s := max(d.Seconds(), 0)

	m.mu.Lock()
	defer m.mu.Unlock()

	for i, b := range latencyBuckets {
		if s <= b {
			m.buckets[i]++
		}
	}

	m.buckets[len(latencyBuckets)]++
	m.sum += s
}

func (m *criticalMetrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	_, _ = fmt.Fprintf(w, "# HELP critical_events_seen_total Critical observations received, duplicates excluded.\n# TYPE critical_events_seen_total counter\ncritical_events_seen_total %d\n"+
		"# HELP critical_alerts_delivered_total Critical alerts delivered to Tim Lapangan.\n# TYPE critical_alerts_delivered_total counter\ncritical_alerts_delivered_total %d\n"+
		"# HELP critical_alerts_duplicates_total Redelivered critical observations acked without a second alert.\n# TYPE critical_alerts_duplicates_total counter\ncritical_alerts_duplicates_total %d\n"+
		"# HELP critical_alerts_malformed_total Critical messages that could not be read.\n# TYPE critical_alerts_malformed_total counter\ncritical_alerts_malformed_total %d\n",
		m.seen.Load(), m.delivered.Load(), m.duplicates.Load(), m.malformed.Load())

	m.mu.Lock()
	defer m.mu.Unlock()

	_, _ = fmt.Fprint(w, "# HELP critical_alert_latency_seconds Time from the sensor response (ingest) to delivery.\n# TYPE critical_alert_latency_seconds histogram\n")

	for i, b := range latencyBuckets {
		_, _ = fmt.Fprintf(w, "critical_alert_latency_seconds_bucket{le=\"%g\"} %d\n", b, m.buckets[i])
	}

	_, _ = fmt.Fprintf(w, "critical_alert_latency_seconds_bucket{le=\"+Inf\"} %d\ncritical_alert_latency_seconds_sum %g\ncritical_alert_latency_seconds_count %d\n",
		m.buckets[len(latencyBuckets)], m.sum, m.buckets[len(latencyBuckets)])
}

// One alert as a server-sent event.
type alertFrame struct {
	id   string
	data []byte
}

// Pushes alerts to the connected Tim Lapangan streams and keeps the latest few so a client that reconnects with Last-Event-ID misses none.
type alertHub struct {
	mu   sync.Mutex
	subs map[chan alertFrame]struct{}
	ring []alertFrame
}

func newAlertHub() *alertHub { return &alertHub{subs: map[chan alertFrame]struct{}{}} }

// Never blocks. A client too slow to keep up is cut off and reconnects, replaying from the ring.
func (h *alertHub) publish(f alertFrame) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.ring = append(h.ring, f); len(h.ring) > alertReplay {
		h.ring = h.ring[1:]
	}

	for ch := range h.subs {
		select {
		case ch <- f:
		default:
			delete(h.subs, ch)
			close(ch)
		}
	}
}

func (h *alertHub) subscribe(lastID string) (replay []alertFrame, ch chan alertFrame, cancel func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if i := slices.IndexFunc(h.ring, func(f alertFrame) bool { return f.id == lastID }); lastID != "" && i >= 0 {
		replay = slices.Clone(h.ring[i+1:])
	}

	ch = make(chan alertFrame, subBuffer)
	h.subs[ch] = struct{}{}

	return replay, ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()

		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
	}
}

func (h *alertHub) subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.subs)
}

// GET /alerts/stream: the Tim Lapangan alert stream, which the Client-Facing API relays.
func (h *alertHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)

		return
	}

	replay, ch, cancel := h.subscribe(r.Header.Get("Last-Event-ID"))
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	if _, err := fmt.Fprint(w, ": open\n\n"); err != nil || rc.Flush() != nil {
		return
	}

	write := func(f alertFrame) bool {
		_, err := fmt.Fprintf(w, "id: %s\nevent: alert\ndata: %s\n\n", f.id, f.data)

		return err == nil && rc.Flush() == nil
	}

	for _, f := range replay {
		if !write(f) {
			return
		}
	}

	ping := time.NewTicker(streamPing)
	defer ping.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case f, ok := <-ch:
			if !ok || !write(f) {
				return
			}
		case <-ping.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil || rc.Flush() != nil {
				return
			}
		}
	}
}

// Alerts Tim Lapangan once per observation_id, from the critical-alerts queue.
type criticalAlerts struct {
	dedup   *dedupStore
	hub     *alertHub
	metrics *criticalMetrics
	now     func() time.Time
	// Writes the alert to the notifier log, a field so tests can watch it.
	logSent func(e HazardEvent, correlationID string)
}

func newCriticalAlerts(d *dedupStore, hub *alertHub, m *criticalMetrics) *criticalAlerts {
	return &criticalAlerts{dedup: d, hub: hub, metrics: m, now: time.Now, logSent: logCritical}
}

func logCritical(e HazardEvent, correlationID string) {
	withID(correlationID).Info("critical alert sent", "observation_id", e.SourceRefID, "hazard_id", e.HazardID, "severity", orDash(e.Severity), "area", orDash(e.AreaName))
}

// Delivery is the log line plus the push to every open stream, both done before the ack. The dedup record is written after delivery, so a crash between the two can alert twice (the stream id lets a client drop the repeat) and never zero times.
func (c *criticalAlerts) handle(m message) outcome {
	var e HazardEvent
	if err := json.Unmarshal(m.body, &e); err != nil || e.SourceRefID == "" {
		c.metrics.malformed.Add(1)
		withID(m.correlationID).Error("critical message received", "result", "rejected", "reason", "malformed")

		return drop
	}

	k := dedupKey{e.SourceRefID, "critical"}
	withID(m.correlationID).Info("critical message received", "observation_id", e.SourceRefID)

	if c.dedup.has(k) {
		c.metrics.duplicates.Add(1)
		withID(m.correlationID).Info("critical message processed", "observation_id", e.SourceRefID, "result", "skipped", "reason", "duplicate")

		return ack
	}

	c.metrics.seen.Add(1)
	c.logSent(e, m.correlationID)
	c.hub.publish(alertFrame{id: e.SourceRefID, data: slices.Clone(m.body)})

	if err := c.dedup.record(k); err != nil {
		c.metrics.seen.Add(-1)
		withID(m.correlationID).Warn("dedup record failed", "observation_id", e.SourceRefID, "error", err)

		return requeue
	}

	c.metrics.delivered.Add(1)
	c.metrics.observe(c.now().Sub(e.IngestedAt))
	withID(m.correlationID).Info("critical message processed", "observation_id", e.SourceRefID, "result", "processed", "latency_ms", c.now().Sub(e.IngestedAt).Milliseconds())

	return ack
}
