package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func criticalMsg(t *testing.T, id string, ingested time.Time) message {
	t.Helper()

	sev := "AWAS"

	b, err := json.Marshal(HazardEvent{
		HazardID: "h-" + id, Source: sourceSensorPantai, SourceRefID: id, HazardType: "COASTAL", Severity: &sev,
		IngestedAt: ingested, Attributes: map[string]any{"critical": true},
	})
	if err != nil {
		t.Fatal(err)
	}

	return message{body: b, correlationID: "c-" + id}
}

func newTestCritical(t *testing.T, path string) (*criticalAlerts, *alertHub) {
	t.Helper()

	d, err := openDedup(path)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = d.close() })

	hub := newAlertHub()
	c := newCriticalAlerts(d, hub, newCriticalMetrics())
	c.now = func() time.Time { return t0.Add(1500 * time.Millisecond) }

	return c, hub
}

func TestCriticalAlertsOncePerObservationAndCountsMatch(t *testing.T) {
	t.Parallel()

	c, hub := newTestCritical(t, filepath.Join(t.TempDir(), "dedup.log"))
	_, ch, cancel := hub.subscribe("")

	defer cancel()

	for _, id := range []string{"o-1", "o-2", "o-1", "o-3", "o-2"} {
		if out := c.handle(criticalMsg(t, id, t0)); out != ack {
			t.Fatalf("%s: %v", id, out)
		}
	}

	if len(ch) != 3 {
		t.Fatalf("%d pushed, want 3", len(ch))
	}

	m := c.metrics
	if m.seen.Load() != 3 || m.delivered.Load() != 3 || m.duplicates.Load() != 2 {
		t.Fatalf("seen %d delivered %d duplicates %d", m.seen.Load(), m.delivered.Load(), m.duplicates.Load())
	}
}

func TestCriticalDedupSurvivesARestartAndAlertsStayBeforeAck(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "dedup.log")
	first, _ := newTestCritical(t, path)
	first.handle(criticalMsg(t, "o-1", t0))

	second, hub := newTestCritical(t, path)
	_, ch, cancel := hub.subscribe("")

	defer cancel()

	second.handle(criticalMsg(t, "o-1", t0))
	second.handle(criticalMsg(t, "o-2", t0))

	if len(ch) != 1 || second.metrics.duplicates.Load() != 1 {
		t.Fatalf("pushed %d duplicates %d, want the repeat skipped and o-2 alerted", len(ch), second.metrics.duplicates.Load())
	}
}

func TestCriticalMalformedIsDroppedAndCounted(t *testing.T) {
	t.Parallel()

	c, _ := newTestCritical(t, filepath.Join(t.TempDir(), "dedup.log"))

	for _, body := range []string{"not json", `{"hazard_id":"h"}`} {
		if out := c.handle(message{body: []byte(body)}); out != drop {
			t.Fatalf("%q: %v", body, out)
		}
	}

	if c.metrics.malformed.Load() != 2 {
		t.Fatal("malformed not counted")
	}
}

func TestCriticalRequeuesWhenTheDedupWriteFails(t *testing.T) {
	t.Parallel()

	c, _ := newTestCritical(t, filepath.Join(t.TempDir(), "dedup.log"))
	_ = c.dedup.file.Close()

	if out := c.handle(criticalMsg(t, "o-1", t0)); out != requeue {
		t.Fatalf("got %v, want requeue", out)
	}

	if c.metrics.seen.Load() != 0 || c.metrics.delivered.Load() != 0 {
		t.Fatal("a requeued message must not count")
	}
}

func TestLatencyHistogramAndMetricsText(t *testing.T) {
	t.Parallel()

	c, _ := newTestCritical(t, filepath.Join(t.TempDir(), "dedup.log"))
	c.handle(criticalMsg(t, "o-1", t0)) // 1.5 s

	rec := httptest.NewRecorder()
	c.metrics.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil))

	body := rec.Body.String()
	for _, want := range []string{
		"critical_events_seen_total 1", "critical_alerts_delivered_total 1",
		`critical_alert_latency_seconds_bucket{le="1"} 0`, `critical_alert_latency_seconds_bucket{le="2"} 1`,
		`critical_alert_latency_seconds_bucket{le="+Inf"} 1`, "critical_alert_latency_seconds_sum 1.5", "critical_alert_latency_seconds_count 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics lack %q in:\n%s", want, body)
		}
	}
}

func TestFanoutCopyOfACriticalEventIsNotAlertedTwice(t *testing.T) {
	t.Parallel()

	n, a := newTestNotifier(t, filepath.Join(t.TempDir(), "dedup.log"))

	if out := n.handle(criticalMsg(t, "o-1", t0)); out != ack || a.count() != 0 {
		t.Fatalf("outcome %v alerts %v, want a silent ack", out, a.sent)
	}
}

func TestHubReplaysAfterLastEventIDAndCutsSlowClients(t *testing.T) {
	t.Parallel()

	h := newAlertHub()
	for i := range 5 {
		h.publish(alertFrame{id: fmt.Sprint(i), data: []byte("{}")})
	}

	replay, _, cancel := h.subscribe("2")
	if len(replay) != 2 || replay[0].id != "3" {
		t.Fatalf("replay %v", replay)
	}

	cancel()

	if replay, _, c := h.subscribe("unknown"); len(replay) != 0 {
		t.Fatalf("unknown id must replay nothing, got %v", replay)
	} else {
		defer c()
	}

	_, slow, _ := h.subscribe("")

	for i := range subBuffer + 1 {
		h.publish(alertFrame{id: fmt.Sprint("s", i), data: []byte("{}")})
	}

	got := 0
	for range slow {
		got++
	}

	if got != subBuffer {
		t.Fatalf("slow client drained %d then closed, want %d", got, subBuffer)
	}
}

func TestStreamPushesAlertsOverSSE(t *testing.T) {
	t.Parallel()

	c, hub := newTestCritical(t, filepath.Join(t.TempDir(), "dedup.log"))
	srv := httptest.NewServer(hub)

	defer srv.Close()

	c.handle(criticalMsg(t, "o-0", t0))

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	req.Header.Set("Last-Event-ID", "none")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}

	go func() {
		for hub.subscribers() == 0 {
			time.Sleep(5 * time.Millisecond)
		}

		c.handle(criticalMsg(t, "o-1", t0))
	}()

	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "id: ") {
			if sc.Text() != "id: o-1" {
				t.Fatalf("got %q, want o-1", sc.Text())
			}

			return
		}
	}

	t.Fatal("stream ended without an alert")
}

// Needs RabbitMQ at RABBITMQ_TEST_URL, skips otherwise. Alerts published while no Notifier runs wait in the durable queue, deliveries a killed consumer never acked come back, and each observation is alerted once.
//
//nolint:paralleltest // needs the real broker
func TestRealBrokerCriticalSurvivesNotifierDowntime(t *testing.T) {
	url := os.Getenv("RABBITMQ_TEST_URL")
	if url == "" {
		t.Skip("RABBITMQ_TEST_URL not set")
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	queue := fmt.Sprintf("test.critical.%d", time.Now().UnixNano())

	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _, _ = ch.QueueDelete(queue, false, false, false) })

	const n = 30

	for i := range n {
		m := criticalMsg(t, fmt.Sprintf("o-%d", i), time.Now())
		if err := ch.PublishWithContext(context.Background(), "", queue, false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: m.body}); err != nil {
			t.Fatal(err)
		}
	}

	path := filepath.Join(t.TempDir(), "dedup.log")

	var (
		mu   sync.Mutex
		sent = map[string]int{}
	)

	// A notifier killed mid-flight: it takes deliveries and drops the connection without acking.
	victim, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}

	vch, err := victim.Channel()
	if err != nil {
		t.Fatal(err)
	}

	deliveries, err := vch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	for range 10 {
		<-deliveries
	}

	_ = victim.Close()

	d, err := openDedup(path)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = d.close() }()

	ca := newCriticalAlerts(d, newAlertHub(), newCriticalMetrics())
	ca.logSent = func(e HazardEvent, _ string) {
		mu.Lock()
		defer mu.Unlock()

		sent[e.SourceRefID]++
	}

	metrics := ca.metrics
	ctx, stop := context.WithCancel(context.Background())
	c := &consumer{url: url, queue: queue, handle: ca.handle, requeueWait: time.Millisecond}

	go c.run(ctx)

	defer stop()

	waitFor(t, "all alerted", func() bool { mu.Lock(); defer mu.Unlock(); return len(sent) == n })
	waitFor(t, "queue drained", func() bool {
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)

		return err == nil && q.Messages == 0 && q.Consumers == 1
	})

	mu.Lock()
	defer mu.Unlock()

	for id, c := range sent {
		if c != 1 {
			t.Errorf("%s alerted %d times", id, c)
		}
	}

	if metrics.seen.Load() != metrics.delivered.Load() {
		t.Fatalf("seen %d delivered %d", metrics.seen.Load(), metrics.delivered.Load())
	}
}
