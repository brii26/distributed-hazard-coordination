package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// The Notifier against a real RabbitMQ at RABBITMQ_TEST_URL; skipped otherwise.

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if cond() {
			return
		}
	}

	t.Fatalf("timed out waiting for %s", what)
}

//nolint:paralleltest // needs the real broker
func TestRealBrokerAlertsOnceAndKeepsDedupAcrossARestart(t *testing.T) {
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

	run := time.Now().UnixNano()
	exchange, queue := fmt.Sprintf("test.hazard.%d", run), fmt.Sprintf("test.notifier.%d", run)

	if err := ch.ExchangeDeclare(exchange, "fanout", false, true, false, false, nil); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _, _ = ch.QueueDelete(queue, false, false, false) })

	publish := func(id, severity string) {
		b, _ := json.Marshal(HazardEvent{HazardID: id, Severity: &severity})
		if err := ch.PublishWithContext(context.Background(), exchange, "", false, false,
			amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, CorrelationId: "c-" + id, Body: b}); err != nil {
			t.Fatal(err)
		}
	}

	path := filepath.Join(t.TempDir(), "dedup.log")
	a := &alerts{}

	start := func() (stop func()) {
		d, err := openDedup(path)
		if err != nil {
			t.Fatal(err)
		}

		c := &consumer{url: url, exchange: exchange, queue: queue, handle: (&notifier{dedup: d, send: a.send}).handle}
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup

		wg.Go(func() { c.run(ctx) })
		waitFor(t, "connected", c.connected)

		return func() { cancel(); wg.Wait(); _ = d.close() }
	}

	stop := start()

	publish("h1", "SIAGA")
	publish("h1", "SIAGA") // at-least-once delivery: the same message twice
	publish("h1", "AWAS")  // an escalation
	waitFor(t, "two alerts", func() bool { return a.count() == 2 })

	stop()

	// Redelivered after a restart: the dedup state on disk still holds.
	publish("h1", "AWAS")

	stop = start()
	defer stop()

	waitFor(t, "the redelivery to be consumed", func() bool {
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)

		return err == nil && q.Messages == 0
	})

	time.Sleep(200 * time.Millisecond)

	if a.count() != 2 {
		t.Fatalf("expected exactly the SIAGA and AWAS alerts, got %v", a.sent)
	}
}
