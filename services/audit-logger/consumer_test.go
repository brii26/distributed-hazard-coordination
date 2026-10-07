package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// The Audit Logger against a real RabbitMQ at RABBITMQ_TEST_URL, skipped otherwise.

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if cond() {
			return
		}
	}

	t.Fatalf("timed out waiting for %s", what)
}

// The exchange is already in use when the Audit Logger is added, and it receives what is published from then on.
//
//nolint:paralleltest // needs the real broker
func TestRealBrokerAddedLaterReceivesNewEvents(t *testing.T) {
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
	exchange, queue := fmt.Sprintf("test.hazard.%d", run), fmt.Sprintf("test.audit.%d", run)

	if err := ch.ExchangeDeclare(exchange, "fanout", false, true, false, false, nil); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _, _ = ch.QueueDelete(queue, false, false, false) })

	publish := func(id string) {
		if err := ch.PublishWithContext(context.Background(), exchange, "", false, false, amqp.Publishing{
			ContentType: "application/json", DeliveryMode: amqp.Persistent, CorrelationId: "c-" + id,
			Body: []byte(fmt.Sprintf(`{"hazard_id":%q,"severity":"SIAGA"}`, id)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	publish("before")

	dir := t.TempDir()

	dedup, audit, err := openStores(filepath.Join(dir, "dedup.log"), filepath.Join(dir, "audit.log"))
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = dedup.close(); _ = audit.close() }()

	c := &consumer{url: url, exchange: exchange, queue: queue, handle: (&auditor{log: audit, dedup: dedup}).handle}
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Go(func() { c.run(ctx) })

	defer func() { cancel(); wg.Wait() }()

	waitFor(t, "connected", c.connected)

	publish("after-1")
	publish("after-2")

	waitFor(t, "the new events in the audit log", func() bool { return len(lines(t, dir)) == 2 })

	for _, e := range lines(t, dir) {
		if e.CorrelationID == "c-before" {
			t.Fatal("an event published before the logger's queue existed cannot reach it")
		}
	}
}
