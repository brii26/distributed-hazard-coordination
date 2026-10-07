package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// The consumer against a real RabbitMQ at RABBITMQ_TEST_URL, skipped otherwise.

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
func TestRealBrokerConsumesAcksAndCatchesUpAfterARestart(t *testing.T) {
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
	exchange, queue, other := fmt.Sprintf("test.hazard.%d", run), fmt.Sprintf("test.dashboard.%d", run), fmt.Sprintf("test.other.%d", run)

	if err := ch.ExchangeDeclare(exchange, "fanout", false, true, false, false, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := ch.QueueDeclare(other, false, true, false, false, nil); err != nil {
		t.Fatal(err)
	}

	if err := ch.QueueBind(other, "", exchange, false, nil); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _, _ = ch.QueueDelete(queue, false, false, false) })

	publish := func(e HazardEvent) {
		b, _ := json.Marshal(e)
		if err := ch.PublishWithContext(context.Background(), exchange, "", false, false,
			amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, CorrelationId: "c-" + e.HazardID, Body: b}); err != nil {
			t.Fatal(err)
		}
	}

	s := newState()
	start := func() (stop func()) {
		c := &consumer{url: url, exchange: exchange, queue: queue, handle: s.handle}
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup

		wg.Go(func() { c.run(ctx) })
		waitFor(t, "connected", c.connected)

		return func() { cancel(); wg.Wait() }
	}

	stop := start()

	publish(event("a", "SEISMIC", "NORMAL"))
	publish(event("b", "VOLCANIC", "SIAGA"))
	waitFor(t, "two events in the state", func() bool { return len(s.snapshot()) == 2 })

	stop()
	publish(event("c", "SEISMIC", "WASPADA"))
	publish(event("a", "SEISMIC", "NORMAL"))

	stop = start()
	defer stop()

	waitFor(t, "catching up after the restart", func() bool { return len(s.snapshot()) == 3 })

	waitFor(t, "every message acked", func() bool {
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)

		return err == nil && q.Messages == 0
	})

	if q, err := ch.QueueDeclarePassive(other, false, true, false, false, nil); err != nil || q.Messages != 4 {
		t.Fatalf("the other queue must hold all 4 messages, got %d %v", q.Messages, err)
	}
}
