package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// The queue binding against a real RabbitMQ at RABBITMQ_TEST_URL, skipped otherwise.
//
//nolint:paralleltest // needs the real broker
func TestRealBrokerQueueIsBoundToTheFanoutAndGetsEveryEvent(t *testing.T) {
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
	exchange, queue, other := fmt.Sprintf("test.hazard.%d", run), fmt.Sprintf("test.summarizer.%d", run), fmt.Sprintf("test.other.%d", run)

	if err := ch.ExchangeDeclare(exchange, "fanout", false, true, false, false, nil); err != nil {
		t.Fatal(err)
	}

	// Another consumer's queue on the same exchange, to show the summarizer takes a copy and does not compete for messages.
	if _, err := ch.QueueDeclare(other, false, true, false, false, nil); err != nil {
		t.Fatal(err)
	}

	if err := ch.QueueBind(other, "", exchange, false, nil); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _, _ = ch.QueueDelete(queue, false, false, false) })

	var got atomic.Int64

	c := &consumer{url: url, exchange: exchange, queue: queue, handle: func([]byte) { got.Add(1) }}
	ctx, cancel := context.WithCancel(context.Background())

	t.Cleanup(cancel)

	go c.run(ctx)

	deadline := time.Now().Add(15 * time.Second)
	for !c.connected() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	if !c.connected() {
		t.Fatal("consumer never connected")
	}

	// The queue exists, is durable and has its own name: a passive declare with the same arguments fails otherwise.
	ch2, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := ch2.QueueDeclarePassive(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("queue %s is not a durable queue: %v", queue, err)
	}

	for i := range 5 {
		if err := ch.PublishWithContext(context.Background(), exchange, "", false, false, amqp.Publishing{Body: event(fmt.Sprint(i), "Padang", "AWAS")}); err != nil {
			t.Fatal(err)
		}
	}

	for deadline := time.Now().Add(15 * time.Second); got.Load() < 5 && time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
	}

	if got.Load() != 5 {
		t.Fatalf("summarizer queue received %d of 5 events", got.Load())
	}

	if q, err := ch.QueueInspect(other); err != nil || q.Messages != 5 {
		t.Fatalf("other queue holds %+v err %v, want its own 5 copies", q, err)
	}
}
