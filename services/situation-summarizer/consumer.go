package main

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	prefetch      = 64
	dialTimeout   = 5 * time.Second
	retryBase     = 200 * time.Millisecond
	retryMax      = 5 * time.Second
	heartbeatTime = 10 * time.Second
)

// Keeps one subscription alive: its own durable queue bound to the fanout exchange, manual ack after handle, reconnect with a growing delay. Nothing else on the alert or ingest path knows this queue exists.
type consumer struct {
	url, exchange, queue string
	// Gets every message body, never fails: a message that cannot be read is skipped.
	handle func(body []byte)

	up atomic.Bool
}

// Broker connection state, for /health.
func (c *consumer) connected() bool { return c.up.Load() }

// Consumes until ctx is done, reconnecting when the connection is lost.
func (c *consumer) run(ctx context.Context) {
	failures := 0

	for ctx.Err() == nil {
		established, err := c.session(ctx)
		c.up.Store(false)

		if ctx.Err() != nil {
			return
		}

		if established {
			failures = 0
		}

		failures++

		log.Printf("rabbitmq disconnected queue=%s: %v; reconnecting", c.queue, err)

		select {
		case <-ctx.Done():
		case <-time.After(min(retryBase*time.Duration(1<<min(failures-1, 10)), retryMax)):
		}
	}
}

// Declares the queue and its binding, the part the integration test checks against a real broker.
func (c *consumer) declare(ch *amqp.Channel) error {
	if _, err := ch.QueueDeclare(c.queue, true, false, false, false, nil); err != nil {
		return err
	}

	return ch.QueueBind(c.queue, "", c.exchange, false, nil)
}

// Runs one connection until it fails or ctx ends. established says whether it got as far as consuming.
func (c *consumer) session(ctx context.Context) (established bool, err error) {
	conn, err := amqp.DialConfig(c.url, amqp.Config{Heartbeat: heartbeatTime, Dial: amqp.DefaultDial(dialTimeout)})
	if err != nil {
		return false, err
	}

	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		return false, err
	}

	if err := c.declare(ch); err != nil {
		return false, err
	}

	if err := ch.Qos(prefetch, 0, false); err != nil {
		return false, err
	}

	deliveries, err := ch.Consume(c.queue, "", false, false, false, false, nil)
	if err != nil {
		return false, err
	}

	c.up.Store(true)
	log.Printf("rabbitmq connected queue=%s exchange=%s", c.queue, c.exchange)

	for {
		select {
		case <-ctx.Done():
			return true, nil
		case d, ok := <-deliveries:
			if !ok {
				return true, errors.New("delivery channel closed")
			}

			// The window is in memory and a lost event only skews one summary, so every message is acked, malformed ones included.
			c.handle(d.Body)

			if err := d.Ack(false); err != nil {
				log.Printf("ack failed delivery_tag=%d: %v", d.DeliveryTag, err)
			}
		}
	}
}
