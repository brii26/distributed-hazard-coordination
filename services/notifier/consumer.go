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
	prefetch      = 16
	dialTimeout   = 5 * time.Second
	retryBase     = 200 * time.Millisecond
	retryMax      = 5 * time.Second
	requeueDelay  = time.Second
	heartbeatTime = 10 * time.Second
)

// message is one delivery as the handler sees it.
type message struct {
	body          []byte
	correlationID string // the AMQP correlation_id property, set by the Aggregator
}

// outcome is what to tell the broker about a message once it is handled.
type outcome int

const (
	ack     outcome = iota // fully processed
	requeue                // failed for now: deliver it again
	drop                   // can never be processed (malformed): do not redeliver
)

// consumer keeps one subscription alive (doc/TRD_M1.md 7, 8): its own durable
// queue bound to the fanout exchange, manual ack only after handle is done, and
// a lost connection redialled with a growing delay. The Aggregator never learns
// this consumer exists: it only publishes to the exchange.
type consumer struct {
	url, exchange, queue string
	handle               func(message) outcome

	up atomic.Bool
}

// connected reports the broker connection state, for /health.
func (c *consumer) connected() bool { return c.up.Load() }

// run consumes until ctx is done, reconnecting whenever the connection is lost.
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

// session runs one connection until it fails or ctx ends. established reports
// whether it got as far as consuming, so the retry delay resets after a real session.
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

	// The queue is ours and durable; the exchange belongs to the broker's own
	// definitions (infra/rabbitmq), so it is only bound to, never declared here.
	if _, err := ch.QueueDeclare(c.queue, true, false, false, false, nil); err != nil {
		return false, err
	}

	if err := ch.QueueBind(c.queue, "", c.exchange, false, nil); err != nil {
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

			c.dispatch(d)
		}
	}
}

// dispatch hands a delivery to handle, then acks or nacks it as handle decided.
func (c *consumer) dispatch(d amqp.Delivery) {
	var err error

	switch c.handle(message{body: d.Body, correlationID: d.CorrelationId}) {
	case ack:
		err = d.Ack(false)
		withID(d.CorrelationId).Info("ack sent", "delivery_tag", d.DeliveryTag)
	case requeue:
		time.Sleep(requeueDelay) // a short pause so a persistent failure does not spin
		err = d.Nack(false, true)
		withID(d.CorrelationId).Warn("nack sent", "requeue", "true", "delivery_tag", d.DeliveryTag)
	case drop:
		err = d.Nack(false, false)
		withID(d.CorrelationId).Info("nack sent", "requeue", "false", "delivery_tag", d.DeliveryTag)
	}

	if err != nil {
		log.Printf("ack/nack failed delivery_tag=%d: %v", d.DeliveryTag, err)
	}
}
