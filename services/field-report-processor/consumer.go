// The field-report-processor reads queued field reports and saves each one through the Aggregator, acking only after it is stored.
package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	prefetch     = 16
	workers      = 4
	dialTimeout  = 5 * time.Second
	redialBase   = 200 * time.Millisecond
	redialMax    = 5 * time.Second
	drainTimeout = 20 * time.Second
	heartbeat    = 10 * time.Second
	queueName    = "field-reports"
	consumerTag  = "field-report-processor"
)

// Keeps one subscription on the field-reports queue alive: manual acks, a few workers, reconnect with a growing delay.
type consumer struct {
	url  string
	proc *processor

	up atomic.Bool
}

func (c *consumer) connected() bool { return c.up.Load() }

// Consumes until ctx is done, redialing when the connection is lost.
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

		slog.Warn("rabbitmq disconnected, reconnecting", "error", err.Error())

		select {
		case <-ctx.Done():
		case <-time.After(min(redialBase<<min(failures-1, 10), redialMax)):
		}
	}
}

// Runs one connection until it fails or ctx ends. On shutdown it stops new deliveries and gives in-flight saves drainTimeout to finish; whatever is not acked stays in the queue.
func (c *consumer) session(ctx context.Context) (established bool, err error) {
	conn, err := amqp.DialConfig(c.url, amqp.Config{Heartbeat: heartbeat, Dial: amqp.DefaultDial(dialTimeout)})
	if err != nil {
		return false, err
	}

	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		return false, err
	}

	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return false, err
	}

	if err := ch.Qos(prefetch, 0, false); err != nil {
		return false, err
	}

	deliveries, err := ch.Consume(queueName, consumerTag, false, false, false, false, nil)
	if err != nil {
		return false, err
	}

	c.up.Store(true)
	slog.Info("rabbitmq connected", "queue", queueName)

	// In-flight saves get their own context so a shutdown does not cut them off.
	workCtx, stopWork := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWork()

	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			for d := range deliveries {
				c.proc.handle(workCtx, job{
					id: d.MessageId, correlationID: d.CorrelationId, body: d.Body,
					ack:  func() error { return d.Ack(false) },
					nack: func(requeue bool) error { return d.Nack(false, requeue) },
				})
			}
		})
	}

	done := make(chan struct{})

	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		return true, errors.New("delivery channel closed")
	case <-ctx.Done():
		_ = ch.Cancel(consumerTag, false)

		select {
		case <-done:
		case <-time.After(drainTimeout):
			stopWork()
		}

		return true, nil
	}
}
