package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// ponytail: fixed, not in the TRD's config list. At ~1 KB an event this is
	// about 1 MB; make it config if the broker is expected to stay down for long.
	defaultPublishBuffer = 1000

	defaultExchange = "hazard.events"
	rabbitDialWait  = 2 * time.Second
	confirmWait     = 3 * time.Second
	flushWait       = 3 * time.Second
	livenessCheck   = time.Second
)

var errNacked = errors.New("broker nacked the message")

// message is one HazardEvent waiting to be published, already encoded.
type message struct {
	seq           uint64
	hazardID      string
	correlationID string
	body          []byte
}

// session is one live broker connection. publish returns only once the broker has
// confirmed the message (doc/TRD_M1.md 3.5); any error means it did not.
type session interface {
	publish(ctx context.Context, m message) error
	alive() bool
	close()
}

// publisher gets events to the broker without ever holding up the caller
// (doc/TRD_M1.md 3.5, 8.4). publish only appends to a bounded in-memory buffer;
// run, in its own goroutine, drains the buffer in order, waiting for the broker's
// confirm on each message and reconnecting with a growing delay when it fails.
type publisher struct {
	dial                func(ctx context.Context) (session, error)
	capacity            int
	retryBase, retryMax time.Duration

	mu      sync.Mutex
	queue   []message // oldest first
	nextSeq uint64
	wake    chan struct{} // tells run the queue is no longer empty

	up atomic.Bool
}

func newPublisher(dial func(context.Context) (session, error), capacity int) *publisher {
	return &publisher{dial: dial, capacity: capacity, retryBase: retryBase, retryMax: retryMax, wake: make(chan struct{}, 1)}
}

// publish queues h and returns at once. With the buffer full the oldest message
// is dropped and logged: its event is still safe in the Canonical Store, only
// not re-published.
func (p *publisher) publish(h HazardEvent, correlationID string) {
	body, err := json.Marshal(h)
	if err != nil {
		log.Printf("publish hazard_id=%s result=failure err=%q", h.HazardID, err)

		return
	}

	p.mu.Lock()

	if len(p.queue) >= p.capacity {
		log.Printf("publish hazard_id=%s result=buffer-drop reason=buffer_full", p.queue[0].hazardID)

		p.queue = p.queue[1:]
	}

	p.nextSeq++
	p.queue = append(p.queue, message{seq: p.nextSeq, hazardID: h.HazardID, correlationID: correlationID, body: body})
	p.mu.Unlock()

	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// connected reports whether a broker connection is open right now, for the health check.
func (p *publisher) connected() bool {
	return p.up.Load()
}

func (p *publisher) front() (message, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.queue) == 0 {
		return message{}, false
	}

	return p.queue[0], true
}

// done removes m once it is confirmed. If the buffer dropped it meanwhile (it was
// the oldest) it is no longer at the front and nothing is removed.
func (p *publisher) done(m message) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.queue) > 0 && p.queue[0].seq == m.seq {
		p.queue = p.queue[1:]
	}
}

// send publishes the oldest message and logs how it went.
// ponytail: one message in flight at a time, so throughput is bounded by the
// confirm round trip; fine at poll-interval rates, batch confirms if it is not.
func (p *publisher) send(ctx context.Context, s session, m message) error {
	if err := s.publish(ctx, m); err != nil {
		log.Printf("publish hazard_id=%s correlation_id=%s result=failure err=%q", m.hazardID, m.correlationID, err)

		return err
	}

	p.done(m)
	log.Printf("publish hazard_id=%s correlation_id=%s result=success", m.hazardID, m.correlationID)

	return nil
}

// run publishes until ctx is done, then makes one bounded attempt to flush what
// is still buffered. A failed or lost connection is redialled with a growing
// delay and the same message is tried again, so nothing is lost short of the
// buffer limit.
func (p *publisher) run(ctx context.Context) {
	var s session

	failures := 0

	defer func() {
		if s != nil {
			p.flush(s)
			s.close()
		}

		p.up.Store(false)
	}()

	idle := time.NewTicker(livenessCheck)
	defer idle.Stop()

	for ctx.Err() == nil {
		if s == nil {
			var err error

			if s, err = p.dial(ctx); err != nil {
				log.Printf("rabbitmq connect failed: %v", err)

				failures++

				p.wait(ctx, failures)

				continue
			}

			log.Print("rabbitmq connected")
			p.up.Store(true)
		}

		m, ok := p.front()
		if !ok {
			select {
			case <-ctx.Done():
			case <-p.wake:
			case <-idle.C:
				if !s.alive() {
					s = p.lose(s)
				}
			}

			continue
		}

		if err := p.send(ctx, s, m); err != nil {
			if ctx.Err() != nil {
				continue // shutting down: keep the session for the final flush
			}

			s = p.lose(s)
			failures++

			p.wait(ctx, failures)

			continue
		}

		failures = 0
	}
}

// lose drops a connection that failed or went away.
func (p *publisher) lose(s session) session {
	s.close()
	p.up.Store(false)
	log.Print("rabbitmq disconnected")

	return nil
}

// wait sleeps the growing retry delay, or less if ctx ends.
func (p *publisher) wait(ctx context.Context, failures int) {
	select {
	case <-ctx.Done():
	case <-time.After(min(p.retryBase*time.Duration(1<<min(failures-1, 10)), p.retryMax)):
	}
}

// flush tries to send what is still buffered at shutdown, for at most flushWait.
// Whatever it cannot send stays safe in the Canonical Store.
func (p *publisher) flush(s session) {
	ctx, cancel := context.WithTimeout(context.Background(), flushWait)
	defer cancel()

	for {
		m, ok := p.front()
		if !ok || p.send(ctx, s, m) != nil {
			return
		}
	}
}

// amqpSession is a session over a real AMQP connection with publisher confirms on.
type amqpSession struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	exchange string
}

// dialAMQP returns a dial function for the broker at url, publishing to exchange.
// The fanout exchange itself is declared by the broker's own definitions
// (infra/rabbitmq), the Aggregator only needs its name.
func dialAMQP(url, exchange string) func(context.Context) (session, error) {
	return func(context.Context) (session, error) {
		conn, err := amqp.DialConfig(url, amqp.Config{Heartbeat: 10 * time.Second, Dial: amqp.DefaultDial(rabbitDialWait)})
		if err != nil {
			return nil, err
		}

		ch, err := conn.Channel()
		if err == nil {
			err = ch.Confirm(false)
		}

		if err != nil {
			_ = conn.Close()

			return nil, err
		}

		return &amqpSession{conn: conn, ch: ch, exchange: exchange}, nil
	}
}

// publish sends m persistent (delivery_mode=2) with the correlation ID as an AMQP
// property, not in the payload, and waits for the broker's confirm.
func (s *amqpSession) publish(ctx context.Context, m message) error {
	conf, err := s.ch.PublishWithDeferredConfirmWithContext(ctx, s.exchange, "", false, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		CorrelationId: m.correlationID,
		MessageId:     m.hazardID,
		Timestamp:     time.Now(),
		Body:          m.body,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, confirmWait)
	defer cancel()

	acked, err := conf.WaitContext(ctx)
	if err != nil {
		return err
	}

	if !acked {
		return errNacked
	}

	return nil
}

func (s *amqpSession) alive() bool { return !s.conn.IsClosed() }

func (s *amqpSession) close() { _ = s.conn.Close() }
