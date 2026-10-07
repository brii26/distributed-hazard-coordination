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
	"go.opentelemetry.io/otel"
)

const (
	// ponytail: fixed, not config. At ~1 KB an event that is ~1 MB. Make it config if the broker may stay down for long.
	defaultPublishBuffer = 1000

	defaultExchange = "hazard.events"
	rabbitDialWait  = 2 * time.Second
	confirmWait     = 3 * time.Second
	flushWait       = 3 * time.Second
	livenessCheck   = time.Second
)

var errNacked = errors.New("broker nacked the message")

// One HazardEvent waiting to be published, already encoded.
type message struct {
	seq           uint64
	hazardID      string
	correlationID string
	body          []byte
	trace         amqpCarrier // the publishing span's context, sent as AMQP headers
}

// One live broker connection. publish returns only after the broker confirms.
type session interface {
	publish(ctx context.Context, m message) error
	alive() bool
	close()
}

// Gets events to the broker without holding up the caller. publish only appends to a bounded buffer, run drains it in order with confirms and reconnects with a growing delay.
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

// Queues h and returns at once. A full buffer drops the oldest message (its event is still in the Canonical Store).
func (p *publisher) publish(ctx context.Context, h HazardEvent, correlationID string) {
	body, err := json.Marshal(h)
	if err != nil {
		withID(correlationID).Warn("publish", "hazard_id", h.HazardID, "result", "failure", "error", err.Error())

		return
	}

	carrier := amqpCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	p.mu.Lock()

	if len(p.queue) >= p.capacity {
		withID(p.queue[0].correlationID).Warn("publish", "hazard_id", p.queue[0].hazardID, "result", "buffer-drop", "reason", "buffer_full")

		p.queue = p.queue[1:]
	}

	p.nextSeq++
	p.queue = append(p.queue, message{seq: p.nextSeq, hazardID: h.HazardID, correlationID: correlationID, body: body, trace: carrier})
	p.mu.Unlock()

	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Whether a broker connection is open right now, for the health check.
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

// Removes m once confirmed. If the buffer already dropped it, nothing is removed.
func (p *publisher) done(m message) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.queue) > 0 && p.queue[0].seq == m.seq {
		p.queue = p.queue[1:]
	}
}

// Publishes the oldest message and logs the result.
// ponytail: one message in flight at a time, so throughput is bounded by the confirm round trip. Fine at polling rates, batch confirms if not.
func (p *publisher) send(ctx context.Context, s session, m message) error {
	if err := s.publish(ctx, m); err != nil {
		withID(m.correlationID).Warn("publish", "hazard_id", m.hazardID, "result", "failure", "error", err.Error())

		return err
	}

	p.done(m)
	withID(m.correlationID).Info("publish", "hazard_id", m.hazardID, "result", "success")

	return nil
}

// Publishes until ctx is done, then one bounded flush. A failed connection is redialed with a growing delay and the same message is retried.
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
				continue
			}

			s = p.lose(s)
			failures++

			p.wait(ctx, failures)

			continue
		}

		failures = 0
	}
}

// Drops a connection that failed or went away.
func (p *publisher) lose(s session) session {
	s.close()
	p.up.Store(false)
	log.Print("rabbitmq disconnected")

	return nil
}

// Sleeps the growing retry delay, or less if ctx ends.
func (p *publisher) wait(ctx context.Context, failures int) {
	select {
	case <-ctx.Done():
	case <-time.After(min(p.retryBase*time.Duration(1<<min(failures-1, 10)), p.retryMax)):
	}
}

// Tries to send what is left in the buffer at shutdown, for at most flushWait. The rest stays safe in the Canonical Store.
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

// A session over a real AMQP connection with publisher confirms on.
type amqpSession struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	exchange string
}

// Dial func for the broker at url, publishing to exchange. The fanout exchange itself comes from the broker's definitions.
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

// Sends m persistent (delivery_mode=2) with the correlation ID as an AMQP property, not in the payload, then waits for the confirm.
func (s *amqpSession) publish(ctx context.Context, m message) (err error) {
	start := time.Now()
	ctx = otel.GetTextMapPropagator().Extract(ctx, m.trace)
	ctx = startCall(withCorrelation(ctx, m.correlationID), "rabbitmq publish exchange="+s.exchange)

	defer func() { logCall(ctx, "rabbitmq publish exchange="+s.exchange, start, err) }()

	headers := amqpCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, headers)

	conf, err := s.ch.PublishWithDeferredConfirmWithContext(ctx, s.exchange, "", false, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		CorrelationId: m.correlationID,
		MessageId:     m.hazardID,
		Headers:       amqp.Table(headers),
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
