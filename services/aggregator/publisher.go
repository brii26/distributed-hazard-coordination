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
	// ponytail: fixed, bukan config. Satu event ~1 KB jadi total ~1 MB, jadiin config kalau broker diperkirakan down lama.
	defaultPublishBuffer = 1000

	defaultExchange = "hazard.events"
	rabbitDialWait  = 2 * time.Second
	confirmWait     = 3 * time.Second
	flushWait       = 3 * time.Second
	livenessCheck   = time.Second
)

var errNacked = errors.New("broker nacked the message")

// satu HazardEvent yang menunggu dipublish, sudah di-encode.
type message struct {
	seq           uint64
	hazardID      string
	correlationID string
	body          []byte
	trace         amqpCarrier // context span publisher, dikirim sebagai header AMQP
}

// satu koneksi broker aktif, publish baru return setelah broker confirm.
type session interface {
	publish(ctx context.Context, m message) error
	alive() bool
	close()
}

// kirim event ke broker tanpa menahan caller: publish cuma append ke buffer terbatas, run drain berurutan dengan confirm dan reconnect dengan delay naik.
type publisher struct {
	dial                func(ctx context.Context) (session, error)
	capacity            int
	retryBase, retryMax time.Duration

	mu      sync.Mutex
	queue   []message // yang paling lama di depan
	nextSeq uint64
	wake    chan struct{} // sinyal ke run kalau antrean terisi

	up atomic.Bool
}

func newPublisher(dial func(context.Context) (session, error), capacity int) *publisher {
	return &publisher{dial: dial, capacity: capacity, retryBase: retryBase, retryMax: retryMax, wake: make(chan struct{}, 1)}
}

// antre h dan langsung return, buffer penuh membuang yang paling lama (event-nya tetap aman di Canonical Store).
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

// koneksi broker lagi terbuka atau nggak, buat health check.
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

// hapus m setelah confirm, kalau sudah terbuang dari buffer nggak ada yang dihapus.
func (p *publisher) done(m message) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.queue) > 0 && p.queue[0].seq == m.seq {
		p.queue = p.queue[1:]
	}
}

// publish pesan paling lama dan log hasilnya.
// ponytail: satu pesan in flight sekali jalan, throughput dibatasi confirm round trip. Cukup untuk interval polling, batch confirm kalau nggak cukup.
func (p *publisher) send(ctx context.Context, s session, m message) error {
	if err := s.publish(ctx, m); err != nil {
		withID(m.correlationID).Warn("publish", "hazard_id", m.hazardID, "result", "failure", "error", err.Error())

		return err
	}

	p.done(m)
	withID(m.correlationID).Info("publish", "hazard_id", m.hazardID, "result", "success")

	return nil
}

// publish sampai ctx selesai lalu satu flush terbatas, koneksi gagal di-redial dengan delay naik dan pesan yang sama dicoba lagi.
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

// buang koneksi yang gagal atau hilang.
func (p *publisher) lose(s session) session {
	s.close()
	p.up.Store(false)
	log.Print("rabbitmq disconnected")

	return nil
}

// tidur selama delay retry, atau lebih singkat kalau ctx selesai.
func (p *publisher) wait(ctx context.Context, failures int) {
	select {
	case <-ctx.Done():
	case <-time.After(min(p.retryBase*time.Duration(1<<min(failures-1, 10)), p.retryMax)):
	}
}

// coba kirim sisa buffer saat shutdown maksimal flushWait, sisanya aman di Canonical Store.
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

// session di atas koneksi AMQP asli dengan publisher confirms.
type amqpSession struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	exchange string
}

// dial function ke broker di url, publish ke exchange (fanout-nya dideklarasi definitions broker).
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

// kirim m persistent (delivery_mode=2), correlation ID jadi AMQP property bukan payload, lalu tunggu confirm.
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
