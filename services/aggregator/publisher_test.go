package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// pengganti RabbitMQ: mencatat yang di-confirm dan bisa disuruh menolak koneksi atau gagal publish.
type fakeBroker struct {
	mu            sync.Mutex
	got           []message
	failDials     int // tolak sekian koneksi dulu
	failPublishes int // gagalkan sekian publish dulu, seperti koneksi putus
	dials         int
	dead          bool // broker hilang: session terbuka lapor closed, koneksi baru ditolak
}

type fakeSession struct{ b *fakeBroker }

func (b *fakeBroker) dial(context.Context) (session, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.dials++

	if b.dead {
		return nil, errors.New("connection refused")
	}

	if b.failDials > 0 {
		b.failDials--

		return nil, errors.New("connection refused")
	}

	return fakeSession{b}, nil
}

func (s fakeSession) publish(_ context.Context, m message) error {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()

	if s.b.failPublishes > 0 {
		s.b.failPublishes--

		return errors.New("connection lost")
	}

	s.b.got = append(s.b.got, m)

	return nil
}

func (s fakeSession) alive() bool {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()

	return !s.b.dead
}

func (fakeSession) close() {}

func (b *fakeBroker) hazardIDs() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	var ids []string
	for _, m := range b.got {
		ids = append(ids, m.hazardID)
	}

	return ids
}

func event(id string) HazardEvent {
	h := hazard(sourcePVMBG, "ref-"+id)
	h.HazardID = id

	return h
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()

	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}

	t.Fatalf("timed out waiting for %s", what)
}

// jalankan p sampai test selesai, return fungsi untuk menghentikan dan menunggu.
func start(t *testing.T, p *publisher) (stop func()) {
	t.Helper()

	p.retryBase, p.retryMax = time.Millisecond, 5*time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Go(func() { p.run(ctx) })

	stop = func() {
		cancel()
		wg.Wait()
	}

	t.Cleanup(stop)

	return stop
}

func TestPublishNeverBlocksAndKeepsTheNewestWhenTheBrokerIsGone(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{failDials: 1 << 30}
	p := newPublisher(b.dial, 3)
	start(t, p)

	done := make(chan struct{})

	go func() {
		defer close(done)

		for i := range 50 {
			p.publish(context.Background(), event(strconv.Itoa(i)), "c")
		}
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publish blocked while the broker was down")
	}

	var kept []string

	p.mu.Lock()
	for _, m := range p.queue {
		kept = append(kept, m.hazardID)
	}
	p.mu.Unlock()

	if fmt.Sprint(kept) != "[47 48 49]" {
		t.Fatalf("expected the 3 newest events kept and the oldest dropped, got %v", kept)
	}
}

func TestMessagesGoOutInOrderWithTheirCorrelationID(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{}
	p := newPublisher(b.dial, 10)
	start(t, p)

	for _, id := range []string{"a", "b", "c"} {
		p.publish(context.Background(), event(id), "cycle-"+id)
	}

	eventually(t, "three confirmed messages", func() bool { return len(b.hazardIDs()) == 3 })

	if got := fmt.Sprint(b.hazardIDs()); got != "[a b c]" {
		t.Fatalf("expected order a b c, got %s", got)
	}

	var body map[string]any
	if err := json.Unmarshal(b.got[1].body, &body); err != nil || body["hazard_id"] != "b" {
		t.Fatalf("payload is not the event: %v %v", body, err)
	}

	if b.got[1].correlationID != "cycle-b" {
		t.Fatalf("correlation ID %q, want cycle-b", b.got[1].correlationID)
	}

	if _, leaked := body["correlation_id"]; leaked {
		t.Fatal("the correlation ID must travel as an AMQP property, not in the payload")
	}
}

func TestBufferedMessagesAreRetriedOnceTheBrokerIsBack(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{failDials: 5}
	p := newPublisher(b.dial, 10)
	start(t, p)

	p.publish(context.Background(), event("a"), "c")
	p.publish(context.Background(), event("b"), "c")

	eventually(t, "both messages after the outage", func() bool { return len(b.hazardIDs()) == 2 })

	if got := fmt.Sprint(b.hazardIDs()); got != "[a b]" {
		t.Fatalf("expected a b once each, got %s", got)
	}
}

func TestAFailedPublishIsRetriedOnANewConnectionWithoutLossOrDuplicate(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{failPublishes: 2}
	p := newPublisher(b.dial, 10)
	start(t, p)

	p.publish(context.Background(), event("a"), "c")
	p.publish(context.Background(), event("b"), "c")

	eventually(t, "both messages", func() bool { return len(b.hazardIDs()) == 2 })

	b.mu.Lock()
	defer b.mu.Unlock()

	if got := b.got[0].hazardID + " " + b.got[1].hazardID; got != "a b" || b.dials < 3 {
		t.Fatalf("expected a then b once each over fresh connections, got %s after %d dials", got, b.dials)
	}
}

func TestConfirmedMessageIsNotRemovedIfTheBufferAlreadyDroppedIt(t *testing.T) {
	t.Parallel()

	p := newPublisher((&fakeBroker{}).dial, 2)
	p.publish(context.Background(), event("a"), "c")
	p.publish(context.Background(), event("b"), "c")

	inFlight, _ := p.front()

	p.publish(context.Background(), event("c"), "c")

	p.done(inFlight)

	if len(p.queue) != 2 || p.queue[0].hazardID != "b" || p.queue[1].hazardID != "c" {
		t.Fatalf("a late confirm for a dropped message must remove nothing, queue is %v", p.queue)
	}
}

func TestConnectedFollowsTheConnection(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{}
	p := newPublisher(b.dial, 10)
	start(t, p)

	eventually(t, "connected", p.connected)

	b.mu.Lock()
	b.dead = true
	b.mu.Unlock()

	eventually(t, "disconnected once the connection is closed", func() bool { return !p.connected() })
}

func TestShutdownFlushesWhatIsBuffered(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{}
	p := newPublisher(b.dial, 10)
	stop := start(t, p)

	eventually(t, "connected", p.connected)

	for i := range 5 {
		p.publish(context.Background(), event(strconv.Itoa(i)), "c")
	}

	stop()

	if got := len(b.hazardIDs()); got != 5 {
		t.Fatalf("expected all 5 flushed at shutdown, got %d", got)
	}
}

// butuh RabbitMQ di RABBITMQ_TEST_URL, skip kalau nggak ada. Cek pesan persistent, correlation ID jadi property, dan di-confirm broker.
func TestRealBroker(t *testing.T) { //nolint:gocyclo // one broker scenario read top to bottom
	t.Parallel()

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

	exchange := fmt.Sprintf("test.hazard.%d", time.Now().UnixNano())

	if err := ch.ExchangeDeclare(exchange, "fanout", false, true, false, false, nil); err != nil {
		t.Fatal(err)
	}

	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := ch.QueueBind(q.Name, "", exchange, false, nil); err != nil {
		t.Fatal(err)
	}

	deliveries, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	p := newPublisher(dialAMQP(url, exchange), 10)
	start(t, p)
	p.publish(context.Background(), event("real-1"), "cycle-9")

	select {
	case d := <-deliveries:
		if d.DeliveryMode != amqp.Persistent || d.CorrelationId != "cycle-9" || d.MessageId != "real-1" || d.ContentType != "application/json" {
			t.Fatalf("unexpected properties: %+v", d)
		}

		var h HazardEvent
		if err := json.Unmarshal(d.Body, &h); err != nil || h.HazardID != "real-1" {
			t.Fatalf("bad payload %s: %v", d.Body, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no message arrived")
	}

	if !p.connected() {
		t.Fatal("expected connected while the broker is up")
	}
}
