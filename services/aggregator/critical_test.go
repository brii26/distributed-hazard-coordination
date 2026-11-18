package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// In memory outbox in insertion order.
type memOutbox struct {
	mu      sync.Mutex
	rows    []outboxRow
	done    map[string]bool
	addErr  error
	sentErr error
}

func (o *memOutbox) add(_ context.Context, id, cid string, body []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.addErr != nil {
		return o.addErr
	}

	for _, r := range o.rows {
		if r.id == id {
			return nil
		}
	}

	o.rows = append(o.rows, outboxRow{id: id, correlationID: cid, body: body})

	return nil
}

func (o *memOutbox) pending(context.Context, int) ([]outboxRow, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	var out []outboxRow

	for _, r := range o.rows {
		if !o.done[r.id] {
			out = append(out, r)
		}
	}

	return out, nil
}

func (o *memOutbox) sent(_ context.Context, id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.sentErr != nil {
		return o.sentErr
	}

	if o.done == nil {
		o.done = map[string]bool{}
	}

	o.done[id] = true

	return nil
}

// A broker for the critical queue that can be down and fails publishes on demand.
type critBroker struct {
	mu            sync.Mutex
	got           []string
	dead          bool
	failPublishes int
}

type critSession struct{ b *critBroker }

func (b *critBroker) dial(context.Context) (criticalSession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.dead {
		return nil, errors.New("connection refused")
	}

	return critSession{b}, nil
}

func (s critSession) publish(_ context.Context, r outboxRow) error {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()

	if s.b.dead {
		return errors.New("connection lost")
	}

	if s.b.failPublishes > 0 {
		s.b.failPublishes--

		return errors.New("nacked")
	}

	s.b.got = append(s.b.got, r.id)

	return nil
}

func (s critSession) alive() bool {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()

	return !s.b.dead
}

func (critSession) close() {}

func (b *critBroker) ids() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return slices.Clone(b.got)
}

func (b *critBroker) setDead(v bool) {
	b.mu.Lock()
	b.dead = v
	b.mu.Unlock()
}

func startRelay(t *testing.T, r *criticalRelay) {
	t.Helper()

	r.retryBase, r.retryMax = time.Millisecond, 5*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		r.run(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func critEvent(id string) HazardEvent {
	sev := severityAwas

	return HazardEvent{HazardID: "h-" + id, Source: sourceSensorPantai, SourceRefID: id, HazardType: typeCoastal, Severity: &sev, Attributes: map[string]any{"critical": true}}
}

func TestIsCritical(t *testing.T) {
	t.Parallel()

	plain := critEvent("a")
	plain.Attributes["critical"] = false

	other := critEvent("b")
	other.Source = sourceBMKG

	for name, tc := range map[string]struct {
		h    HazardEvent
		want bool
	}{"critical coastal": {critEvent("c"), true}, "not critical": {plain, false}, "other source": {other, false}, "no attribute": {HazardEvent{Source: sourceSensorPantai}, false}} {
		if got := isCritical(tc.h); got != tc.want {
			t.Errorf("%s: got %v want %v", name, got, tc.want)
		}
	}
}

func TestRelayDeliversEveryStagedEventAcrossABrokerOutage(t *testing.T) {
	t.Parallel()

	b := &critBroker{dead: true}
	o := &memOutbox{}
	r := newCriticalRelay(o, b.dial)
	startRelay(t, r)

	var want []string

	for i := range 20 {
		id := fmt.Sprintf("o-%d", i)
		want = append(want, id)

		if err := r.stage(context.Background(), critEvent(id), "c"); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(30 * time.Millisecond)

	if got := b.ids(); len(got) != 0 {
		t.Fatalf("published %d while the broker was down", len(got))
	}

	b.setDead(false)
	eventually(t, "all delivered in order", func() bool { return slices.Equal(b.ids(), want) })
}

func TestRelayRetriesANackedMessageWithoutLoss(t *testing.T) {
	t.Parallel()

	b := &critBroker{failPublishes: 3}
	o := &memOutbox{}
	r := newCriticalRelay(o, b.dial)
	startRelay(t, r)

	for _, id := range []string{"a", "b", "c"} {
		if err := r.stage(context.Background(), critEvent(id), "c"); err != nil {
			t.Fatal(err)
		}
	}

	eventually(t, "a, b, c in order after three failures", func() bool { return slices.Equal(b.ids(), []string{"a", "b", "c"}) })
}

func TestRelayPicksUpRowsLeftByAPreviousProcess(t *testing.T) {
	t.Parallel()

	o := &memOutbox{rows: []outboxRow{{id: "old-1"}, {id: "old-2"}}}
	b := &critBroker{}
	startRelay(t, newCriticalRelay(o, b.dial))

	eventually(t, "leftovers published", func() bool { return slices.Equal(b.ids(), []string{"old-1", "old-2"}) })
}

func TestStageIsIdempotentPerObservation(t *testing.T) {
	t.Parallel()

	o := &memOutbox{}
	r := newCriticalRelay(o, (&critBroker{}).dial)

	for range 3 {
		if err := r.stage(context.Background(), critEvent("same"), "c"); err != nil {
			t.Fatal(err)
		}
	}

	if rows, _ := o.pending(context.Background(), 10); len(rows) != 1 {
		t.Fatalf("%d rows, want 1", len(rows))
	}
}

func TestIngestStagesCriticalBeforeUpsertAndSkipsUpsertOnFailure(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		order []string
	)

	note := func(s string) {
		mu.Lock()
		defer mu.Unlock()

		order = append(order, s)
	}

	stageErr := errors.New("db down")
	failing := map[string]int{"bad": 1}

	in := &ingest{
		name:      sourceSensorPantai,
		retryBase: time.Millisecond, retryMax: 2 * time.Millisecond,
		mapBatch: func(pollResult) ([]HazardEvent, error) {
			plain := critEvent("plain")
			plain.Attributes["critical"] = false

			return []HazardEvent{critEvent("c1"), plain, critEvent("bad")}, nil
		},
		upsert: func(_ context.Context, h HazardEvent) (bool, error) {
			note("upsert " + h.SourceRefID)
			return true, nil
		},
		publish: func(_ context.Context, h HazardEvent, _ string) { note("publish " + h.SourceRefID) },
		stage: func(_ context.Context, h HazardEvent, _ string) error {
			if failing[h.SourceRefID] > 0 {
				failing[h.SourceRefID]--

				return stageErr
			}

			note("stage " + h.SourceRefID)

			return nil
		},
	}

	in.handle(context.Background(), pollResult{})

	want := []string{"stage c1", "upsert c1", "publish c1", "upsert plain", "publish plain", "stage bad", "upsert bad", "publish bad"}
	if !slices.Equal(order, want) {
		t.Fatalf("got %v want %v", order, want)
	}
}

// Needs RabbitMQ at RABBITMQ_TEST_URL, skips otherwise. The message lands in a durable queue as persistent and an unroutable one is refused.
func TestCriticalRealBroker(t *testing.T) {
	t.Parallel()

	url := os.Getenv("RABBITMQ_TEST_URL")
	if url == "" {
		t.Skip("RABBITMQ_TEST_URL not set")
	}

	queue := fmt.Sprintf("test.critical.%d", time.Now().UnixNano())

	s, err := dialCritical(url, queue)(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	defer s.close()

	if err := s.publish(context.Background(), outboxRow{id: "o-1", correlationID: "c-1", body: []byte(`{"x":1}`)}); err != nil {
		t.Fatal(err)
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

	d, ok, err := ch.Get(queue, true)
	if err != nil || !ok {
		t.Fatalf("no message (err %v)", err)
	}

	if d.DeliveryMode != amqp.Persistent || d.MessageId != "o-1" || d.CorrelationId != "c-1" {
		t.Fatalf("unexpected properties %+v", d)
	}

	if _, err := ch.QueueDelete(queue, false, false, false); err != nil {
		t.Fatal(err)
	}

	if err := s.publish(context.Background(), outboxRow{id: "o-2", body: []byte(`{}`)}); err == nil {
		t.Fatal("publish to a missing queue must fail, not be dropped")
	}
}

// Needs CANONICAL_DB_TEST_URL, skips otherwise.
func TestPgOutbox(t *testing.T) {
	s := testStore(t, 2)
	o := pgOutbox{db: s.db}
	ctx := context.Background()
	id := fmt.Sprintf("pg-%d", time.Now().UnixNano())

	t.Cleanup(func() { _, _ = s.db.ExecContext(ctx, `DELETE FROM critical_outbox WHERE observation_id = $1`, id) })

	for range 2 {
		if err := o.add(ctx, id, "c-1", []byte(`{"a":1}`)); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := o.pending(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}

	n := 0

	for _, r := range rows {
		if r.id == id {
			n++

			if r.correlationID != "c-1" || string(r.body) != `{"a":1}` {
				t.Fatalf("bad row %+v", r)
			}
		}
	}

	if n != 1 {
		t.Fatalf("%d pending rows for the id, want 1", n)
	}

	if err := o.sent(ctx, id); err != nil {
		t.Fatal(err)
	}

	rows, _ = o.pending(ctx, 1000)
	for _, r := range rows {
		if r.id == id {
			t.Fatal("still pending after sent")
		}
	}
}
