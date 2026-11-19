package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

// The satellite pipeline (#313) runs under the SATELIT lease: feed reader → bounded pile → fire engine → micro-batches saved to the Canonical Store and published.
//
// Recovery is a replay. Every run reads the feed from offset 0 and rebuilds the engine. Up to the bookmark it only rebuilds: those offsets were saved, parked and published before. Past the bookmark it saves again. The engine is deterministic, so the replay gives the same fires, names, versions and counts as a run without a crash.
//
// Guarantees (doc/P4_FEED_NOTES.md):
//   - fires are effectively-once: a save is a versioned upsert, so writing a fire again changes nothing;
//   - publishes are at-least-once: the bookmark moves only after the broker confirmed the batch, so a crash before that publishes the batch again.
//
// The bookmark is not fenced: a stale leader can only write a bookmark for offsets it fully saved, and a bookmark that moves back only makes the next replay save more.

const (
	defaultSatelitLateness = 12 * time.Hour // L, declared in doc/P4_FEED_NOTES.md; no event in the #308 captures was later than 5.2 h
	satelitPile            = 2048           // records read but not yet applied; when full the reader stops reading
	satelitBatchSize       = 500
	satelitBatchWait       = time.Second
)

type satelitConfig struct {
	enabled        bool
	baseURL, token string
	lateness       time.Duration
}

// Reads SATELIT_ENABLED and, when it is true, SATELLITE_BASE_URL, SATELLITE_GROUP_TOKEN and SATELIT_LATENESS.
func loadSatelitConfig() (satelitConfig, error) {
	raw := os.Getenv("SATELIT_ENABLED")
	if raw == "" {
		return satelitConfig{}, nil
	}

	on, err := strconv.ParseBool(raw)
	if err != nil {
		return satelitConfig{}, fmt.Errorf("invalid SATELIT_ENABLED value: %q", raw)
	}

	if !on {
		return satelitConfig{}, nil
	}

	c := satelitConfig{enabled: true, baseURL: os.Getenv("SATELLITE_BASE_URL"), token: os.Getenv("SATELLITE_GROUP_TOKEN")}
	if c.baseURL == "" || c.token == "" {
		return satelitConfig{}, errors.New("SATELLITE_BASE_URL and SATELLITE_GROUP_TOKEN must be set when SATELIT_ENABLED is true")
	}

	if c.lateness, err = envDuration("SATELIT_LATENESS", defaultSatelitLateness); err != nil {
		return satelitConfig{}, err
	}

	return c, nil
}

// Where the bookmark lives (source_cursors in the coordination database).
type bookmarkStore interface {
	getCursor(ctx context.Context, source string) (string, error)
	setCursor(ctx context.Context, source, cursor string) error
}

// A publisher that never drops and says when the broker confirmed.
type confirmPublisher interface {
	publishWait(ctx context.Context, h HazardEvent, correlationID string) (uint64, error)
	waitConfirmed(ctx context.Context, seq uint64) error
}

type satelitPipeline struct {
	read     func(ctx context.Context, start int64, out chan<- feedRecord) error
	upsert   func(context.Context, HazardEvent) (bool, error)
	park     func(context.Context, failedMessage) error
	marks    bookmarkStore
	pub      confirmPublisher
	onFenced func(epoch int64) // tells the elector a write was fenced
	now      func() time.Time  // ingested_at and received_at only; the engine never reads it

	lateness  time.Duration
	batchSize int
	batchWait time.Duration
	backoff   *backoff
}

func newSatelitPipeline(c satelitConfig, upsert func(context.Context, HazardEvent) (bool, error), db *store, pub *publisher) (*satelitPipeline, error) {
	r, err := newSatelitReader(c.baseURL, c.token)
	if err != nil {
		return nil, err
	}

	return &satelitPipeline{
		read: r.run, upsert: upsert, park: db.insertFailed, marks: db, pub: pub, now: time.Now,
		lateness: c.lateness, batchSize: satelitBatchSize, batchWait: satelitBatchWait,
		backoff: newBackoff(satelitBackoffBase, satelitBackoffMax),
	}, nil
}

// work is the elector's work function: it runs until the lease is lost or the process stops.
func (p *satelitPipeline) work(ctx context.Context) {
	if err := p.run(ctx); ctx.Err() == nil {
		withID("").Error("satelit pipeline stopped", "error", err.Error())
	}
}

// One micro-batch: the offsets applied since the last save and what they changed.
type satelitBatch struct {
	events int
	last   int64
	fires  []fireSnapshot
	parked []failedMessage
}

// run replays the feed from offset 0 and saves past the bookmark. It returns when ctx ends, the feed refuses the token, or a write is fenced.
func (p *satelitPipeline) run(ctx context.Context) error {
	mark, err := p.bookmark(ctx)
	if err != nil {
		return err
	}

	withID("").Info("satelit pipeline started", "bookmark", mark, "lateness", p.lateness.String())

	ctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup

	defer wg.Wait()
	defer cancel()

	pile := make(chan feedRecord, satelitPile)
	readDone := make(chan error, 1)

	wg.Go(func() { readDone <- p.read(ctx, 0, pile) })

	e := newFireEngine(p.lateness)
	b := &satelitBatch{}

	tick := time.NewTicker(p.batchWait)
	defer tick.Stop()

	for err == nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-readDone:
		case rec := <-pile:
			if p.take(e, b, rec, mark) && b.events >= p.batchSize {
				err = p.save(ctx, e, b)
			}
		case <-tick.C:
			if b.events > 0 {
				err = p.save(ctx, e, b)
			}
		}
	}

	return err
}

// The last saved offset, or -1 when nothing was saved yet. A store outage is retried.
func (p *satelitPipeline) bookmark(ctx context.Context) (int64, error) {
	for {
		c, err := p.marks.getCursor(ctx, sourceSatelit)
		if err == nil {
			if c == "" {
				return -1, nil
			}

			n, err := strconv.ParseInt(c, 10, 64)
			if err != nil || n < 0 {
				return 0, fmt.Errorf("satelit bookmark %q is not an offset", c)
			}

			return n, nil
		}

		wait := p.backoff.failed()
		withID("").Warn("satelit bookmark not read", "error", err.Error(), "retry_in", wait.String())

		if !sleepCtx(ctx, wait) {
			return 0, ctx.Err()
		}
	}
}

// take applies one record to the engine and reports whether it joined the batch. Records up to the bookmark only rebuild the engine; at the bookmark the changes so far are dropped, because they were saved before.
func (p *satelitPipeline) take(e *fireEngine, b *satelitBatch, rec feedRecord, mark int64) bool {
	var reason string

	ev, err := parseFeedEvent(rec.Kind, rec.Data)

	switch {
	case err != nil:
		reason = codeMalformed + ": " + err.Error()
	case ev.Offset != rec.Offset:
		reason = fmt.Sprintf("%s: data offset %d under event id %d", codeMalformed, ev.Offset, rec.Offset)
	default:
		reason = e.apply(ev)
	}

	if rec.Offset <= mark {
		if rec.Offset == mark {
			e.drain()
		}

		return false
	}

	if reason != "" {
		m := newFailedMessage(sourceSatelit, rec.Data, reason, false, p.now())
		m.SourceRefID = ev.Key
		b.parked = append(b.parked, m)
	}

	b.events++
	b.last = rec.Offset

	return true
}

// save writes the batch, retrying until it is stored, and then clears it. While it retries nothing new is applied, so the pile fills and the reader stops (backpressure).
func (p *satelitPipeline) save(ctx context.Context, e *fireEngine, b *satelitBatch) error {
	b.fires = e.drain()

	for {
		err := p.write(ctx, b)
		if err == nil {
			break
		}

		if errors.Is(err, errFenced) {
			if epoch, ok := leaseEpoch(ctx); ok && p.onFenced != nil {
				p.onFenced(epoch)
			}

			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		wait := p.backoff.failed()
		withID("").Warn("satelit batch not saved", "last_offset", b.last, "error", err.Error(), "retry_in", wait.String())

		if !sleepCtx(ctx, wait) {
			return ctx.Err()
		}
	}

	p.backoff.reset()
	withID("").Info("satelit batch saved", "events", b.events, "fires", len(b.fires), "parked", len(b.parked), "bookmark", b.last)

	*b = satelitBatch{}

	return nil
}

// write stores the fires, then the parked records, publishes every fire, waits for the broker, and only then moves the bookmark.
// Every fire is published even when its upsert changed nothing: after a crash that is how a batch saved but not confirmed reaches the broker.
func (p *satelitPipeline) write(ctx context.Context, b *satelitBatch) error {
	cid := "satelit-" + strconv.FormatInt(b.last, 10)
	ctx = withCorrelation(ctx, cid)
	at := p.now()

	events := make([]HazardEvent, len(b.fires))

	for i, s := range b.fires {
		events[i] = mapFire(s, at)

		if _, err := p.upsert(ctx, events[i]); err != nil {
			return fmt.Errorf("save %s: %w", s.Name, err)
		}
	}

	for _, m := range b.parked {
		if err := p.park(ctx, m); err != nil {
			return fmt.Errorf("park %s: %w", m.MessageID, err)
		}
	}

	var last uint64

	for _, h := range events {
		seq, err := p.pub.publishWait(ctx, h, cid)
		if err != nil {
			return fmt.Errorf("publish %s: %w", h.SourceRefID, err)
		}

		last = seq
	}

	if err := p.pub.waitConfirmed(ctx, last); err != nil {
		return fmt.Errorf("wait for broker confirms: %w", err)
	}

	return p.marks.setCursor(ctx, sourceSatelit, strconv.FormatInt(b.last, 10))
}
