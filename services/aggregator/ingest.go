package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/trace"
)

const (
	// ponytail: fixed. Make it config if a slow database ever makes it matter.
	ingestBuffer = 16
	retryBase    = 200 * time.Millisecond
	retryMax     = 5 * time.Second
)

// Second stage per source: map the poller's records, upsert them, publish the new or changed ones.
type ingest struct {
	name                string
	in                  <-chan pollResult
	mapBatch            func(pollResult) ([]HazardEvent, error)
	upsert              func(context.Context, HazardEvent) (bool, error)
	publish             func(ctx context.Context, h HazardEvent, correlationID string)
	retryBase, retryMax time.Duration
	// Set, a critical event is written here before its upsert so it survives a crash or a broker outage.
	stage func(ctx context.Context, h HazardEvent, correlationID string) error
	// Called after every batch with whether all of it was stored. Nil for sources that keep no durable cursor.
	afterBatch func(ctx context.Context, res pollResult, clean bool)
	// Called when a store rejected a write as stale, with the batch's token. Nil without a lease.
	onFenced func(epoch int64)
}

// Runs until the poller closes the channel, so queued records are still written on shutdown.
func (i *ingest) run(ctx context.Context) {
	for res := range i.in {
		i.handle(ctx, res)
	}
}

// Maps one poll and writes it in order. Rejected records are skipped.
func (i *ingest) handle(ctx context.Context, res pollResult) {
	start := time.Now()
	ctx, span := tracer.Start(trace.ContextWithRemoteSpanContext(ctx, res.trace), "ingest "+i.name)

	defer span.End()

	ctx = withCorrelation(ctx, res.correlationID)

	if res.epoch > 0 {
		ctx = withLeaseEpoch(ctx, res.epoch)
	}

	l := withID(res.correlationID)

	for _, d := range describeFields(i.name, res.records) {
		l.Info("unknown field", "source", i.name, "field", d.field, "decision", d.decision)
	}

	for _, d := range describeFields(i.name+" warnings", res.warnings) {
		l.Info("unknown field", "source", i.name, "field", d.field, "decision", d.decision)
	}

	events, err := i.mapBatch(res)
	if err != nil {
		l.Warn("ingest rejected records", "source", i.name, "error", err.Error())
	}

	changed := 0
	clean := err == nil

	for _, h := range events {
		if i.stage != nil && isCritical(h) {
			if err := i.stageCritical(ctx, h, res.correlationID); err != nil {
				l.Warn("ingest critical stage failed", "source", i.name, "source_ref_id", h.SourceRefID, "error", err.Error())

				clean = false

				if ctx.Err() != nil {
					break
				}

				continue
			}
		}

		c, err := i.write(ctx, h)
		if errors.Is(err, errFenced) {
			l.Warn("fenced", "source", i.name, "source_ref_id", h.SourceRefID, "epoch", res.epoch, "error", err.Error())

			if i.onFenced != nil {
				i.onFenced(res.epoch)
			}

			clean = false

			break
		}

		if err != nil {
			l.Warn("ingest upsert failed", "source", i.name, "source_ref_id", h.SourceRefID, "error", err.Error())

			clean = false

			if ctx.Err() != nil {
				break
			}

			continue
		}

		if c {
			changed++

			i.publish(ctx, h, res.correlationID)
		}
	}

	l.Info("ingest", "source", i.name, "records", len(res.records), "mapped", len(events),
		"rejected", len(res.records)-len(events), "changed", changed, "duration_ms", time.Since(start).Milliseconds())

	if i.afterBatch != nil {
		i.afterBatch(ctx, res, clean)
	}
}

// Upserts h. An outage is retried with a growing delay and holds the batch (the poller waits too), a data error returns at once.
func (i *ingest) write(ctx context.Context, h HazardEvent) (bool, error) {
	for attempt := 0; ; attempt++ {
		changed, err := i.upsert(context.WithoutCancel(ctx), h)
		if err == nil || isPermanent(err) || errors.Is(err, errFenced) {
			return changed, err
		}

		withID(correlationOf(ctx)).Warn("ingest upsert retrying", "source", i.name, "source_ref_id", h.SourceRefID, "error", err.Error())

		select {
		case <-ctx.Done():
			return false, err
		case <-time.After(min(i.retryBase*time.Duration(1<<min(attempt, 10)), i.retryMax)):
		}
	}
}

// Stages h with the same retry as the upsert. A failure skips the upsert too, so the record is polled and staged again.
func (i *ingest) stageCritical(ctx context.Context, h HazardEvent, correlationID string) error {
	for attempt := 0; ; attempt++ {
		err := i.stage(context.WithoutCancel(ctx), h, correlationID)
		if err == nil || isPermanent(err) {
			return err
		}

		withID(correlationID).Warn("ingest critical stage retrying", "source", i.name, "source_ref_id", h.SourceRefID, "error", err.Error())

		select {
		case <-ctx.Done():
			return err
		case <-time.After(min(i.retryBase*time.Duration(1<<min(attempt, 10)), i.retryMax)):
		}
	}
}

// A Postgres data exception (class 22) or constraint violation (23), pointless to retry.
func isPermanent(err error) bool {
	var pg *pgconn.PgError

	return errors.As(err, &pg) && (strings.HasPrefix(pg.Code, "22") || strings.HasPrefix(pg.Code, "23"))
}

// One source's whole path: poller, channel, ingest.
type pipeline struct {
	poller *poller
	ingest *ingest
	// Set, the poller only runs while this lease is held.
	lease *elector
}

// Runs both stages until ctx is done and ingest has drained.
func (p pipeline) run(ctx context.Context) {
	var wg sync.WaitGroup

	if p.lease == nil {
		wg.Go(func() { p.poller.run(ctx) })
	} else {
		wg.Go(func() {
			defer close(p.poller.out)

			p.lease.run(ctx, p.poller.loop)
		})
	}

	if p.lease != nil {
		p.ingest.onFenced = p.lease.fence
	}

	wg.Go(func() { p.ingest.run(ctx) })

	wg.Wait()
}

// One pipeline per source, all sharing the same upsert.
func newPipelines(c pollConfig, upsert func(context.Context, HazardEvent) (bool, error), publish func(context.Context, HazardEvent, string)) []pipeline {
	m := newMapper()
	mappers := map[string]func(pollResult) ([]HazardEvent, error){
		sourceBMKG:  func(r pollResult) ([]HazardEvent, error) { return m.mapSeismic(r.records, r.warnings) },
		sourcePVMBG: func(r pollResult) ([]HazardEvent, error) { return m.mapVolcanic(r.records) },
	}

	var after func(context.Context, pollResult, bool)

	if c.pantai != nil {
		mappers[sourceSensorPantai], after = c.pantai.mapBatch, c.pantai.afterBatch
	}

	var out []pipeline

	for _, p := range newPollers(c) {
		ch := make(chan pollResult, ingestBuffer)
		p.out = ch

		in := &ingest{
			name: p.name, in: ch, mapBatch: mappers[p.name], upsert: upsert, publish: publish,
			retryBase: retryBase, retryMax: retryMax,
		}
		if p.name == sourceSensorPantai {
			in.afterBatch = after
		}

		out = append(out, pipeline{poller: p, ingest: in})
	}

	return out
}
