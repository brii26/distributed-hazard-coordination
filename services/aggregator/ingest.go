package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// ponytail: fixed; make it config if a slow database ever makes it matter.
	ingestBuffer = 16
	retryBase    = 200 * time.Millisecond
	retryMax     = 5 * time.Second
)

// ingest is one source's second stage (doc/TRD_M1.md 3.5): map the records the
// poller handed over, upsert each event, publish the new or changed ones. One per
// source, so a slow write for one never holds up the other.
type ingest struct {
	name     string
	in       <-chan pollResult
	mapBatch func(pollResult) ([]HazardEvent, error)
	upsert   func(context.Context, HazardEvent) (bool, error)
	// publish hands a new or changed event, with its poll cycle's correlation ID, to the broker publisher.
	publish             func(h HazardEvent, correlationID string)
	retryBase, retryMax time.Duration
}

// run ingests until the poller closes the channel, so queued records are still written on shutdown.
func (i *ingest) run(ctx context.Context) {
	for res := range i.in {
		i.handle(ctx, res)
	}
}

// handle maps one poll's records and writes them in order; a rejected record is skipped.
func (i *ingest) handle(ctx context.Context, res pollResult) {
	start := time.Now()
	ctx = withCorrelation(ctx, res.correlationID)
	l := withID(res.correlationID)

	events, err := i.mapBatch(res)
	if err != nil {
		l.Warn("ingest rejected records", "source", i.name, "error", err.Error())
	}

	changed := 0

	for _, h := range events {
		c, err := i.write(ctx, h)
		if err != nil {
			l.Warn("ingest upsert failed", "source", i.name, "source_ref_id", h.SourceRefID, "error", err.Error())

			if ctx.Err() != nil {
				break // shutting down with the database unreachable: give up the rest
			}

			continue
		}

		if c {
			changed++

			i.publish(h, res.correlationID)
		}
	}

	l.Info("ingest", "source", i.name, "records", len(res.records), "mapped", len(events),
		"rejected", len(res.records)-len(events), "changed", changed, "duration_ms", time.Since(start).Milliseconds())
}

// write upserts h. An outage is retried with a growing delay, which holds the batch:
// the channel fills and the poller waits, so nothing is lost. A data error cannot
// succeed on retry and is returned at once. Shutdown ends the retrying.
func (i *ingest) write(ctx context.Context, h HazardEvent) (bool, error) {
	for attempt := 0; ; attempt++ {
		// Not cancelled with ctx, so a draining shutdown still writes.
		changed, err := i.upsert(context.WithoutCancel(ctx), h)
		if err == nil || isPermanent(err) {
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

// isPermanent reports a Postgres data exception (class 22) or constraint violation (23).
func isPermanent(err error) bool {
	var pg *pgconn.PgError

	return errors.As(err, &pg) && (strings.HasPrefix(pg.Code, "22") || strings.HasPrefix(pg.Code, "23"))
}

// pipeline is one source's whole path: poller, channel, ingest.
type pipeline struct {
	poller *poller
	ingest *ingest
}

// run runs both stages until ctx is done and the ingest side has drained.
func (p pipeline) run(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Go(func() { p.poller.run(ctx) })
	wg.Go(func() { p.ingest.run(ctx) })

	wg.Wait()
}

// newPipelines builds one pipeline per source; all share the one upsert (the store's pool).
func newPipelines(c pollConfig, upsert func(context.Context, HazardEvent) (bool, error), publish func(HazardEvent, string)) []pipeline {
	m := newMapper()
	mappers := map[string]func(pollResult) ([]HazardEvent, error){
		sourceBMKG:  func(r pollResult) ([]HazardEvent, error) { return m.mapSeismic(r.records, r.warnings) },
		sourcePVMBG: func(r pollResult) ([]HazardEvent, error) { return m.mapVolcanic(r.records) },
	}

	var out []pipeline

	for _, p := range newPollers(c) {
		ch := make(chan pollResult, ingestBuffer)
		p.out = ch

		out = append(out, pipeline{
			poller: p,
			ingest: &ingest{
				name: p.name, in: ch, mapBatch: mappers[p.name], upsert: upsert, publish: publish,
				retryBase: retryBase, retryMax: retryMax,
			},
		})
	}

	return out
}
