package main

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// ingestBuffer is how many polls' worth of records may queue between a poller
	// and its ingest goroutine before the poller has to wait.
	// ponytail: fixed; expose as config if a slow database ever makes it matter.
	ingestBuffer = 16
	retryBase    = 200 * time.Millisecond
	retryMax     = 5 * time.Second
)

// ingest is one source's second stage (doc/TRD_M1.md 3.5): it takes the records
// the poller handed over, maps them, upserts each event and calls publish for the
// ones that were new or changed. There is one per source, running for the whole
// life of the process, so a slow mapping or database write for one source never
// holds up the other, and the poll loop never waits for the database.
type ingest struct {
	name     string
	in       <-chan pollResult
	mapBatch func(pollResult) ([]HazardEvent, error)
	upsert   func(context.Context, HazardEvent) (bool, error)
	// publish is called for every new or changed event. It does nothing until
	// the RabbitMQ publisher (#23) is plugged in.
	publish             func(HazardEvent)
	retryBase, retryMax time.Duration
}

// run ingests until the poller closes the channel, so everything the poller
// handed over is still written when the service is stopping.
func (i *ingest) run(ctx context.Context) {
	for res := range i.in {
		i.handle(ctx, res)
	}
}

// handle maps one poll's records and writes them in order. A record the mapper
// rejects is logged and skipped, the rest still go through.
func (i *ingest) handle(ctx context.Context, res pollResult) {
	start := time.Now()

	events, err := i.mapBatch(res)
	if err != nil {
		log.Printf("ingest source=%s rejected records: %v", i.name, err)
	}

	changed := 0

	for _, h := range events {
		c, err := i.write(ctx, h)
		if err != nil {
			log.Printf("ingest source=%s upsert failed record=%s: %v", i.name, h.SourceRefID, err)

			if ctx.Err() != nil {
				break // stopping while the database is unreachable: give up the rest of this batch
			}

			continue
		}

		if c {
			changed++

			i.publish(h)
		}
	}

	log.Printf("ingest source=%s records=%d mapped=%d rejected=%d changed=%d duration_ms=%d",
		i.name, len(res.records), len(events), len(res.records)-len(events), changed, time.Since(start).Milliseconds())
}

// write upserts h. A failure that looks like an outage (connection lost, timeout)
// is retried with a growing delay, which holds this source's batch in place: its
// channel fills and the poller waits, so nothing is lost. A data error (a
// constraint or data exception) cannot succeed on retry and is returned at once.
// Stopping ends the retrying.
func (i *ingest) write(ctx context.Context, h HazardEvent) (bool, error) {
	for attempt := 0; ; attempt++ {
		// Not cancelled with ctx, so the writes of a draining shutdown still run;
		// each is bounded by the store's own timeout.
		changed, err := i.upsert(context.WithoutCancel(ctx), h)
		if err == nil || isPermanent(err) {
			return changed, err
		}

		log.Printf("ingest source=%s record=%s: %v, retrying", i.name, h.SourceRefID, err)

		select {
		case <-ctx.Done():
			return false, err
		case <-time.After(min(i.retryBase*time.Duration(1<<min(attempt, 10)), i.retryMax)):
		}
	}
}

// isPermanent reports whether err is a data error that no retry can fix:
// Postgres class 22 (data exception) or 23 (integrity constraint violation).
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

// newPipelines builds one pipeline per source, each with its own channel and
// ingest goroutine, all sharing the one upsert (the store's pool).
func newPipelines(c pollConfig, upsert func(context.Context, HazardEvent) (bool, error), publish func(HazardEvent)) []pipeline {
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
