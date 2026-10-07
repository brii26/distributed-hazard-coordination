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
	// ponytail: fixed; make it config if a slow database ever makes it matter.
	ingestBuffer = 16
	retryBase    = 200 * time.Millisecond
	retryMax     = 5 * time.Second
)

// tahap kedua per source (TRD 3.5): mapping record dari poller, upsert, lalu publish yang baru atau berubah.
type ingest struct {
	name                string
	in                  <-chan pollResult
	mapBatch            func(pollResult) ([]HazardEvent, error)
	upsert              func(context.Context, HazardEvent) (bool, error)
	publish             func(ctx context.Context, h HazardEvent, correlationID string)
	retryBase, retryMax time.Duration
}

// jalan sampai poller nutup channel, jadi record yang antre tetap ditulis saat shutdown.
func (i *ingest) run(ctx context.Context) {
	for res := range i.in {
		i.handle(ctx, res)
	}
}

// mapping satu poll lalu tulis berurutan, record yang ditolak di-skip.
func (i *ingest) handle(ctx context.Context, res pollResult) {
	start := time.Now()
	ctx, span := tracer.Start(trace.ContextWithRemoteSpanContext(ctx, res.trace), "ingest "+i.name)

	defer span.End()

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
}

// upsert h. Outage di-retry dengan delay naik dan menahan batch (poller ikut nunggu), error data langsung dikembalikan.
func (i *ingest) write(ctx context.Context, h HazardEvent) (bool, error) {
	for attempt := 0; ; attempt++ {
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

// error data Postgres (class 22) atau constraint violation (23), nggak ada gunanya di-retry.
func isPermanent(err error) bool {
	var pg *pgconn.PgError

	return errors.As(err, &pg) && (strings.HasPrefix(pg.Code, "22") || strings.HasPrefix(pg.Code, "23"))
}

// jalur satu source: poller, channel, ingest.
type pipeline struct {
	poller *poller
	ingest *ingest
}

// jalanin dua tahap sampai ctx selesai dan ingest drain.
func (p pipeline) run(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Go(func() { p.poller.run(ctx) })
	wg.Go(func() { p.ingest.run(ctx) })

	wg.Wait()
}

// satu pipeline per source, semuanya share upsert yang sama.
func newPipelines(c pollConfig, upsert func(context.Context, HazardEvent) (bool, error), publish func(context.Context, HazardEvent, string)) []pipeline {
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
