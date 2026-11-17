package main

import (
	"context"
	"encoding/json"
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
	// Tries a record gets before it is handed to the retry worker.
	quickTries = 3
)

// Second stage per source: map the poller's records, upsert them, publish the new or changed ones.
type ingest struct {
	name                string
	in                  <-chan pollResult
	mapBatch            func(pollResult) ([]HazardEvent, error)
	upsert              func(context.Context, HazardEvent) (bool, error)
	publish             func(ctx context.Context, h HazardEvent, correlationID string)
	retryBase, retryMax time.Duration

	// Set, rejected records and records the store would not take are kept as failed messages. Unset, an outage holds the batch.
	fail  func(context.Context, failedMessage) error
	retry retryConfig
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

	raws := i.keepRejected(ctx, res, events, err)
	outage := false
	changed := 0

	for n, h := range events {
		c, err := i.write(ctx, h, raws[n], &outage)
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

// Stores every rejected record as a parked message and returns the record as received for each mapped event (the encoded event if the batch gives no 1:1 match).
func (i *ingest) keepRejected(ctx context.Context, res pollResult, events []HazardEvent, mapErr error) [][]byte {
	if i.fail == nil {
		return make([][]byte, len(events))
	}

	bad := map[int]error{}
	collectRecordErrors(mapErr, bad)

	raws := make([][]byte, 0, len(events))

	for idx, rec := range res.records {
		var raw []byte
		if len(res.rawRecords) == len(res.records) {
			raw = res.rawRecords[idx]
		} else {
			raw, _ = json.Marshal(rec)
		}

		if cause, rejected := bad[idx]; !rejected {
			raws = append(raws, raw)
		} else if i.fail != nil {
			_, code := classifyFailure(cause)
			m := newFailedMessage(i.name, raw, code+": "+cause.Error(), false, time.Now())
			m.CorrelationID = res.correlationID

			if err := i.fail(context.WithoutCancel(ctx), m); err != nil {
				withID(res.correlationID).Warn("ingest could not keep rejected record", "source", i.name, "error", err.Error())
			}
		}
	}

	if len(raws) != len(events) {
		raws = make([][]byte, len(events))
	}

	return raws
}

func collectRecordErrors(err error, into map[int]error) {
	switch e := err.(type) { //nolint:errorlint // walks the joined errors by hand
	case nil:
	case interface{ Unwrap() []error }:
		for _, c := range e.Unwrap() {
			collectRecordErrors(c, into)
		}
	default:
		if re, ok := errors.AsType[*recordError](err); ok {
			into[re.index] = re.err
		}
	}
}

// Keeps a record the loop could not write. False if the failed message could not be stored either, then the caller keeps waiting.
func (i *ingest) handOver(ctx context.Context, h HazardEvent, raw []byte, err error, retryable bool, code string, outage *bool) bool {
	l := withID(correlationOf(ctx))

	if perr := i.park(ctx, h, raw, err, retryable, code); perr != nil {
		*outage = false

		l.Warn("ingest could not keep failed record", "source", i.name, "source_ref_id", h.SourceRefID, "error", perr.Error())

		return false
	}

	if retryable && isOutage(code) {
		*outage = true
	}

	l.Warn("ingest kept failed record", "source", i.name, "source_ref_id", h.SourceRefID, "code", code, "retryable", retryable)

	return true
}

// Keeps h as a failed message. A retryable one carries the mapped event and its first retry time. raw is the record as received.
func (i *ingest) park(ctx context.Context, h HazardEvent, raw []byte, err error, retryable bool, code string) error {
	now := time.Now()

	if raw == nil {
		raw, _ = json.Marshal(h)
	}

	m := newFailedMessage(i.name, raw, code+": "+err.Error(), retryable, now)
	m.SourceRefID = h.SourceRefID
	m.CorrelationID = correlationOf(ctx)

	if retryable {
		ev, merr := json.Marshal(h)
		if merr != nil {
			return merr
		}

		m.MappedEvent, m.NextAttemptAt = ev, now.Add(i.retry.delay(1))
	}

	return i.fail(context.WithoutCancel(ctx), m)
}

// Upserts h. With fail set, a record gets quickTries tries, then it is kept as a failed message (at once for a data error, and for every later record of the batch once the store was found down). Without fail an outage is retried with a growing delay and holds the batch (the poller waits too).
func (i *ingest) write(ctx context.Context, h HazardEvent, raw []byte, outage *bool) (bool, error) { //nolint:gocyclo // one retry loop with its exits
	var err error

	for attempt := 0; ; attempt++ {
		if *outage && i.fail != nil {
			err = errStoreDown
		} else {
			var changed bool
			if changed, err = i.upsert(context.WithoutCancel(ctx), h); err == nil {
				return changed, nil
			}
		}

		retryable, code := classifyFailure(err)

		switch {
		case i.fail == nil && isPermanent(err):
			return false, err
		case i.fail != nil && (!retryable || attempt+1 >= quickTries || *outage):
			if i.handOver(ctx, h, raw, err, retryable, code, outage) {
				return false, nil
			}
		}

		withID(correlationOf(ctx)).Warn("ingest upsert retrying", "source", i.name, "source_ref_id", h.SourceRefID, "error", err.Error())

		select {
		case <-ctx.Done():
			return false, err
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
