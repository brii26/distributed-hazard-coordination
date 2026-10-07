package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// pesan yang diantre di span poll cycle harus membawa trace ID itu di header publish, kalau nggak consumer nggak bisa join trace.
func TestPublishCarriesTraceContext(t *testing.T) { //nolint:paralleltest // swaps the global tracer and propagator
	otel.SetTextMapPropagator(propagation.TraceContext{})

	old := tracer
	tracer = sdktrace.NewTracerProvider().Tracer("test")

	t.Cleanup(func() { tracer = old })

	ctx, span := tracer.Start(context.Background(), "poll")
	defer span.End()

	p := newPublisher(nil, 1)
	p.publish(ctx, HazardEvent{HazardID: "h"}, traceIDOf(ctx))

	m, ok := p.front()
	if !ok {
		t.Fatal("nothing queued")
	}

	got := trace.SpanContextFromContext(otel.GetTextMapPropagator().Extract(context.Background(), m.trace))
	if got.TraceID() != span.SpanContext().TraceID() {
		t.Fatalf("message trace %s, want %s", got.TraceID(), span.SpanContext().TraceID())
	}
}
