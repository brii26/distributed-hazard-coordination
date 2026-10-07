package main

import (
	"context"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer(serviceName)

// setupTracing sends spans to the OTLP endpoint in OTEL_EXPORTER_OTLP_ENDPOINT when
// TRACING_ENABLED=true; otherwise tracing stays the no-op default. Export is batched
// and asynchronous, so a Jaeger that is down costs a request nothing (#116).
func setupTracing(ctx context.Context) (shutdown func(), err error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	if os.Getenv("TRACING_ENABLED") != "true" {
		return func() {}, nil
	}

	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(serviceName))),
	)
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer(serviceName)

	return func() {
		flush, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = tp.Shutdown(flush)
	}, nil
}

// traceIDOf is the trace ID in ctx as 32 hex characters, or "" when there is none.
// With tracing on it doubles as the correlation ID, so a log line's correlation_id
// is the Jaeger trace ID.
func traceIDOf(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}

	return ""
}

type callKey struct{}

// startCall opens a client span for one outbound call; logCall ends it with the
// call's result, so the span duration is the call's latency.
func startCall(ctx context.Context, target string) context.Context {
	ctx, sp := tracer.Start(ctx, target, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("correlation_id", correlationOf(ctx))))

	return context.WithValue(ctx, callKey{}, sp)
}

func endCall(ctx context.Context, err error) {
	sp, ok := ctx.Value(callKey{}).(trace.Span)
	if !ok {
		return
	}

	if err != nil {
		sp.RecordError(err)
		sp.SetStatus(codes.Error, err.Error())
	}

	sp.End()
}

// amqpCarrier lets the propagator read and write a message's AMQP headers.
type amqpCarrier map[string]any

func (c amqpCarrier) Get(k string) string { s, _ := c[k].(string); return s }
func (c amqpCarrier) Set(k, v string)     { c[k] = v }
func (c amqpCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}

	return keys
}
