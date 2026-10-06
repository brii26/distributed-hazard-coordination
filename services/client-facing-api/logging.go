package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const serviceName = "client-facing-api"

// newLogger returns a JSON logger with the field names doc/TRD_M1.md 0.3
// requires: timestamp, level, service, correlation_id, message (same as bmkg-mock).
func newLogger(w io.Writer) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) > 0 {
				return a
			}

			switch a.Key {
			case slog.TimeKey:
				a.Key = "timestamp"
			case slog.MessageKey:
				a.Key = "message"
			case slog.LevelKey:
				a.Value = slog.StringValue(strings.ToLower(a.Value.String()))
			}

			return a
		},
	})

	return slog.New(h).With("service", serviceName)
}

// setupLogging makes w the one log output, with the stdlib log package routed
// through the JSON logger, so a plain log.Printf is a JSON line too, with
// correlation_id "-" until it has one (#34).
func setupLogging(w io.Writer) {
	l := newLogger(w)
	base.Store(l)
	slog.SetDefault(l.With("correlation_id", "-"))
}

// base is the logger without a correlation_id, so one can be added per call.
var base atomic.Pointer[slog.Logger]

// Installed at start-up, before main, so every line from the first one is JSON.
func init() { setupLogging(os.Stdout) }

// callStatus classifies an outbound call's result as doc/TRD_M1.md 0.3 asks.
func callStatus(err error) string {
	var t interface{ Timeout() bool }

	switch {
	case err == nil:
		return "success"
	case errors.Is(err, context.DeadlineExceeded), status.Code(err) == codes.DeadlineExceeded, errors.As(err, &t) && t.Timeout():
		return "timeout"
	default:
		return "failure"
	}
}

// correlationOf is the correlation ID an outgoing call carries as gRPC metadata, or "-".
func correlationOf(ctx context.Context) string {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if v := md.Get("x-correlation-id"); len(v) > 0 && v[0] != "" {
			return v[0]
		}
	}

	return "-"
}

// logCall records one outbound call (gRPC to the Auth Service or Aggregator, or a TCP probe): target, duration_ms,
// status, and the cause when it failed (doc/TRD_M1.md 0.3).
func logCall(ctx context.Context, target string, start time.Time, err error) {
	// An RPC answered with Unauthenticated, AlreadyExists or InvalidArgument is a
	// reply the caller acts on, not a failed call.
	switch status.Code(err) {
	case codes.Unauthenticated, codes.AlreadyExists, codes.InvalidArgument:
		err = nil
	}

	l := base.Load().With("correlation_id", correlationOf(ctx))

	attrs := []any{"target", target, "duration_ms", time.Since(start).Milliseconds(), "status", callStatus(err)}

	if err != nil {
		l.Warn("outbound call", append(attrs, "error", err.Error())...)

		return
	}

	l.Info("outbound call", attrs...)
}

// logOutbound is a gRPC client interceptor that logs every call made on a
// connection to addr, so no call site can forget to.
func logOutbound(addr string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		logCall(ctx, "grpc "+addr+method, start, err)

		return err
	}
}
