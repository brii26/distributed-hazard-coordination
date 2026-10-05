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

	"google.golang.org/grpc/metadata"
)

const serviceName = "auth-service"

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
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &t) && t.Timeout():
		return "timeout"
	default:
		return "failure"
	}
}

// correlationOf is the correlation ID the caller sent as gRPC metadata, or "-".
func correlationOf(ctx context.Context) string {
	if v := metadata.ValueFromIncomingContext(ctx, "x-correlation-id"); len(v) > 0 && v[0] != "" {
		return v[0]
	}

	return "-"
}

// logCall records one outbound call (here: the User DB): target, duration_ms,
// status, and the cause when it failed (doc/TRD_M1.md 0.3).
func logCall(ctx context.Context, target string, start time.Time, err error) {
	l := base.Load().With("correlation_id", correlationOf(ctx))

	attrs := []any{"target", target, "duration_ms", time.Since(start).Milliseconds(), "status", callStatus(err)}

	if err != nil {
		l.Warn("outbound call", append(attrs, "error", err.Error())...)

		return
	}

	l.Info("outbound call", attrs...)
}
