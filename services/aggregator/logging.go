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
)

const serviceName = "aggregator"

// The JSON logger every log line goes through. Atomic because setupLogging may swap it while other goroutines log.
var base atomic.Pointer[slog.Logger]

func init() { setupLogging(os.Stdout) }

// JSON logger with the required field names.
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

// Makes w the one log output. The stdlib log routes through it too, with correlation_id "-".
func setupLogging(w io.Writer) {
	l := newLogger(w)
	base.Store(l)
	slog.SetDefault(l.With("correlation_id", "-"))
}

// Logger for one correlation ID ("-" if none yet).
func withID(id string) *slog.Logger {
	if id == "" {
		id = "-"
	}

	return base.Load().With("correlation_id", id)
}

type correlationKey struct{}

// Carries a poll cycle's correlation ID down to the calls it makes.
func withCorrelation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

func correlationOf(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)

	return id
}

// Classifies an outbound call result as success, timeout or failure.
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

// Logs one outbound call: target, duration_ms, status, and the cause on failure.
func logCall(ctx context.Context, target string, start time.Time, err error) {
	endCall(ctx, err)

	attrs := []any{"target", target, "duration_ms", time.Since(start).Milliseconds(), "status", callStatus(err)}

	if err != nil {
		withID(correlationOf(ctx)).Warn("outbound call", append(attrs, "error", err.Error())...)

		return
	}

	withID(correlationOf(ctx)).Info("outbound call", attrs...)
}
