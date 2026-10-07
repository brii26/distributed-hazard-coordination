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

// base is the JSON logger every line goes through (see withID, logCall). Atomic
// because setupLogging may swap it while other goroutines log.
var base atomic.Pointer[slog.Logger]

// Installed at start-up, before main, so every line from the first one is JSON.
func init() { setupLogging(os.Stdout) }

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

// setupLogging makes w the one log output: the JSON logger above, and the stdlib
// log package routed through it, so a plain log.Printf is a JSON line too, with
// correlation_id "-" until it has one (#34).
func setupLogging(w io.Writer) {
	l := newLogger(w)
	base.Store(l)
	slog.SetDefault(l.With("correlation_id", "-"))
}

// withID is the logger for one correlation ID ("-" when there is none yet).
func withID(id string) *slog.Logger {
	if id == "" {
		id = "-"
	}

	return base.Load().With("correlation_id", id)
}

type correlationKey struct{}

// withCorrelation carries a poll cycle's correlation ID down to the calls it makes.
func withCorrelation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

func correlationOf(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)

	return id
}

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

// logCall records one outbound call: target, duration_ms, status, and the cause
// when it failed (doc/TRD_M1.md 0.3).
func logCall(ctx context.Context, target string, start time.Time, err error) {
	endCall(ctx, err)

	attrs := []any{"target", target, "duration_ms", time.Since(start).Milliseconds(), "status", callStatus(err)}

	if err != nil {
		withID(correlationOf(ctx)).Warn("outbound call", append(attrs, "error", err.Error())...)

		return
	}

	withID(correlationOf(ctx)).Info("outbound call", attrs...)
}
