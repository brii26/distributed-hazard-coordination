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

// logger JSON yang dipakai semua log, atomic karena setupLogging bisa swap saat goroutine lain nulis.
var base atomic.Pointer[slog.Logger]

func init() { setupLogging(os.Stdout) }

// logger JSON pake nama field sesuai spec logging.
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

// jadiin w satu-satunya output log, log stdlib ikut lewat sini pake correlation_id "-".
func setupLogging(w io.Writer) {
	l := newLogger(w)
	base.Store(l)
	slog.SetDefault(l.With("correlation_id", "-"))
}

// logger buat satu correlation ID ("-" kalau belum ada).
func withID(id string) *slog.Logger {
	if id == "" {
		id = "-"
	}

	return base.Load().With("correlation_id", id)
}

type correlationKey struct{}

// bawa correlation ID satu poll cycle ke call-call di bawahnya.
func withCorrelation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

func correlationOf(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)

	return id
}

// klasifikasi hasil outbound call (success, timeout, failure) sesuai spec logging.
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

// log satu outbound call: target, duration_ms, status, dan penyebab kalau gagal.
func logCall(ctx context.Context, target string, start time.Time, err error) {
	endCall(ctx, err)

	attrs := []any{"target", target, "duration_ms", time.Since(start).Milliseconds(), "status", callStatus(err)}

	if err != nil {
		withID(correlationOf(ctx)).Warn("outbound call", append(attrs, "error", err.Error())...)

		return
	}

	withID(correlationOf(ctx)).Info("outbound call", attrs...)
}
