package main

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
)

const serviceName = "dashboard-updater"

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

// Makes w the one log output, stdlib log goes through it too with correlation_id "-".
func setupLogging(w io.Writer) {
	l := newLogger(w)
	base.Store(l)
	slog.SetDefault(l.With("correlation_id", "-"))
}

// Logger without a correlation_id, so withID can add the right one.
var base atomic.Pointer[slog.Logger]

// Logger for one message's correlation ID, from the AMQP property ("-" if none).
func withID(id string) *slog.Logger {
	if id == "" {
		id = "-"
	}

	return base.Load().With("correlation_id", id)
}

func init() { setupLogging(os.Stdout) }
