package main

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
)

const serviceName = "dashboard-updater"

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

// base is the logger without a correlation_id, so withID can add the right one.
var base atomic.Pointer[slog.Logger]

// withID is the logger for one message's correlation ID, the AMQP correlation_id
// property the Aggregator set from its poll cycle ("-" when a message has none).
func withID(id string) *slog.Logger {
	if id == "" {
		id = "-"
	}

	return base.Load().With("correlation_id", id)
}

// Installed at start-up, before main, so every line from the first one is JSON.
func init() { setupLogging(os.Stdout) }
