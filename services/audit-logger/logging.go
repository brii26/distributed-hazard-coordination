package main

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

const serviceName = "audit-logger"

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
	slog.SetDefault(newLogger(w).With("correlation_id", "-"))
}

// Installed at start-up, before main, so every line from the first one is JSON.
func init() { setupLogging(os.Stdout) }
