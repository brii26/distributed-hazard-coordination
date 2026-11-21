package main

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

const serviceName = "situation-summarizer"

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

	return slog.New(h).With("service", serviceName, "correlation_id", "-")
}

// Makes w the one log output, stdlib log goes through it too.
func setupLogging(w io.Writer) {
	slog.SetDefault(newLogger(w))
}

func init() { setupLogging(os.Stdout) }
