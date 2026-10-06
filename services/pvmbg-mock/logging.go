package main

import (
	"io"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

const serviceName = "pvmbg-mock"

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

// base is the logger without a correlation_id, so a request can add its own.
var base atomic.Pointer[slog.Logger]

// Installed at start-up, before main, so every line from the first one is JSON.
func init() { setupLogging(os.Stdout) }

const correlationHeader = "X-Correlation-ID"

var validCorrelation = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

type statusRecorder struct {
	http.ResponseWriter

	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// withRequestLog logs one line per request with the poll cycle's correlation ID
// the Aggregator sends (doc/TRD_M1.md 0.1), "-" when there is none. /health is
// logged at debug level so the container healthcheck doesn't flood the output.
func withRequestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(correlationHeader)
		if !validCorrelation.MatchString(id) {
			id = "-"
		}

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()

		next.ServeHTTP(rec, r)

		// r.Pattern is set by the mux: a server-defined route, never raw client input.
		endpoint, level := r.Pattern, slog.LevelInfo
		if endpoint == "" {
			endpoint = "unmatched"
		}

		if endpoint == "GET /health" {
			level = slog.LevelDebug
		}

		base.Load().With("correlation_id", id).Log(r.Context(), level, "request",
			"endpoint", endpoint, "status", rec.status, "duration_ms", time.Since(start).Milliseconds())
	})
}
