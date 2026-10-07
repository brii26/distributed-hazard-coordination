package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const (
	serviceName       = "bmkg-mock"
	correlationHeader = "X-Correlation-ID"
	maxCorrelationLen = 128
	healthPattern     = "GET /health"
)

type loggerCtxKey struct{}

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

// Takes the Aggregator's poll ID from the request, or generates one so every request log line has a correlation_id.
func correlationID(r *http.Request) string {
	if id := r.Header.Get(correlationHeader); id != "" {
		return id[:min(len(id), maxCorrelationLen)]
	}

	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "unknown"
	}

	return hex.EncodeToString(b)
}

// The request-scoped logger, already carrying correlation_id.
func logFrom(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerCtxKey{}).(*slog.Logger); ok {
		return l
	}

	return slog.Default()
}

type statusRecorder struct {
	http.ResponseWriter

	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Logs one line per request and puts a correlation-scoped logger in the request context. /health logs at debug so the healthcheck doesn't flood the output.
func withRequestLog(base *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := correlationID(r)
		l := base.With("correlation_id", id)

		w.Header().Set(correlationHeader, id)

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		req := r.WithContext(context.WithValue(r.Context(), loggerCtxKey{}, l))
		start := time.Now()

		next.ServeHTTP(rec, req)

		endpoint, level := req.Pattern, slog.LevelInfo
		if endpoint == "" {
			endpoint = "unmatched"
		}

		if endpoint == healthPattern {
			level = slog.LevelDebug
		}

		l.Log(req.Context(), level, "request",
			"endpoint", endpoint, "status", rec.status, "duration_ms", time.Since(start).Milliseconds())
	})
}
