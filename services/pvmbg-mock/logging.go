package main

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

const serviceName = "pvmbg-mock"

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

// Logger without a correlation_id, so a request can add its own.
var base atomic.Pointer[slog.Logger]

func init() { setupLogging(os.Stdout) }

const correlationHeader = "X-Correlation-ID"

var validCorrelation = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// The caller's correlation ID, "-" if missing or not a safe token.
func correlationID(r *http.Request) string {
	id := r.Header.Get(correlationHeader)
	if !validCorrelation.MatchString(id) {
		return "-"
	}

	return id
}

const maxAdminBody = 1024

// Reads up to 1 KB of an admin call and returns a logger that carries method, path, body and correlation ID.
func adminCall(r *http.Request) ([]byte, *slog.Logger) {
	raw, _ := io.ReadAll(io.LimitReader(r.Body, maxAdminBody+1))

	cut := len(raw) > maxAdminBody
	if cut {
		raw = raw[:maxAdminBody]
	}

	return raw, base.Load().With("correlation_id", correlationID(r),
		"method", r.Method, "path", r.URL.Path, "body", string(raw), "body_truncated", cut)
}

type statusRecorder struct {
	http.ResponseWriter

	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Logs one line per request with the poll cycle's correlation ID from the Aggregator ("-" if none). /health logs at debug so the healthcheck doesn't flood the output.
func withRequestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := correlationID(r)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()

		next.ServeHTTP(rec, r)

		endpoint, level := r.Pattern, slog.LevelInfo
		if endpoint == "" {
			endpoint = "unmatched"
		}

		if endpoint == "GET /health" {
			level = slog.LevelDebug
		}

		base.Load().With("correlation_id", id).Log(r.Context(), level, "request",
			"endpoint", endpoint, "remote_addr", remoteHost(r), "status", rec.status, "duration_ms", time.Since(start).Milliseconds())
	})
}

// The caller's IP without the port, so a load test can tell which Aggregator instance fetched.
func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return host
}
