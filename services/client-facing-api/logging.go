package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const serviceName = "client-facing-api"

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

// Logger without a correlation_id, so one can be added per call.
var base atomic.Pointer[slog.Logger]

func init() { setupLogging(os.Stdout) }

// Classifies an outbound call result as success, timeout or failure.
func callStatus(err error) string {
	var t interface{ Timeout() bool }

	switch {
	case err == nil:
		return "success"
	case errors.Is(err, context.DeadlineExceeded), status.Code(err) == codes.DeadlineExceeded, errors.As(err, &t) && t.Timeout():
		return "timeout"
	default:
		return "failure"
	}
}

// The correlation ID an outgoing call carries as gRPC metadata, or "-".
func correlationOf(ctx context.Context) string {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if v := md.Get("x-correlation-id"); len(v) > 0 && v[0] != "" {
			return v[0]
		}
	}

	return "-"
}

// Logs one outbound call (gRPC to Auth or Aggregator, or a TCP probe): target, duration_ms, status, and the cause on failure. Unauthenticated, AlreadyExists and InvalidArgument replies count as success.
func logCall(ctx context.Context, target string, start time.Time, err error) {
	switch status.Code(err) { //nolint:exhaustive // only these three are replies, not failures
	case codes.Unauthenticated, codes.AlreadyExists, codes.InvalidArgument:
		err = nil
	}

	l := base.Load().With("correlation_id", correlationOf(ctx))

	attrs := []any{"target", target, "duration_ms", time.Since(start).Milliseconds(), "status", callStatus(err)}

	if err != nil {
		l.Warn("outbound call", append(attrs, "error", err.Error())...)

		return
	}

	l.Info("outbound call", attrs...)
}

// gRPC client interceptor that logs every call on a connection to addr, so no call site can forget.
func logOutbound(addr string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if id := requestID(ctx); id != "" && correlationOf(ctx) == "-" {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-correlation-id", id)
		}

		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		logCall(ctx, "grpc "+addr+method, start, err)

		return err
	}
}

const correlationHeader = "X-Correlation-ID"

// Accepts a client's own ID only if it is short and plain, so it is safe in logs, metadata and the response header.
var validCorrelation = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

type requestIDKey struct{}

// The correlation ID of the client request ctx belongs to, or "".
func requestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)

	return id
}

// Logger for one client request, carrying its correlation ID.
func logFor(ctx context.Context) *slog.Logger {
	id := requestID(ctx)
	if id == "" {
		id = "-"
	}

	return base.Load().With("correlation_id", id)
}

func newCorrelationID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "unknown"
	}

	return hex.EncodeToString(b)
}

type statusRecorder struct {
	http.ResponseWriter

	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Lets http.NewResponseController reach the real writer to flush and set deadlines, which the hazard stream needs.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// Gives every client request a correlation ID: the client's X-Correlation-ID if usable, a new one otherwise. Returned in the response header, carried to every log line and outbound call, one log line per request. /health logs at debug.
func withCorrelation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(correlationHeader)
		if !validCorrelation.MatchString(id) {
			if id = traceIDOf(r.Context()); id == "" {
				id = newCorrelationID()
			}
		}

		trace.SpanFromContext(r.Context()).SetAttributes(attribute.String("correlation_id", id))

		w.Header().Set(correlationHeader, id)

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		start := time.Now()

		next.ServeHTTP(rec, r.WithContext(ctx))

		level := slog.LevelInfo
		if r.URL.Path == "/health" {
			level = slog.LevelDebug
		}

		logFor(ctx).Log(ctx, level, "request", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "duration_ms", time.Since(start).Milliseconds())
	})
}
