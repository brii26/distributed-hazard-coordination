package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Reason codes stored in front of the detail, "code: detail".
const (
	codeSchema           = "schema_incompatible"
	codeMalformed        = "malformed"
	codeMalformedJSON    = "malformed_json"
	codeTooDeep          = "too_deep"
	codeInvalidUTF8      = "invalid_utf8"
	codeTooLarge         = "too_large"
	codeInvalidValue     = "invalid_value"
	codeTimeout          = "timeout"
	codeStoreUnavailable = "store_unavailable"
	codeUpstream5xx      = "upstream_5xx"
	codeBreakerOpen      = "breaker_open"
	codeUnknown          = "unknown"
)

var (
	errBreakerOpen = errors.New("circuit breaker open")
	// Set on a record that skipped its quick tries because an earlier record of the batch found the store down.
	errStoreDown = errors.New("store down earlier in this batch")
	errTooDeep   = errors.New("nesting too deep")
	errNotUTF8   = errors.New("invalid utf-8")
	errOversize  = errors.New("record too large")
	errBadEvent  = errors.New("stored mapped event unreadable")
)

// An upstream answered with this HTTP status.
type statusError struct{ code int }

func (e *statusError) Error() string { return fmt.Sprintf("upstream status %d", e.code) }

// Says whether trying err again later can change the result, and a short code for it. Order matters: data errors first, then outages, anything else is retryable so the attempt cap parks it.
func classifyFailure(err error) (retryable bool, code string) { //nolint:gocyclo // one flat switch, one case per class
	var (
		schema *IncompatibleSchema
		syn    *json.SyntaxError
		typ    *json.UnmarshalTypeError
		pg     *pgconn.PgError
		st     *statusError
		nerr   net.Error
	)

	switch {
	case errors.As(err, &schema):
		return false, codeSchema
	case errors.Is(err, errNotUTF8):
		return false, codeInvalidUTF8
	case errors.Is(err, errTooDeep):
		return false, codeTooDeep
	case errors.Is(err, errOversize):
		return false, codeTooLarge
	case errors.As(err, &syn), errors.Is(err, errNotArray), errors.Is(err, errTrailing):
		return false, codeMalformedJSON
	case errors.As(err, &typ), errors.Is(err, errBadEvent):
		return false, codeMalformed
	case isPermanent(err):
		return false, codeInvalidValue
	case errors.Is(err, context.DeadlineExceeded):
		return true, codeTimeout
	case errors.As(err, &pg) && (strings.HasPrefix(pg.Code, "08") || strings.HasPrefix(pg.Code, "53") || strings.HasPrefix(pg.Code, "57P")):
		return true, codeStoreUnavailable
	case errors.Is(err, sql.ErrConnDone), errors.Is(err, errNoNodes), errors.Is(err, errStoreDown):
		return true, codeStoreUnavailable
	case errors.As(err, &nerr):
		if nerr.Timeout() {
			return true, codeTimeout
		}

		return true, codeStoreUnavailable
	case errors.As(err, &st) && st.code >= 500:
		return true, codeUpstream5xx
	case errors.Is(err, errBreakerOpen):
		return true, codeBreakerOpen
	case err != nil && isRecordError(err):
		return false, codeMalformed
	}

	return true, codeUnknown
}

func isRecordError(err error) bool {
	var re *recordError

	return errors.As(err, &re)
}

// Stored reason "code: detail"; IncompatibleSchema already starts with its code, so it is not added twice.
func failureReason(code string, err error) string {
	if detail := err.Error(); strings.HasPrefix(detail, code+": ") {
		return detail
	}

	return code + ": " + err.Error()
}

// True when the code means the store itself is not answering.
func isOutage(code string) bool { return code == codeTimeout || code == codeStoreUnavailable }

type retryConfig struct {
	base, cap, poll time.Duration
	maxAttempts     int
}

var defaultRetryConfig = retryConfig{base: 10 * time.Second, cap: 10 * time.Minute, poll: 5 * time.Second, maxAttempts: 8}

// Wait after failure number n (1 based): min(cap, base * 2^(n-1)), then equal jitter, so the result is in [d/2, d].
func (c retryConfig) delay(n int) time.Duration {
	d := min(c.cap, c.base<<min(max(n-1, 0), 20))

	return d/2 + rand.N(d/2+1) //nolint:gosec // jitter, not security
}
