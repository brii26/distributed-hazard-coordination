package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var _ net.Error = timeoutError{}

func TestClassifyFailure(t *testing.T) {
	t.Parallel()

	pg := func(code string) error { return &pgconn.PgError{Code: code} }

	cases := []struct {
		name      string
		err       error
		retryable bool
		code      string
	}{
		{"schema", &IncompatibleSchema{Reason: "schema_incompatible: x"}, false, codeSchema},
		{"schema wrapped", fmt.Errorf("map: %w", &IncompatibleSchema{Reason: "x"}), false, codeSchema},
		{"json syntax", &json.SyntaxError{}, false, codeMalformed},
		{"json type", &json.UnmarshalTypeError{}, false, codeMalformed},
		{"utf8", errNotUTF8, false, codeMalformed},
		{"too deep", errTooDeep, false, codeMalformed},
		{"oversize", errOversize, false, codeMalformed},
		{"mapper rejection", &recordError{1, errors.New("bad field")}, false, codeMalformed},
		{"record error with schema", &recordError{0, &IncompatibleSchema{Reason: "x"}}, false, codeSchema},
		{"pg data exception", pg("22003"), false, codeInvalidValue},
		{"pg constraint", pg("23505"), false, codeInvalidValue},
		{"deadline", context.DeadlineExceeded, true, codeTimeout},
		{"net timeout", timeoutError{}, true, codeTimeout},
		{"net refused", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true, codeStoreUnavailable},
		{"pg connection", pg("08006"), true, codeStoreUnavailable},
		{"pg resources", pg("53300"), true, codeStoreUnavailable},
		{"pg shutdown", pg("57P01"), true, codeStoreUnavailable},
		{"conn done", sql.ErrConnDone, true, codeStoreUnavailable},
		{"no nodes", errNoNodes, true, codeStoreUnavailable},
		{"upstream 503", &statusError{503}, true, codeUpstream5xx},
		{"breaker", fmt.Errorf("poll: %w", errBreakerOpen), true, codeBreakerOpen},
		{"unknown", errors.New("something odd"), true, codeUnknown},
		{"pg other class", pg("42P01"), true, codeUnknown},
		{"4xx upstream is unknown", &statusError{404}, true, codeUnknown},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if r, code := classifyFailure(c.err); r != c.retryable || code != c.code {
				t.Fatalf("got (%v, %s), want (%v, %s)", r, code, c.retryable, c.code)
			}
		})
	}
}

func TestBackoffStaysInJitterRangeAndUnderCap(t *testing.T) {
	t.Parallel()

	c := defaultRetryConfig
	want := []int{10, 20, 40, 80, 160, 320, 600, 600, 600}

	for n, w := range want {
		full := time.Duration(w) * time.Second

		for range 200 {
			if d := c.delay(n + 1); d < full/2 || d > full {
				t.Fatalf("failure %d: delay %v outside [%v, %v]", n+1, d, full/2, full)
			}
		}
	}

	if d := c.delay(1000); d > c.cap {
		t.Fatalf("huge failure count must stay under the cap, got %v", d)
	}
}

func TestLoadRetryConfig(t *testing.T) {
	t.Setenv("RETRY_BASE", "")
	t.Setenv("RETRY_CAP", "")
	t.Setenv("RETRY_POLL", "")
	t.Setenv("RETRY_MAX_ATTEMPTS", "")

	if c, err := loadRetryConfig(); err != nil || c != defaultRetryConfig {
		t.Fatalf("defaults: %+v %v", c, err)
	}

	for _, bad := range [][2]string{{"RETRY_BASE", "soon"}, {"RETRY_CAP", "-1s"}, {"RETRY_POLL", "0s"}, {"RETRY_MAX_ATTEMPTS", "0"}, {"RETRY_CAP", "1s"}} {
		t.Setenv(bad[0], bad[1])

		if _, err := loadRetryConfig(); err == nil {
			t.Fatalf("%s=%s must be refused", bad[0], bad[1])
		}

		t.Setenv(bad[0], "")
	}
}
