package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Records publishes; gate, when set, holds each one until it is closed.
type fakeBroker struct {
	mu   sync.Mutex
	got  []reportMsg
	err  error
	gate chan struct{}
}

func (f *fakeBroker) publish(ctx context.Context, m reportMsg) error {
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if f.err != nil {
		return f.err
	}

	f.mu.Lock()
	f.got = append(f.got, m)
	f.mu.Unlock()

	return nil
}

func (f *fakeBroker) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.got)
}

func postReport(a *fieldReportsAPI, role, key, body string) *httptest.ResponseRecorder {
	return postReportAs(a, "f@x.example", role, key, body)
}

func postReportAs(a *fieldReportsAPI, email, role, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/field-reports", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}

	if role != "" {
		req = req.WithContext(context.WithValue(req.Context(), claimsKey{}, jwtClaims{Email: email, Role: role}))
	}

	rec := httptest.NewRecorder()
	withCorrelation(a).ServeHTTP(rec, req)

	return rec
}

func reportID(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()

	var b map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil || b["report_id"] == "" {
		t.Fatalf("body %q has no report_id", rec.Body.String())
	}

	return b["report_id"]
}

const okBody = `{"description":"flooded road","latitude":-6.2,"longitude":106.8,"kind":"flood"}`

func TestInvalidRequestsAre400AndPublishNothing(t *testing.T) {
	t.Parallel()

	cases := map[string]struct{ key, body string }{
		"missing description": {"", `{"kind":"x"}`},
		"empty description":   {"", `{"description":""}`},
		"4097 bytes":          {"", `{"description":"` + strings.Repeat("a", 4097) + `"}`},
		"bad utf8":            {"", "{\"description\":\"\xff\xfe\"}"},
		"wrong type":          {"", `{"description":5}`},
		"latitude only":       {"", `{"description":"x","latitude":1}`},
		"latitude 91":         {"", `{"description":"x","latitude":91,"longitude":1}`},
		"longitude 181":       {"", `{"description":"x","latitude":1,"longitude":181}`},
		"kind 65 chars":       {"", `{"description":"x","kind":"` + strings.Repeat("k", 65) + `"}`},
		"bad json":            {"", `{"description":`},
		"over 8 KiB":          {"", `{"description":"x","kind":"` + strings.Repeat("a", 9000) + `"}`},
		"key with space":      {"a b", okBody},
		"key 65 chars":        {strings.Repeat("a", 65), okBody},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fb := &fakeBroker{}
			if rec := postReport(newFieldReportsAPI(fb), roleLapangan, c.key, c.body); rec.Code != http.StatusBadRequest {
				t.Fatalf("code %d, want 400 (%s)", rec.Code, rec.Body)
			}

			if fb.count() != 0 {
				t.Fatal("published an invalid report")
			}
		})
	}
}

func TestRolesAreChecked(t *testing.T) {
	t.Parallel()

	for role, want := range map[string]int{roleMedia: 403, roleLapangan: 202, roleOperator: 202, "": 403} {
		if rec := postReport(newFieldReportsAPI(&fakeBroker{}), role, "", okBody); rec.Code != want {
			t.Errorf("role %q: code %d, want %d", role, rec.Code, want)
		}
	}
}

func TestFieldReportsNoTokenIs401(t *testing.T) {
	t.Parallel()

	h := newJWTVerifier([]byte("s")).require(newFieldReportsAPI(&fakeBroker{}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/field-reports", strings.NewReader(okBody)))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code %d, want 401", rec.Code)
	}
}

func TestAnswersOnlyAfterTheConfirm(t *testing.T) {
	t.Parallel()

	fb := &fakeBroker{gate: make(chan struct{})}
	done := make(chan *httptest.ResponseRecorder, 1)

	go func() { done <- postReport(newFieldReportsAPI(fb), roleLapangan, "", okBody) }()

	select {
	case <-done:
		t.Fatal("answered before the confirm")
	case <-time.After(150 * time.Millisecond):
	}

	close(fb.gate)

	if rec := <-done; rec.Code != http.StatusAccepted {
		t.Fatalf("code %d, want 202", rec.Code)
	}
}

func TestBrokerTroubleIs503AndNotCached(t *testing.T) {
	t.Parallel()

	for name, fb := range map[string]*fakeBroker{
		"down":    {err: errBrokerDown},
		"nack":    {err: errNacked},
		"timeout": {gate: make(chan struct{})},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			a := newFieldReportsAPI(fb)
			rec := postReport(a, roleLapangan, "k1", okBody)

			if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
				t.Fatalf("code %d retry-after %q, want 503 and 1", rec.Code, rec.Header().Get("Retry-After"))
			}

			if len(a.seen) != 0 {
				t.Fatal("cached an unconfirmed report")
			}
		})
	}
}

func TestIdempotency(t *testing.T) {
	t.Parallel()

	fb := &fakeBroker{}
	a := newFieldReportsAPI(fb)
	first, again := postReport(a, roleLapangan, "k-1", okBody), postReport(a, roleLapangan, "k-1", okBody)

	if first.Code != 202 || again.Code != 202 || reportID(t, first) != reportID(t, again) || len(reportID(t, first)) != 36 || fb.count() != 1 {
		t.Fatalf("same key twice: %d %d, publishes %d", first.Code, again.Code, fb.count())
	}

	if rec := postReport(a, roleLapangan, "k-1", `{"description":"other"}`); rec.Code != http.StatusConflict {
		t.Fatalf("other body: code %d, want 409", rec.Code)
	}

	x, y := reportID(t, postReport(a, roleLapangan, "", okBody)), reportID(t, postReport(a, roleLapangan, "", okBody))
	if x == y || len(x) != 36 {
		t.Fatalf("generated ids %q %q", x, y)
	}
}

func TestIdempotencyIsPerReporter(t *testing.T) {
	t.Parallel()

	fb := &fakeBroker{}
	a := newFieldReportsAPI(fb)
	one, two := postReportAs(a, "a@x.example", roleLapangan, "k-1", okBody), postReportAs(a, "b@x.example", roleLapangan, "k-1", okBody)

	if one.Code != 202 || two.Code != 202 || reportID(t, one) == reportID(t, two) || fb.count() != 2 {
		t.Fatalf("two users, same key: %d %d, publishes %d", one.Code, two.Code, fb.count())
	}

	if strings.Contains(one.Body.String(), "a@x.example") || strings.Contains(string(fb.got[0].id), "@") {
		t.Fatal("e-mail leaked into the report id")
	}

	if rec := postReportAs(a, "b@x.example", roleLapangan, "k-1", `{"description":"other"}`); rec.Code != http.StatusConflict {
		t.Fatalf("same user, other body: code %d, want 409", rec.Code)
	}
}

func TestEnvelope(t *testing.T) { //nolint:gocyclo // one flat list of field checks
	t.Parallel()

	fb := &fakeBroker{}
	postReport(newFieldReportsAPI(fb), roleOperator, "abc", okBody)

	if fb.count() != 1 {
		t.Fatal("not published")
	}

	m := fb.got[0]

	var e reportEnvelope
	if err := json.Unmarshal(m.body, &e); err != nil {
		t.Fatal(err)
	}

	if len(m.id) != 36 || e.ReportID != m.id || e.ReporterID != "f@x.example" || e.ReporterRole != roleOperator || e.Description != "flooded road" || e.Kind != "flood" || *e.Latitude != -6.2 || e.ReceivedAt == "" || m.correlationID == "" {
		t.Fatalf("envelope %+v msg %+v", e, m)
	}

	fb2 := &fakeBroker{}
	postReport(newFieldReportsAPI(fb2), roleOperator, "", `{"description":"d"}`)

	if strings.Contains(string(fb2.got[0].body), "latitude") || strings.Contains(string(fb2.got[0].body), "kind") {
		t.Fatalf("absent fields not omitted: %s", fb2.got[0].body)
	}
}

func TestMetricsHaveTheNewSeries(t *testing.T) {
	t.Parallel()

	a := newFieldReportsAPI(&fakeBroker{})
	postReport(a, roleLapangan, "m", okBody)
	postReport(a, roleLapangan, "m", okBody)

	a2 := newFieldReportsAPI(&fakeBroker{err: errors.New("x")})
	postReport(a2, roleLapangan, "", okBody)

	rec := httptest.NewRecorder()
	a.writeMetrics(rec)
	a2.writeMetrics(rec)

	for _, want := range []string{"client_api_field_reports_accepted_total 1", "client_api_field_reports_deduped_total 1", "client_api_field_reports_unavailable_total 1", `client_api_field_report_confirm_seconds_bucket{le="+Inf"} 1`, "client_api_field_report_confirm_seconds_count 1"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("metrics lack %q:\n%s", want, rec.Body)
		}
	}
}

func hmacSHA256(secret []byte, s string) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(s))

	return m.Sum(nil)
}

func TestRedialsWhenOnlyTheChannelCloses(t *testing.T) {
	t.Parallel()

	conn, ch := make(chan *amqp.Error), make(chan *amqp.Error, 1)
	done := make(chan struct{})

	go func() {
		waitClosed(context.Background(), conn, ch)
		close(done)
	}()

	ch <- &amqp.Error{Code: amqp.NotFound}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waitClosed ignored the channel close")
	}
}

func TestUnroutablePublishIsReported(t *testing.T) {
	t.Parallel()

	in := make(chan amqp.Return, 2)
	l := newReturnLog(in)
	in <- amqp.Return{MessageId: "a"}

	if l.unroutable("b") || !l.unroutable("a") || l.unroutable("a") {
		t.Fatal("return must match its own message id, once")
	}
}
