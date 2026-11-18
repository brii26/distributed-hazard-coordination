package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brii26/distributed-hazard-coordination/services/situation-summarizer/fakellm"
)

func testConfig() config {
	return config{interval: time.Hour, span: 5 * time.Minute, staleAfter: 2 * time.Minute, llmTimeout: 200 * time.Millisecond, bucket: time.Minute, history: 10, zThreshold: 3, minEvents: 5}
}

type stubLLM struct {
	calls atomic.Int32
	fn    func(ctx context.Context) (string, error)
}

func (s *stubLLM) Summarize(ctx context.Context, _ string) (string, error) {
	s.calls.Add(1)

	return s.fn(ctx)
}

func newTestSummarizer(l llm) (*summarizer, *window, *time.Time) {
	w, now := testWindow()
	s := newSummarizer(testConfig(), w, l, newBreaker(2, time.Minute))
	s.now = func() time.Time { return *now }

	return s, w, now
}

func TestRuleSummaryWhenAIIsOff(t *testing.T) {
	t.Parallel()

	s, w, _ := newTestSummarizer(nil)
	w.add(event("a", "Padang", "AWAS"))
	w.add(event("b", "Padang", "WASPADA"))
	w.add(event("c", "Jakarta", "WASPADA"))

	r := s.refresh(context.Background())
	if r.Source != "rule" || r.LLM != llmDisabled || r.Events != 3 {
		t.Fatalf("got %+v", r)
	}

	for _, want := range []string{"3 hazard events", "Highest severity AWAS (1 events)", "Padang 2"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary %q lacks %q", r.Summary, want)
		}
	}
}

func TestRuleSummaryOfAnEmptyWindow(t *testing.T) {
	t.Parallel()

	s, _, _ := newTestSummarizer(nil)
	if r := s.refresh(context.Background()); !strings.Contains(r.Summary, "No hazard events") || len(r.Anomalies) != 0 {
		t.Fatalf("got %+v", r)
	}
}

func TestLLMSummaryIsUsedWhenItAnswers(t *testing.T) {
	t.Parallel()

	s, w, _ := newTestSummarizer(&stubLLM{fn: func(context.Context) (string, error) { return "Calm day.", nil }})
	w.add(event("a", "Padang", "AWAS"))

	if r := s.refresh(context.Background()); r.Source != "llm" || r.Summary != "Calm day." || r.LLM != llmOK {
		t.Fatalf("got %+v", r)
	}
}

func TestFallbackWhenTheLLMErrors(t *testing.T) {
	t.Parallel()

	s, w, _ := newTestSummarizer(&stubLLM{fn: func(context.Context) (string, error) { return "", errInvalidResponse }})
	w.add(event("a", "Padang", "AWAS"))

	if r := s.refresh(context.Background()); r.Source != "rule" || r.LLM != llmFailed || !strings.Contains(r.Summary, "1 hazard events") {
		t.Fatalf("got %+v", r)
	}
}

func TestHardTimeoutEvenWhenTheClientIgnoresItsContext(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	s, w, _ := newTestSummarizer(&stubLLM{fn: func(context.Context) (string, error) { <-release; return "late", nil }})
	w.add(event("a", "Padang", "AWAS"))

	start := time.Now()
	r := s.refresh(context.Background())

	if took := time.Since(start); took > time.Second {
		t.Fatalf("refresh took %s, the timeout is 200ms", took)
	}

	if r.Source != "rule" || r.LLM != llmFailed {
		t.Fatalf("got %+v", r)
	}
}

func TestBreakerStopsCallingTheLLMAndHalfOpensLater(t *testing.T) {
	t.Parallel()

	stub := &stubLLM{fn: func(context.Context) (string, error) { return "", errors.New("down") }}
	s, w, _ := newTestSummarizer(stub)
	now := time.Unix(5000, 0)
	s.br.now = func() time.Time { return now }
	w.add(event("a", "Padang", "AWAS"))

	s.refresh(context.Background())
	s.refresh(context.Background())

	if r := s.refresh(context.Background()); r.LLM != llmBreakerOpen || stub.calls.Load() != 2 {
		t.Fatalf("llm=%s calls=%d, want breaker_open and no third call", r.LLM, stub.calls.Load())
	}

	now = now.Add(2 * time.Minute)
	stub.fn = func(context.Context) (string, error) { return "back", nil }

	if r := s.refresh(context.Background()); r.Source != "llm" || stub.calls.Load() != 3 {
		t.Fatalf("got %+v after the trial", r)
	}
}

func TestStaleMarkerAndUnavailableBeforeTheFirstSummary(t *testing.T) {
	t.Parallel()

	s, _, now := newTestSummarizer(nil)

	rec := httptest.NewRecorder()
	s.serveSummary(rec, httptest.NewRequest(http.MethodGet, "/summary", nil))

	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"unavailable"`) {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}

	s.refresh(context.Background())

	if v, _ := s.view(); v.Status != "ok" {
		t.Fatalf("fresh summary is %s", v.Status)
	}

	*now = now.Add(3 * time.Minute)

	if v, _ := s.view(); v.Status != "stale" || v.AgeSeconds != 180 {
		t.Fatalf("got %s age %d, want stale 180", v.Status, v.AgeSeconds)
	}
}

func TestInvalidJSONFromTheFakeFallsBack(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(fakellm.Handler(fakellm.ModeInvalidJSON, 0))
	t.Cleanup(srv.Close)

	s, w, _ := newTestSummarizer(newHTTPLLM(srv.URL, "m", "k", time.Second))
	w.add(event("a", "Padang", "AWAS"))

	if r := s.refresh(context.Background()); r.Source != "rule" || r.LLM != llmFailed {
		t.Fatalf("got %+v", r)
	}
}

//nolint:paralleltest // swaps the global logger
func TestKeyIsSentButNeverLoggedOrPrinted(t *testing.T) {
	const key = "sk-test-secret-123"

	var gotAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")

		http.Error(w, "nope", http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)

	var logs bytes.Buffer

	old := slog.Default()

	slog.SetDefault(newLogger(&logs))
	t.Cleanup(func() { slog.SetDefault(old) })

	l := newHTTPLLM(srv.URL, "m", key, time.Second)
	s, w, _ := newTestSummarizer(l)
	w.add(event("a", "Padang", "AWAS"))
	s.refresh(context.Background())

	if gotAuth != "Bearer "+key {
		t.Fatalf("auth header %q", gotAuth)
	}

	if strings.Contains(logs.String(), key) || strings.Contains(l.String(), key) {
		t.Fatalf("key leaked: %s", logs.String())
	}

	if logs.Len() == 0 {
		t.Fatal("expected the failure to be logged")
	}
}
