package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/brii26/distributed-hazard-coordination/services/situation-summarizer/fakellm"
)

// GET /summary is a cache read, so it stays fast while a refresh is stuck on the fake LLM in each mode (slow sleeps the full 60 s).
func TestSummaryAnswersUnder300msWithEachFakeLLMMode(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{fakellm.ModeOK, fakellm.ModeSlow, fakellm.ModeInvalidJSON, fakellm.ModeDead} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			fake := httptest.NewServer(fakellm.Handler(mode, fakellm.DefaultSleep))
			t.Cleanup(fake.Close)

			cfg := testConfig()
			cfg.llmTimeout = 2 * time.Second
			w := newWindow(time.Hour)
			s := newSummarizer(cfg, w, newHTTPLLM(fake.URL, "m", "k", cfg.llmTimeout), newBreaker(3, time.Minute))
			w.add(event("a", "Padang", "AWAS"))

			api := httptest.NewServer(newMux(s, func() bool { return true }, new(atomicCounter)))
			t.Cleanup(api.Close)

			get := func() (int, time.Duration) {
				start := time.Now()

				resp, err := http.Get(api.URL + "/summary") //nolint:noctx // test
				if err != nil {
					t.Fatal(err)
				}

				_ = resp.Body.Close()

				return resp.StatusCode, time.Since(start)
			}

			// Before the first summary exists: unavailable, and fast.
			if code, d := get(); code != http.StatusServiceUnavailable || d > 300*time.Millisecond {
				t.Fatalf("before the first summary: %d in %s", code, d)
			}

			s.refresh(context.Background()) // a cached result, rule or llm

			var wg sync.WaitGroup

			wg.Go(func() { s.refresh(context.Background()) }) // a refresh that may sit on the fake for its whole timeout

			for range 20 {
				if code, d := get(); code != http.StatusOK || d > 300*time.Millisecond {
					t.Fatalf("during a refresh: %d in %s", code, d)
				}
			}

			wg.Wait()
		})
	}
}
