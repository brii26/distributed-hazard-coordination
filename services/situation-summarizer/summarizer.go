package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// Why the summary is not from the LLM, for the answer and the metrics.
const (
	llmDisabled    = "disabled"
	llmOK          = "ok"
	llmFailed      = "failed"
	llmBreakerOpen = "breaker_open"
)

type result struct {
	Summary       string    `json:"summary"`
	Source        string    `json:"source"` // "rule" or "llm"
	LLM           string    `json:"llm"`
	Events        int       `json:"events"`
	WindowSeconds int       `json:"window_seconds"`
	Anomalies     []anomaly `json:"anomalies"`
	GeneratedAt   time.Time `json:"generated_at"`
}

// What GET /summary answers: the cached result plus how old it is.
type view struct {
	Status     string `json:"status"` // "ok" or "stale"
	AgeSeconds int    `json:"age_seconds"`
	result
}

type config struct {
	interval, span, staleAfter, llmTimeout, bucket time.Duration
	history                                        int
	zThreshold, minEvents                          float64
}

// Builds the summary on a timer and keeps the newest one. Requests only ever read that cache, so a slow or dead LLM can delay the next refresh but never an answer.
type summarizer struct {
	cfg config
	win *window
	llm llm // nil when AI is off
	br  *breaker
	now func() time.Time

	cur atomic.Pointer[result]

	// Counters for /metrics.
	ruleSummaries, llmSummaries, llmFailures, llmSkipped atomic.Int64
}

func newSummarizer(cfg config, win *window, l llm, br *breaker) *summarizer {
	return &summarizer{cfg: cfg, win: win, llm: l, br: br, now: time.Now}
}

var severityRank = map[string]int{"AWAS": 4, "SIAGA": 3, "WASPADA": 2, "NORMAL": 1}

// The rule based summary, also the facts handed to the LLM.
func (s *summarizer) facts(events []stamped, an []anomaly) string {
	if len(events) == 0 {
		return fmt.Sprintf("No hazard events in the last %d minutes.", int(s.cfg.span.Minutes()))
	}

	byType, byArea, bySeverity := map[string]int{}, map[string]int{}, map[string]int{}
	for _, e := range events {
		byType[e.hazard]++
		byArea[e.area]++
		bySeverity[e.severity]++
	}

	var b strings.Builder

	fmt.Fprintf(&b, "%d hazard events in the last %d minutes (%s).", len(events), int(s.cfg.span.Minutes()), top(byType, 3))

	worst := ""
	for sev := range bySeverity {
		if worst == "" || severityRank[sev] > severityRank[worst] || (severityRank[sev] == severityRank[worst] && sev < worst) {
			worst = sev
		}
	}

	fmt.Fprintf(&b, " Highest severity %s (%d events). Busiest areas: %s.", worst, bySeverity[worst], top(byArea, 3))

	for _, a := range an {
		fmt.Fprintf(&b, " Unusual rate in %s: %g events against a usual %g (z=%g).", a.Area, a.Count, a.Mean, a.Z)
	}

	return b.String()
}

// "name n, name n" for the largest counts, ties by name.
func top(m map[string]int, n int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool { return m[keys[i]] > m[keys[j]] || (m[keys[i]] == m[keys[j]] && keys[i] < keys[j]) })

	parts := make([]string, 0, n)
	for _, k := range keys[:min(n, len(keys))] {
		parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
	}

	return strings.Join(parts, ", ")
}

// Builds one summary and stores it. The LLM is asked only when AI is on and the breaker allows it, with a hard timeout, and any failure leaves the rule based text.
func (s *summarizer) refresh(ctx context.Context) result {
	events := s.win.since(s.cfg.span)
	an := s.win.anomalies(s.cfg.bucket, s.cfg.history, s.cfg.zThreshold, s.cfg.minEvents)
	facts := s.facts(events, an)

	r := result{Summary: facts, Source: "rule", LLM: llmDisabled, Events: len(events), WindowSeconds: int(s.cfg.span.Seconds()), Anomalies: an, GeneratedAt: s.now()}
	if r.Anomalies == nil {
		r.Anomalies = []anomaly{}
	}

	switch {
	case s.llm == nil:
	case len(events) == 0:
		r.LLM = llmOK // nothing to rewrite, no call
	case !s.br.allow():
		r.LLM = llmBreakerOpen
		s.llmSkipped.Add(1)
	default:
		text, err := s.ask(ctx, facts)
		s.br.done(err == nil)

		if err != nil {
			r.LLM = llmFailed
			s.llmFailures.Add(1)
			slog.Warn("llm summary failed, using the rule summary", "error", err.Error(), "breaker", s.br.current().String())
		} else {
			r.Summary, r.Source, r.LLM = text, "llm", llmOK
		}
	}

	if r.Source == "llm" {
		s.llmSummaries.Add(1)
	} else {
		s.ruleSummaries.Add(1)
	}

	s.cur.Store(&r)

	return r
}

// One LLM call under the hard timeout, whatever the client does with it.
func (s *summarizer) ask(ctx context.Context, facts string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.llmTimeout)
	defer cancel()

	type answer struct {
		text string
		err  error
	}

	ch := make(chan answer, 1)

	go func() {
		t, err := s.llm.Summarize(ctx, facts)
		ch <- answer{t, err}
	}()

	select {
	case a := <-ch:
		return a.text, a.err
	case <-ctx.Done():
		return "", fmt.Errorf("llm timeout after %s", s.cfg.llmTimeout)
	}
}

// Refreshes once at start and then every interval until ctx ends.
func (s *summarizer) run(ctx context.Context) {
	s.refresh(ctx)

	t := time.NewTicker(s.cfg.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.refresh(ctx)
		}
	}
}

// The cached result with its age, false before the first refresh.
func (s *summarizer) view() (view, bool) {
	r := s.cur.Load()
	if r == nil {
		return view{}, false
	}

	age := s.now().Sub(r.GeneratedAt)
	v := view{Status: "ok", AgeSeconds: int(age.Seconds()), result: *r}

	if age > s.cfg.staleAfter {
		v.Status = "stale"
	}

	return v, true
}

// GET /summary: a memory read, no lock held while writing and no call to the LLM or the broker.
func (s *summarizer) serveSummary(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	v, ok := s.view()
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "unavailable"})

		return
	}

	_ = json.NewEncoder(w).Encode(v)
}

func (s *summarizer) writeMetrics(w http.ResponseWriter) {
	_, _ = fmt.Fprintf(w, "# HELP situation_summaries_total Summaries built, by source.\n# TYPE situation_summaries_total counter\n"+
		"situation_summaries_total{source=\"rule\"} %d\nsituation_summaries_total{source=\"llm\"} %d\n"+
		"# HELP situation_llm_failures_total LLM calls that failed or timed out.\n# TYPE situation_llm_failures_total counter\nsituation_llm_failures_total %d\n"+
		"# HELP situation_llm_skipped_total LLM calls skipped by the open circuit breaker.\n# TYPE situation_llm_skipped_total counter\nsituation_llm_skipped_total %d\n"+
		"# HELP situation_breaker_open 1 while the LLM circuit breaker is not closed.\n# TYPE situation_breaker_open gauge\nsituation_breaker_open %d\n",
		s.ruleSummaries.Load(), s.llmSummaries.Load(), s.llmFailures.Load(), s.llmSkipped.Load(), s.breakerGauge())
}

func (s *summarizer) breakerGauge() int {
	if s.br.current() == closed {
		return 0
	}

	return 1
}
