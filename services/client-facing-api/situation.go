package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultSituationTimeoutMS = 200
	maxSituationBody          = 64 << 10
	// A copy older than this is no longer worth showing as stale.
	maxSituationAge = 15 * time.Minute
	staleAfter      = 2 * time.Minute
)

// GET /situation: the Situation Summarizer's last summary, read-only. It reads a cached result from the summarizer under a short timeout and never reaches the alert or ingest path. When the summarizer is slow, down or answers garbage, the last good copy is served marked "stale", and with no copy the answer is 503 "unavailable". Both come back well inside 300 ms because the call is capped at SITUATION_TIMEOUT_MS (200 by default).
type situationAPI struct {
	url     string
	client  *http.Client
	timeout time.Duration
	now     func() time.Time

	mu   sync.Mutex
	last map[string]any // the last good body
	at   time.Time      // when it was generated
}

func newSituationAPI(upstream string, timeout time.Duration) *situationAPI {
	return &situationAPI{url: strings.TrimRight(upstream, "/") + "/summary", client: &http.Client{}, timeout: timeout, now: time.Now}
}

// Reads the summarizer once, nil on any failure: timeout, refusal, bad status or a body that is not a summary.
func (s *situationAPI) fetch(ctx context.Context) map[string]any {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return nil
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var body map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxSituationBody)).Decode(&body); err != nil {
		return nil
	}

	gen, _ := body["generated_at"].(string)
	if _, err := time.Parse(time.RFC3339Nano, gen); err != nil {
		return nil
	}

	if _, ok := body["summary"].(string); !ok {
		return nil
	}

	return body
}

func (s *situationAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	claims, _ := claimsFrom(r.Context())

	if claims.Role != roleLapangan && claims.Role != roleOperator {
		writeJSON(w, http.StatusForbidden, errBody("the situation summary is for Lapangan and Operator"))

		return
	}

	if body := s.fetch(r.Context()); body != nil {
		gen, _ := time.Parse(time.RFC3339Nano, body["generated_at"].(string)) //nolint:forcetypeassert // checked in fetch

		s.mu.Lock()
		s.last, s.at = body, gen
		s.mu.Unlock()

		age := s.now().Sub(gen)
		if age > staleAfter {
			body["status"] = "stale"
		} else {
			body["status"] = "ok"
		}

		body["age_seconds"] = int(age.Seconds())

		writeJSON(w, http.StatusOK, body)

		return
	}

	s.mu.Lock()
	last, at := s.last, s.at
	s.mu.Unlock()

	age := s.now().Sub(at)
	if last == nil || age > maxSituationAge {
		logFor(r.Context()).Warn("situation upstream", "result", "unavailable")
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})

		return
	}

	out := make(map[string]any, len(last)+2)
	for k, v := range last {
		out[k] = v
	}

	out["status"], out["age_seconds"] = "stale", int(age.Seconds())

	logFor(r.Context()).Warn("situation upstream", "result", "stale")
	writeJSON(w, http.StatusOK, out)
}
