package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	maxLLMResponse = 64 << 10
	maxSummaryLen  = 1000
)

var errInvalidResponse = errors.New("llm answered something that is not a summary")

// The one thing the summarizer needs from a language model. Everything else (timeout, breaker, fallback) is outside it.
type llm interface {
	// Rewrites the facts as a short situation summary. Must honour ctx.
	Summarize(ctx context.Context, facts string) (string, error)
}

// Calls a JSON endpoint: POST {"model","prompt"} with a bearer key, expects {"summary":"..."}. The key lives only in this struct, it is never logged or put in an error.
type httpLLM struct {
	url, model string
	key        string
	client     *http.Client
}

func newHTTPLLM(url, model, key string, timeout time.Duration) *httpLLM {
	return &httpLLM{url: url, model: model, key: key, client: &http.Client{Timeout: timeout}}
}

// Redacts the key if the struct is ever printed.
func (h *httpLLM) String() string { return "httpLLM(" + h.url + ")" }

func (h *httpLLM) Summarize(ctx context.Context, facts string) (string, error) {
	body, err := json.Marshal(map[string]string{
		"model":  h.model,
		"prompt": "Write a situation summary of at most two sentences for disaster field teams from these facts, use only the facts:\n" + facts,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	if err != nil {
		return "", errors.New("bad llm request")
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.key)

	resp, err := h.client.Do(req)
	if err != nil {
		// The client error holds the URL, never a header.
		return "", fmt.Errorf("llm call: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("llm status %d", resp.StatusCode)
	}

	var out struct {
		Summary string `json:"summary"`
	}

	if err := json.NewDecoder(io.LimitReader(resp.Body, maxLLMResponse)).Decode(&out); err != nil {
		return "", errInvalidResponse
	}

	text := strings.TrimSpace(out.Summary)
	if text == "" {
		return "", errInvalidResponse
	}

	if len(text) > maxSummaryLen {
		text = text[:maxSummaryLen]
	}

	return text, nil
}
