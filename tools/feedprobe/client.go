package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultBaseURL = "https://satellite.naufarrel.dev"
	tokenEnv       = "SATELLITE_GROUP_TOKEN"
	// badToken is sent by --bad-token to observe the 401 answer; it is not a secret.
	badToken     = "feedprobe-invalid-token"
	maxBodyBytes = 64 << 10
)

// feedClient talks to the feed. The token lives only in memory and only in the Authorization header.
type feedClient struct {
	base  *url.URL
	token string
	http  *http.Client
}

func newFeedClient(baseURL string, useBadToken bool) (*feedClient, error) {
	token := os.Getenv(tokenEnv)
	if useBadToken {
		token = badToken
	}

	if token == "" {
		return nil, fmt.Errorf("%s is not set", tokenEnv)
	}

	base, err := url.Parse(baseURL)
	if err != nil || (base.Scheme != "https" && base.Scheme != "http") || base.Host == "" {
		return nil, fmt.Errorf("invalid --base-url %q", baseURL)
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ForceAttemptHTTP2:     true,
		// A fresh TCP connection per stream request, so each attempt is a real new connection.
		DisableKeepAlives: true,
	}

	// No Client.Timeout: a stream stays open for as long as the capture runs.
	return &feedClient{base: base, token: token, http: &http.Client{Transport: transport}}, nil
}

func (c *feedClient) request(ctx context.Context, path string, query url.Values, lastEventID string) (*http.Request, error) {
	u := c.base.JoinPath(path)
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)

	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}

	return req, nil
}

// session fetches GET /v1/session once and returns the status and raw body.
func (c *feedClient) session(ctx context.Context) (int, []byte, error) {
	req, err := c.request(ctx, "/v1/session", nil, "")
	if err != nil {
		return 0, nil, err
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("GET /v1/session: %w", err)
	}

	defer func() { _ = resp.Body.Close() }() // body fully read below

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return resp.StatusCode, body, fmt.Errorf("read /v1/session body: %w", err)
	}

	return resp.StatusCode, body, nil
}

// safeHeaders copies response headers, leaving out anything that could carry credentials.
func safeHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))

	for name, values := range h {
		lower := strings.ToLower(name)
		if lower == "set-cookie" || lower == "authorization" || lower == "proxy-authorization" {
			continue
		}

		out[name] = strings.Join(values, ", ")
	}

	return out
}

// retryAfter reads Retry-After as seconds or an HTTP date; zero when absent or invalid.
func retryAfter(h http.Header, now time.Time) time.Duration {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}

	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}

	if at, err := http.ParseTime(v); err == nil && at.After(now) {
		return at.Sub(now)
	}

	return 0
}

// sleepCtx waits d or until ctx ends, and reports whether the full wait happened.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
