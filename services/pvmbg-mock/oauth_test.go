package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	testClientID     = "client"
	testClientSecret = "secret"
	testCode         = "one-time-code"
)

func newTestStore() *tokenStore {
	return newTokenStore(testClientID, testClientSecret, testCode)
}

func postToken(t *testing.T, store *tokenStore, secret string, form url.Values) (int, map[string]any) {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/oauth/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(testClientID, secret)

	rec := httptest.NewRecorder()
	newMux(store, testAdminKey).ServeHTTP(rec, req)

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad json %q: %v", rec.Body.String(), err)
	}

	return rec.Code, body
}

func codeForm(code string) url.Values {
	return url.Values{"grant_type": {grantAuthCode}, "code": {code}}
}

func refreshForm(token string) url.Values {
	return url.Values{"grant_type": {grantRefreshToken}, "refresh_token": {token}}
}

func TestOAuthOneTimeCodeAndRotation(t *testing.T) {
	t.Parallel()

	store := newTestStore()

	status, first := postToken(t, store, testClientSecret, codeForm(testCode))
	if status != http.StatusOK || first["expires_in"] != float64(tokenTTLSeconds) {
		t.Fatalf("first exchange: got %d %v", status, first)
	}

	if status, _ = postToken(t, store, testClientSecret, codeForm(testCode)); status != http.StatusBadRequest {
		t.Fatalf("replayed one-time code: expected 400, got %d", status)
	}

	oldRefresh, _ := first["refresh_token"].(string)

	status, second := postToken(t, store, testClientSecret, refreshForm(oldRefresh))
	if status != http.StatusOK || second["refresh_token"] == oldRefresh || second["access_token"] == first["access_token"] {
		t.Fatalf("refresh should rotate both tokens: got %d %v", status, second)
	}

	if status, _ = postToken(t, store, testClientSecret, refreshForm(oldRefresh)); status != http.StatusBadRequest {
		t.Fatalf("old refresh token must stop working: got %d", status)
	}
}

func TestOAuthRejects(t *testing.T) {
	t.Parallel()

	store := newTestStore()

	if status, _ := postToken(t, store, "wrong", codeForm(testCode)); status != http.StatusUnauthorized {
		t.Fatalf("wrong secret: expected 401, got %d", status)
	}

	if status, _ := postToken(t, store, testClientSecret, url.Values{"grant_type": {"password"}}); status != http.StatusBadRequest {
		t.Fatalf("unsupported grant: expected 400, got %d", status)
	}

	if status, _ := postToken(t, store, testClientSecret, refreshForm("nope")); status != http.StatusBadRequest {
		t.Fatalf("refresh before any issuance: expected 400, got %d", status)
	}

	// A wrong code must not burn the real one.
	if status, _ := postToken(t, store, testClientSecret, codeForm("bad")); status != http.StatusBadRequest {
		t.Fatalf("wrong code: expected 400, got %d", status)
	}

	if status, _ := postToken(t, store, testClientSecret, codeForm(testCode)); status != http.StatusOK {
		t.Fatalf("real code after a wrong one: expected 200, got %d", status)
	}
}

func TestOAuthOneTimeCodeConcurrent(t *testing.T) {
	t.Parallel()

	store := newTestStore()

	var wg sync.WaitGroup

	var ok atomic.Int32

	for range 20 {
		wg.Go(func() {
			if status, _ := postToken(t, store, testClientSecret, codeForm(testCode)); status == http.StatusOK {
				ok.Add(1)
			}
		})
	}

	wg.Wait()

	if ok.Load() != 1 {
		t.Fatalf("exactly one concurrent exchange may succeed, got %d", ok.Load())
	}
}
