package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	firstAccess  = "access-1"
	firstRefresh = "refresh-1"
)

// /oauth/token minimal yang ngitung grant, code cuma diterima sekali dan cuma refresh token terbaru.
type fakePVMBG struct {
	mu        sync.Mutex
	codeCalls int
	codeUsed  bool
	refresh   string
	n         int
	ttl       int
	reject    bool
}

func (f *fakePVMBG) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if id, secret, ok := r.BasicAuth(); !ok || id != "client" || secret != "secret" {
		w.WriteHeader(http.StatusUnauthorized)

		return
	}

	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	switch r.PostFormValue("grant_type") {
	case grantAuthCode:
		f.codeCalls++

		if f.codeUsed || r.PostFormValue("code") != "the-code" {
			invalidGrant(w)

			return
		}

		f.codeUsed = true
	case grantRefreshToken:
		if f.reject || r.PostFormValue("refresh_token") != f.refresh {
			invalidGrant(w)

			return
		}
	default:
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	f.n++
	f.refresh = "refresh-" + strconv.Itoa(f.n)

	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token":  "access-" + strconv.Itoa(f.n),
		"refresh_token": f.refresh,
		"expires_in":    f.ttl,
	})
}

// jawab kayak writeOAuthError mock asli.
func invalidGrant(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
}

// restart melupakan semua token dan status code terpakai, kayak mock asli.
func (f *fakePVMBG) restart() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.codeUsed, f.refresh = false, ""
}

func newTestManager(t *testing.T, ttl int) (*tokenManager, *fakePVMBG) {
	t.Helper()

	fake := &fakePVMBG{ttl: ttl}
	srv := httptest.NewServer(fake)

	t.Cleanup(srv.Close)

	m := newTokenManager(srv.URL, "client", "secret", "the-code", filepath.Join(t.TempDir(), "token.json"))
	m.retryDelay = 10 * time.Millisecond

	return m, fake
}

func readTokenFile(t *testing.T, path string) tokenFile {
	t.Helper()

	b, err := os.ReadFile(path) //nolint:gosec // test temp dir
	if err != nil {
		t.Fatal(err)
	}

	var f tokenFile
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}

	return f
}

func TestBootstrapPersistsToken(t *testing.T) {
	t.Parallel()

	m, _ := newTestManager(t, 60)

	if err := m.load(); err != nil {
		t.Fatal(err)
	}

	f := readTokenFile(t, m.path)
	if f.AccessToken != firstAccess || f.RefreshToken != firstRefresh || !f.ExpiresAt.After(time.Now()) {
		t.Fatalf("bad persisted token %+v", f)
	}

	if m.Token() != firstAccess {
		t.Fatalf("Token() = %q", m.Token())
	}
}

func TestRestartLoadsPersistedTokenWithoutCode(t *testing.T) {
	t.Parallel()

	m, fake := newTestManager(t, 60)
	if err := m.load(); err != nil {
		t.Fatal(err)
	}

	restarted := newTokenManager(m.baseURL, "client", "secret", "the-code", m.path)
	if err := restarted.load(); err != nil {
		t.Fatal(err)
	}

	if restarted.Token() != firstAccess {
		t.Fatalf("restart should reuse the persisted token, got %q", restarted.Token())
	}

	if fake.codeCalls != 1 {
		t.Fatalf("one-time code used %d times, want 1", fake.codeCalls)
	}
}

func TestRotatesBeforeExpiry(t *testing.T) {
	t.Parallel()

	m, _ := newTestManager(t, 1)

	if err := m.load(); err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()

	go m.run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for m.Token() == firstAccess && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if m.Token() == firstAccess {
		t.Fatal("token was not rotated")
	}

	if f := readTokenFile(t, m.path); f.RefreshToken == firstRefresh {
		t.Fatalf("file still holds the bootstrap pair: %+v", f)
	}
}

func TestRejectedRotationKeepsServingAndRecovers(t *testing.T) {
	t.Parallel()

	m, fake := newTestManager(t, 60)
	if err := m.load(); err != nil {
		t.Fatal(err)
	}

	fake.mu.Lock()
	fake.reject = true
	fake.mu.Unlock()

	if err := m.rotate(); err == nil {
		t.Fatal("expected rotation error")
	}

	if m.LastError() == nil || m.Token() != firstAccess {
		t.Fatalf("failed rotation must keep the old token and record the error (token=%q)", m.Token())
	}

	fake.mu.Lock()
	fake.reject = false
	fake.mu.Unlock()

	if err := m.rotate(); err != nil || m.LastError() != nil || m.Token() != "access-2" {
		t.Fatalf("rotation should recover: err=%v last=%v token=%q", err, m.LastError(), m.Token())
	}
}

func TestBootstrapFailureDoesNotCrashAndRetries(t *testing.T) {
	t.Parallel()

	m, fake := newTestManager(t, 60)
	m.oneTimeCode = "wrong"

	if err := m.load(); err == nil || m.Token() != "" {
		t.Fatalf("expected a failed bootstrap, err=%v token=%q", err, m.Token())
	}

	m.mu.Lock()
	m.oneTimeCode = "the-code"
	m.mu.Unlock()

	go m.run(t.Context())

	deadline := time.Now().Add(2 * time.Second)
	for m.Token() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if m.Token() == "" || fake.codeCalls < 2 {
		t.Fatalf("bootstrap should be retried (token=%q, code calls=%d)", m.Token(), fake.codeCalls)
	}
}

func TestRotationAfterAPVMBGRestartFallsBackToBootstrap(t *testing.T) {
	t.Parallel()

	m, fake := newTestManager(t, 60)
	if err := m.load(); err != nil {
		t.Fatal(err)
	}

	fake.restart()

	if err := m.rotate(); err != nil {
		t.Fatalf("rotation must recover through a fresh bootstrap, got %v", err)
	}

	if m.LastError() != nil || m.Token() != "access-2" {
		t.Fatalf("expected the new pair in use, got token=%q last=%v", m.Token(), m.LastError())
	}

	if got := readTokenFile(t, m.path); got.AccessToken != "access-2" || got.RefreshToken != "refresh-2" {
		t.Fatalf("the new pair must be persisted, got %+v", got)
	}

	fake.mu.Lock()
	calls := fake.codeCalls
	fake.mu.Unlock()

	if calls != 2 {
		t.Fatalf("expected the one-time code once at start and once in the fallback, got %d", calls)
	}

	if err := m.rotate(); err != nil || m.Token() != "access-3" {
		t.Fatalf("expected a normal rotation afterwards, got token=%q err=%v", m.Token(), err)
	}
}

func TestAFailedFallbackKeepsTheStoredToken(t *testing.T) {
	t.Parallel()

	m, fake := newTestManager(t, 60)
	if err := m.load(); err != nil {
		t.Fatal(err)
	}

	fake.mu.Lock()
	fake.reject = true
	fake.mu.Unlock()

	if err := m.rotate(); !errors.Is(err, errInvalidGrant) {
		t.Fatalf("expected the fallback to fail with invalid_grant, got %v", err)
	}

	if m.Token() != firstAccess || readTokenFile(t, m.path).RefreshToken != firstRefresh {
		t.Fatal("a failed fallback must leave the stored pair untouched")
	}
}
