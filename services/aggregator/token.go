package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	grantAuthCode     = "authorization_code"
	grantRefreshToken = "refresh_token"

	// defaultRotateMargin is how long before expires_at a token loaded from the
	// volume is rotated; freshly issued tokens rotate at a third of their TTL.
	defaultRotateMargin = 20 * time.Second
	defaultRetryDelay   = 5 * time.Second
	maxTokenResponse    = 4096
)

// tokenFile is what lives in PVMBG_TOKEN_VOLUME_PATH (doc/TRD_M1.md 3.0).
// ExpiresAt is absolute so a restart needs no record of when it was received.
type tokenFile struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

// tokenManager owns the PVMBG OAuth token: bootstrap with the one-time code on
// the first ever start, rotation with the refresh token before expiry, and
// persistence to the volume so a restart never needs the (single-use) code again.
type tokenManager struct {
	baseURL      string
	clientID     string
	clientSecret string
	oneTimeCode  string
	path         string
	client       *http.Client
	retryDelay   time.Duration

	// mu guards cur, margin and lastErr: pollers read Token() while the
	// rotation goroutine writes.
	mu      sync.RWMutex
	cur     tokenFile
	margin  time.Duration
	lastErr error
}

func newTokenManager(baseURL, clientID, clientSecret, oneTimeCode, path string) *tokenManager {
	return &tokenManager{
		baseURL:      strings.TrimRight(baseURL, "/"),
		clientID:     clientID,
		clientSecret: clientSecret,
		oneTimeCode:  oneTimeCode,
		path:         path,
		client:       &http.Client{Timeout: 5 * time.Second},
		retryDelay:   defaultRetryDelay,
		margin:       defaultRotateMargin,
	}
}

// Token returns the current access token, or "" while none has been obtained.
func (m *tokenManager) Token() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.cur.AccessToken
}

// LastError is the most recent bootstrap/rotation failure, nil after a success.
// The PVMBG circuit breaker counts it like any failed poll.
func (m *tokenManager) LastError() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.lastErr
}

// load reads the persisted token, or bootstraps when there is none. A file that
// cannot be read is treated as missing: the worst case is a bootstrap that PVMBG
// rejects because the code was already spent, which run() keeps retrying.
func (m *tokenManager) load() error {
	b, err := os.ReadFile(m.path) //nolint:gosec // path is operator config (PVMBG_TOKEN_VOLUME_PATH), not request input
	if err == nil {
		var f tokenFile
		if err = json.Unmarshal(b, &f); err == nil && f.AccessToken != "" && f.RefreshToken != "" {
			m.mu.Lock()
			m.cur = f
			m.mu.Unlock()
			log.Printf("pvmbg token loaded from volume, expires_at=%s", f.ExpiresAt.Format(time.RFC3339))

			return nil
		}
	}

	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Printf("pvmbg token file unusable, bootstrapping: %v", err)
	}

	return m.obtain(grantAuthCode, url.Values{"code": {m.oneTimeCode}})
}

// rotate exchanges the stored refresh token for a new pair.
func (m *tokenManager) rotate() error {
	m.mu.RLock()
	refresh := m.cur.RefreshToken
	m.mu.RUnlock()

	return m.obtain(grantRefreshToken, url.Values{"refresh_token": {refresh}})
}

// obtain calls PVMBG's token endpoint and, on success, persists the new pair
// before exposing it through Token().
func (m *tokenManager) obtain(grant string, form url.Values) error {
	err := m.exchange(grant, form)

	m.mu.Lock()
	m.lastErr = err
	m.mu.Unlock()

	if err != nil {
		log.Printf("pvmbg token %s failed: %v", grant, err)
	}

	return err
}

func (m *tokenManager) exchange(grant string, form url.Values) error {
	form.Set("grant_type", grant)

	ctx, cancel := context.WithTimeout(context.Background(), m.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.baseURL+"/oauth/token", strings.NewReader(form.Encode())) //nolint:gosec // baseURL is operator config (PVMBG_BASE_URL)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(m.clientID, m.clientSecret)

	resp, err := m.client.Do(req) //nolint:gosec // see NewRequestWithContext above
	if err != nil {
		return err
	}

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponse))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return err
	}

	if tr.AccessToken == "" || tr.RefreshToken == "" || tr.ExpiresIn <= 0 {
		return errors.New("token response missing access_token, refresh_token or expires_in")
	}

	ttl := time.Duration(tr.ExpiresIn) * time.Second

	return m.store(tokenFile{
		AccessToken:  tr.AccessToken,
		RefreshToken: tr.RefreshToken,
		ExpiresAt:    time.Now().Add(ttl),
	}, ttl/3)
}

// store persists f (temp file + rename, so a crash never leaves a half-written
// file) and only then makes it current, so the volume never trails what is in use.
func (m *tokenManager) store(f tokenFile, margin time.Duration) error {
	b, err := json.Marshal(f) //nolint:gosec // persisting the token to the volume is the point (TRD 3.0)
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(m.path), ".pvmbg-token-*")
	if err != nil {
		return err
	}

	defer func() { _ = os.Remove(tmp.Name()) }() //nolint:gosec // temp file we just created; no-op once renamed

	if _, err = tmp.Write(b); err != nil {
		_ = tmp.Close()

		return err
	}

	if err = tmp.Close(); err != nil {
		return err
	}

	if err = os.Rename(tmp.Name(), m.path); err != nil { //nolint:gosec // operator-configured volume path
		return err
	}

	m.mu.Lock()
	m.cur = f
	m.margin = margin
	m.mu.Unlock()

	log.Printf("pvmbg token stored, expires_at=%s", f.ExpiresAt.Format(time.RFC3339))

	return nil
}

// nextWait is how long run() sleeps before the next attempt: until the token is
// due for rotation, or retryDelay while there is no token or the last attempt failed.
func (m *tokenManager) nextWait() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.cur.AccessToken == "" || m.lastErr != nil {
		return m.retryDelay
	}

	return max(time.Until(m.cur.ExpiresAt)-m.margin, 0)
}

// run keeps the token fresh until ctx is done. A failed attempt is logged and
// retried; it never takes the process down.
func (m *tokenManager) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(m.nextWait()):
		}

		if m.Token() == "" {
			_ = m.load()
		} else {
			_ = m.rotate()
		}
	}
}
