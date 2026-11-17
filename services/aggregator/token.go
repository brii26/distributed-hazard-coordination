package main

import (
	"context"
	"database/sql"
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

	defaultRotateMargin = 20 * time.Second
	defaultRetryDelay   = 5 * time.Second
	maxTokenResponse    = 4096
)

// Contents of the token volume file. ExpiresAt is absolute, so a restart needs no record of when it was received.
type tokenFile struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// Shared home of the token pair, so the next lease holder continues with the pair the last one rotated.
type tokenStore interface {
	// ok is false when no pair is stored yet.
	load(ctx context.Context) (f tokenFile, epoch int64, ok bool, err error)
	// Refuses with errStaleToken if the stored pair belongs to a higher epoch. Compare and write are one statement.
	save(ctx context.Context, f tokenFile, epoch int64) error
}

var (
	errStaleToken = errors.New("token pair belongs to a newer lease epoch")
	errNotLeader  = errors.New("pvmbg token manager is not running here, this replica does not hold the lease")
)

type pgTokenStore struct {
	db     *sql.DB
	source string
}

const (
	loadTokenSQL = `SELECT access_token, refresh_token, expires_at, epoch FROM source_token WHERE source = $1`
	saveTokenSQL = `
INSERT INTO source_token (source, access_token, refresh_token, expires_at, epoch)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (source) DO UPDATE
SET access_token = EXCLUDED.access_token, refresh_token = EXCLUDED.refresh_token, expires_at = EXCLUDED.expires_at, epoch = EXCLUDED.epoch, updated_at = now()
WHERE source_token.epoch <= EXCLUDED.epoch`
)

func (s pgTokenStore) load(ctx context.Context) (tokenFile, int64, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	var (
		f     tokenFile
		epoch int64
	)

	err := s.db.QueryRowContext(ctx, loadTokenSQL, s.source).Scan(&f.AccessToken, &f.RefreshToken, &f.ExpiresAt, &epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return tokenFile{}, 0, false, nil
	}

	if err != nil {
		return tokenFile{}, 0, false, err
	}

	return f, epoch, true, nil
}

func (s pgTokenStore) save(ctx context.Context, f tokenFile, epoch int64) error {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	res, err := s.db.ExecContext(ctx, saveTokenSQL, s.source, f.AccessToken, f.RefreshToken, f.ExpiresAt, epoch)
	if err != nil {
		return err
	}

	if n, err := res.RowsAffected(); err != nil || n == 0 {
		if err != nil {
			return err
		}

		return errStaleToken
	}

	return nil
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

// Owns the PVMBG OAuth token: bootstrap with the one-time code, rotate with the refresh token, persist so neither a restart nor a takeover needs the code again.
// It only talks to PVMBG between lead() starting and ending, which is while this replica holds the lease. The pair goes to the shared store (when set) and to the volume file.
type tokenManager struct {
	baseURL       string
	clientID      string
	clientSecret  string
	oneTimeCode   string
	path          string
	shared        tokenStore // nil: the volume file is the only copy
	pendingShared bool       // cur is not in the shared store yet
	epoch         int64      // lease epoch of the current term, 0 without a lease
	client        *http.Client
	retryDelay    time.Duration
	kick          chan struct{} // Reject wakes run() to renew now

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
		kick:         make(chan struct{}, 1),
		margin:       defaultRotateMargin,
		lastErr:      errNotLeader,
	}
}

// Current access token, or "" if none yet.
func (m *tokenManager) Token() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.cur.AccessToken
}

// Called when PVMBG refuses the access token (401). run() renews at once instead of waiting for the next rotation, and calls coalesce.
func (m *tokenManager) Reject() {
	select {
	case m.kick <- struct{}{}:
	default:
	}
}

// Last bootstrap or rotation error (nil after a success). The PVMBG breaker counts it as a failed poll.
func (m *tokenManager) LastError() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.lastErr
}

// Takes the stored pair, or bootstraps if there is none. Order: shared store, volume file, one-time code. A volume file that cannot be read counts as missing. A shared store that cannot be read is an error: the file may be older than the shared pair, so neither it nor the one time code is tried until the store answers.
func (m *tokenManager) load() error {
	if m.shared != nil {
		f, _, ok, err := m.shared.load(context.Background())

		switch {
		case err != nil:
			// The file may hold a pair another pod already rotated, and the one time code is spent. Wait for the store instead.
			err = fmt.Errorf("shared token store: %w", err)

			m.mu.Lock()
			m.lastErr = err
			m.mu.Unlock()

			log.Printf("pvmbg shared token unreadable, not using the volume or the one time code: %v", err)

			return err
		case ok && f.AccessToken != "" && f.RefreshToken != "":
			m.adopt(f)
			log.Printf("pvmbg token loaded from shared store, expires_at=%s", f.ExpiresAt.Format(time.RFC3339))
			m.writeFile(f)

			return nil
		}
	}

	b, err := os.ReadFile(m.path) //nolint:gosec // path is operator config (PVMBG_TOKEN_VOLUME_PATH), not request input
	if err == nil {
		var f tokenFile
		if err = json.Unmarshal(b, &f); err == nil && f.AccessToken != "" && f.RefreshToken != "" {
			m.adopt(f)
			log.Printf("pvmbg token loaded from volume, expires_at=%s", f.ExpiresAt.Format(time.RFC3339))

			if m.shared != nil {
				if err := m.shared.save(context.Background(), f, m.epoch); err != nil {
					log.Printf("pvmbg token not copied to the shared store: %v", err)
				}
			}

			return nil
		}
	}

	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Printf("pvmbg token file unusable, bootstrapping: %v", err)
	}

	return m.obtain(grantAuthCode, url.Values{"code": {m.oneTimeCode}})
}

// Makes a loaded pair current and clears the not-leader error.
func (m *tokenManager) adopt(f tokenFile) {
	m.mu.Lock()
	m.cur = f
	m.lastErr = nil
	m.mu.Unlock()
}

// Runs fn as the PVMBG token holder: loads or bootstraps the pair, keeps it fresh in the background and, when fn returns or ctx ends, stops and forgets it. ctx carries the lease epoch.
func (m *tokenManager) lead(ctx context.Context, fn func(context.Context)) {
	m.mu.Lock()
	m.epoch, _ = leaseEpoch(ctx)
	m.mu.Unlock()

	_ = m.load()

	m.mu.RLock()
	due := m.cur.AccessToken != "" && time.Until(m.cur.ExpiresAt) <= m.margin
	m.mu.RUnlock()

	if due {
		_ = m.rotate()
	}

	rctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup

	wg.Go(func() { m.run(rctx) })

	defer func() {
		cancel()
		wg.Wait()

		m.mu.Lock()
		m.cur, m.lastErr = tokenFile{}, errNotLeader
		m.mu.Unlock()
	}()

	fn(ctx)
}

// Swaps the refresh token for a new pair. errInvalidGrant means PVMBG rejected the grant itself (unknown refresh token).
var errInvalidGrant = errors.New("invalid_grant")

func (m *tokenManager) rotate() error {
	m.flushShared()

	m.mu.RLock()
	refresh := m.cur.RefreshToken
	m.mu.RUnlock()

	err := m.obtain(grantRefreshToken, url.Values{"refresh_token": {refresh}})
	if !errors.Is(err, errInvalidGrant) {
		return err
	}

	// Another holder may have rotated the pair already. Try the stored one before the one time code, which is spent after the first use.
	if next, ok := m.newerSharedRefresh(refresh); ok {
		withID("").Warn("token refresh retry", "reason", "invalid_grant, shared store holds a newer pair")

		err = m.obtain(grantRefreshToken, url.Values{"refresh_token": {next}})
		if !errors.Is(err, errInvalidGrant) {
			return err
		}
	}

	withID("").Warn("token bootstrap fallback", "reason", "refresh token rejected with invalid_grant")

	return m.obtain(grantAuthCode, url.Values{"code": {m.oneTimeCode}})
}

// The refresh token in the shared store when it differs from used, so it is a newer pair.
func (m *tokenManager) newerSharedRefresh(used string) (string, bool) {
	if m.shared == nil {
		return "", false
	}

	f, _, ok, err := m.shared.load(context.Background())
	if err != nil || !ok || f.RefreshToken == "" || f.RefreshToken == used {
		return "", false
	}

	return f.RefreshToken, true
}

// Retries a shared save that failed earlier. A refused save means a newer term owns the pair, so this holder drops it.
func (m *tokenManager) flushShared() {
	m.mu.RLock()
	pending, f, epoch := m.pendingShared, m.cur, m.epoch
	m.mu.RUnlock()

	if !pending || m.shared == nil || f.AccessToken == "" {
		return
	}

	err := m.shared.save(context.Background(), f, epoch)

	switch {
	case err == nil:
		m.mu.Lock()
		m.pendingShared = false
		m.mu.Unlock()
	case errors.Is(err, errStaleToken):
		m.mu.Lock()
		m.cur, m.pendingShared, m.lastErr = tokenFile{}, false, err
		m.mu.Unlock()
	default:
		log.Printf("pvmbg token still not in the shared store: %v", err)
	}
}

// Calls PVMBG's token endpoint and persists the new pair before exposing it through Token().
func (m *tokenManager) obtain(grant string, form url.Values) error {
	err := m.exchange(grant, form)

	m.mu.Lock()
	m.lastErr = err
	m.mu.Unlock()

	if err != nil {
		log.Printf("pvmbg token %s failed: %v", grant, err)

		return err
	}

	msg := "token bootstrapped"
	if grant == grantRefreshToken {
		msg = "token rotated"
	}

	m.mu.RLock()
	expires := m.cur.ExpiresAt
	m.mu.RUnlock()

	withID("").Info(msg, "grant", grant, "expires_at", expires.Format(time.RFC3339), "result", "success")

	return nil
}

func (m *tokenManager) exchange(grant string, form url.Values) (err error) {
	form.Set("grant_type", grant)

	start := time.Now()
	defer func() { logCall(context.Background(), "POST "+m.baseURL+"/oauth/token grant_type="+grant, start, err) }()

	ctx, cancel := context.WithTimeout(context.Background(), m.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.baseURL+"/oauth/token", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(m.clientID, m.clientSecret)

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponse))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		var oauthErr struct {
			Error string `json:"error"`
		}

		if json.Unmarshal(body, &oauthErr) == nil && oauthErr.Error == "invalid_grant" {
			return fmt.Errorf("token endpoint returned %d: %w", resp.StatusCode, errInvalidGrant)
		}

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

// Persists f (shared store with the epoch, then the volume file) and only then makes it current, so neither copy lags behind. A refused shared write means a newer term owns the pair.
func (m *tokenManager) store(f tokenFile, margin time.Duration) error {
	m.mu.RLock()
	epoch := m.epoch
	m.mu.RUnlock()

	pending := false

	if m.shared != nil {
		if err := m.shared.save(context.Background(), f, epoch); err != nil {
			if errors.Is(err, errStaleToken) {
				return fmt.Errorf("saving the token pair: %w", err)
			}

			// PVMBG already rotated, so this pair is the only valid one. Keep it here and retry the save.
			log.Printf("pvmbg token not saved to the shared store, keeping it locally: %v", err)

			pending = true
		}
	}

	if err := m.writeFile(f); err != nil {
		return err
	}

	m.mu.Lock()
	m.cur = f
	m.margin = margin
	m.pendingShared = pending
	m.mu.Unlock()

	log.Printf("pvmbg token stored, expires_at=%s", f.ExpiresAt.Format(time.RFC3339))

	return nil
}

// Writes the volume copy (temp file, then rename).
func (m *tokenManager) writeFile(f tokenFile) error {
	b, err := json.Marshal(f) //nolint:gosec // persisting the token to the volume is the point
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

	return os.Rename(tmp.Name(), m.path) //nolint:gosec // operator-configured volume path
}

// Waits until the token is due for rotation, or retryDelay if there is no token or the last attempt failed.
func (m *tokenManager) nextWait() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.cur.AccessToken == "" || m.lastErr != nil {
		return m.retryDelay
	}

	due := max(time.Until(m.cur.ExpiresAt)-m.margin, 0)
	if m.pendingShared {
		return min(due, m.retryDelay)
	}

	return due
}

// Keeps the token fresh until ctx is done. Failures are logged and retried.
func (m *tokenManager) run(ctx context.Context) {
	for {
		forced := false

		select {
		case <-ctx.Done():
			return
		case <-time.After(m.nextWait()):
		case <-m.kick:
			forced = true
		}

		if m.Token() == "" {
			_ = m.load()

			continue
		}

		m.flushShared()

		if forced || m.refreshDue() {
			_ = m.rotate()
		}
	}
}

// False while the current pair is still valid for longer than the margin, so a retry of the shared save does not burn a refresh.
func (m *tokenManager) refreshDue() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.cur.AccessToken != "" && time.Until(m.cur.ExpiresAt) <= m.margin
}
