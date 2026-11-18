package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// In-memory store with the same epoch rule as the Postgres one.
type memTokenStore struct {
	mu    sync.Mutex
	f     tokenFile
	epoch int64
	set   bool
}

func (s *memTokenStore) load(context.Context) (tokenFile, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.f, s.epoch, s.set, nil
}

func (s *memTokenStore) save(_ context.Context, f tokenFile, epoch int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.set && s.epoch > epoch {
		return errStaleToken
	}

	s.f, s.epoch, s.set = f, epoch, true

	return nil
}

// Store variants every shared store test runs against. The Postgres one skips without CANONICAL_DB_TEST_URL.
func tokenStores(t *testing.T) map[string]func(t *testing.T) tokenStore {
	t.Helper()

	return map[string]func(t *testing.T) tokenStore{
		"memory": func(*testing.T) tokenStore { return &memTokenStore{} },
		"postgres": func(t *testing.T) tokenStore {
			t.Helper()

			s := testStore(t, 2)
			src := "TEST-" + strconv.FormatInt(time.Now().UnixNano(), 10)

			t.Cleanup(func() {
				_, _ = s.db.ExecContext(context.Background(), `DELETE FROM source_token WHERE source = $1`, src)
			})

			return pgTokenStore{db: s.db, source: src}
		},
	}
}

func TestSharedTokenStoreRefusesALowerEpoch(t *testing.T) {
	for name, mk := range tokenStores(t) {
		t.Run(name, func(t *testing.T) {
			st, ctx := mk(t), context.Background()
			exp := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)

			if _, _, ok, err := st.load(ctx); err != nil || ok {
				t.Fatalf("an empty store must load as missing, got ok=%v err=%v", ok, err)
			}

			newer := tokenFile{AccessToken: "a2", RefreshToken: "r2", ExpiresAt: exp}
			if err := st.save(ctx, newer, 2); err != nil {
				t.Fatal(err)
			}

			if err := st.save(ctx, tokenFile{AccessToken: "a1", RefreshToken: "r1", ExpiresAt: exp}, 1); !errors.Is(err, errStaleToken) {
				t.Fatalf("epoch 1 after epoch 2 must be refused, got %v", err)
			}

			got, epoch, ok, err := st.load(ctx)
			if err != nil || !ok || epoch != 2 || got.AccessToken != "a2" || got.RefreshToken != "r2" || !got.ExpiresAt.Equal(exp) {
				t.Fatalf("the epoch 2 pair must survive, got %+v epoch=%d ok=%v err=%v", got, epoch, ok, err)
			}

			if err := st.save(ctx, tokenFile{AccessToken: "a3", RefreshToken: "r3", ExpiresAt: exp}, 2); err != nil {
				t.Fatalf("the same epoch rotates its own pair, got %v", err)
			}

			if err := st.save(ctx, tokenFile{AccessToken: "a4", RefreshToken: "r4", ExpiresAt: exp}, 3); err != nil {
				t.Fatalf("a higher epoch takes over, got %v", err)
			}
		})
	}
}

// PVMBG stand-in: the fake token endpoint plus a resource that only accepts the newest access token.
func newPVMBGServer(t *testing.T, ttl int) (*httptest.Server, *fakePVMBG) {
	t.Helper()

	fake := &fakePVMBG{ttl: ttl}
	mux := http.NewServeMux()
	mux.Handle("/oauth/token", fake)
	mux.HandleFunc("/volcanic-reports", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		want := "Bearer access-" + strconv.Itoa(fake.n)
		fake.mu.Unlock()

		if r.Header.Get("Authorization") != want {
			w.WriteHeader(http.StatusUnauthorized)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv, fake
}

func replicaManager(t *testing.T, srv *httptest.Server, st tokenStore) *tokenManager {
	t.Helper()

	m := newTokenManager(srv.URL, "client", "secret", "the-code", filepath.Join(t.TempDir(), "token.json"))
	m.retryDelay = 10 * time.Millisecond
	m.shared = st

	return m
}

// Polls the resource with the manager's current token.
func pollWith(t *testing.T, srv *httptest.Server, m *tokenManager) int {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/volcanic-reports", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Authorization", "Bearer "+m.Token())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	_ = resp.Body.Close()

	return resp.StatusCode
}

func TestSuccessorContinuesWithTheStoredPairAndNeverUsesTheCode(t *testing.T) {
	for name, mk := range tokenStores(t) {
		t.Run(name, func(t *testing.T) {
			st := mk(t)
			srv, fake := newPVMBGServer(t, 60)
			first, second := replicaManager(t, srv, st), replicaManager(t, srv, st)

			first.lead(withLeaseEpoch(context.Background(), 1), func(context.Context) {
				if code := pollWith(t, srv, first); code != http.StatusOK {
					t.Fatalf("first holder poll got %d", code)
				}
			})

			if first.Token() != "" || !errors.Is(first.LastError(), errNotLeader) {
				t.Fatalf("a manager that lost the lease must forget the token, got %q %v", first.Token(), first.LastError())
			}

			second.lead(withLeaseEpoch(context.Background(), 2), func(context.Context) {
				if second.Token() == "" || second.LastError() != nil {
					t.Fatalf("the successor needs the stored pair at once, got %q %v", second.Token(), second.LastError())
				}

				if err := second.rotate(); err != nil {
					t.Fatalf("the successor must rotate the inherited refresh token, got %v", err)
				}

				if code := pollWith(t, srv, second); code != http.StatusOK {
					t.Fatalf("successor poll got %d", code)
				}
			})

			fake.mu.Lock()
			calls := fake.codeCalls
			fake.mu.Unlock()

			if calls != 1 {
				t.Fatalf("the one-time code must be used once across both holders, got %d", calls)
			}

			if got, epoch, _, _ := st.load(context.Background()); epoch != 2 || got.AccessToken != "access-2" {
				t.Fatalf("the shared store must hold the successor's pair, got %+v epoch=%d", got, epoch)
			}
		})
	}
}

func TestAManagerThatIsNotLeaderNeverCallsTheEndpoint(t *testing.T) {
	t.Parallel()

	srv, fake := newPVMBGServer(t, 60)
	m := replicaManager(t, srv, &memTokenStore{})

	if m.Token() != "" || !errors.Is(m.LastError(), errNotLeader) {
		t.Fatalf("a follower has no token and says why, got %q %v", m.Token(), m.LastError())
	}

	m.Reject()
	time.Sleep(50 * time.Millisecond)

	fake.mu.Lock()
	defer fake.mu.Unlock()

	if fake.codeCalls != 0 || fake.n != 0 {
		t.Fatalf("a follower must not touch the token endpoint, got code=%d issued=%d", fake.codeCalls, fake.n)
	}
}

func TestAStaleHolderCannotReplaceTheNewerPair(t *testing.T) {
	t.Parallel()

	srv, _ := newPVMBGServer(t, 60)
	st := &memTokenStore{}
	old := replicaManager(t, srv, st)
	old.epoch = 1

	if err := st.save(context.Background(), tokenFile{AccessToken: "new", RefreshToken: "new-r", ExpiresAt: time.Now().Add(time.Minute)}, 2); err != nil {
		t.Fatal(err)
	}

	err := old.store(tokenFile{AccessToken: "old", RefreshToken: "old-r", ExpiresAt: time.Now().Add(time.Minute)}, time.Second)
	if !errors.Is(err, errStaleToken) || old.Token() != "" {
		t.Fatalf("a stale holder must be refused and not use the pair, got %v token=%q", err, old.Token())
	}

	if got, _, _, _ := st.load(context.Background()); got.AccessToken != "new" {
		t.Fatalf("the newer pair must stay, got %+v", got)
	}
}

func TestFirstHolderWithoutAStoredPairBootstrapsAndSharesIt(t *testing.T) {
	t.Parallel()

	srv, _ := newPVMBGServer(t, 60)
	st := &memTokenStore{}
	m := replicaManager(t, srv, st)

	m.lead(withLeaseEpoch(context.Background(), 1), func(context.Context) {
		if m.Token() != firstAccess {
			t.Fatalf("expected the bootstrapped token, got %q", m.Token())
		}
	})

	if got, epoch, ok, _ := st.load(context.Background()); !ok || epoch != 1 || got.RefreshToken != firstRefresh {
		t.Fatalf("the bootstrapped pair must be shared with its epoch, got %+v epoch=%d", got, epoch)
	}
}

// Store that fails to read until healed, and counts the reads.
type brokenTokenStore struct {
	memTokenStore
	mu2     sync.Mutex
	broken  bool
	readErr error
}

func (s *brokenTokenStore) load(ctx context.Context) (tokenFile, int64, bool, error) {
	s.mu2.Lock()
	broken, err := s.broken, s.readErr
	s.mu2.Unlock()

	if broken {
		return tokenFile{}, 0, false, err
	}

	return s.memTokenStore.load(ctx)
}

func (s *brokenTokenStore) heal() {
	s.mu2.Lock()
	s.broken = false
	s.mu2.Unlock()
}

func TestSharedStoreReadErrorDoesNotFallBackToTheFileOrTheCode(t *testing.T) {
	t.Parallel()

	srv, fake := newPVMBGServer(t, 60)
	st := &brokenTokenStore{broken: true, readErr: errors.New("connection refused")}
	m := replicaManager(t, srv, st)

	stale := tokenFile{AccessToken: "old-a", RefreshToken: "old-r", ExpiresAt: time.Now().Add(time.Hour)}

	b, _ := json.Marshal(stale)
	if err := os.WriteFile(m.path, b, 0o600); err != nil {
		t.Fatal(err)
	}

	m.mu.Lock()
	m.epoch = 2
	m.mu.Unlock()

	err := m.load()
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("a failing shared store must fail the load, got %v", err)
	}

	if m.Token() != "" || m.LastError() == nil {
		t.Fatalf("no token and a visible error expected, got %q %v", m.Token(), m.LastError())
	}

	fake.mu.Lock()
	calls := fake.codeCalls + fake.n
	fake.mu.Unlock()

	if calls != 0 {
		t.Fatalf("the one time code and the endpoint must stay untouched, got %d calls", calls)
	}

	good := tokenFile{AccessToken: "shared-a", RefreshToken: "shared-r", ExpiresAt: time.Now().Add(time.Hour)}
	if err := st.memTokenStore.save(context.Background(), good, 1); err != nil {
		t.Fatal(err)
	}

	st.heal()

	if err := m.load(); err != nil || m.Token() != "shared-a" {
		t.Fatalf("after the store answers the shared pair wins over the file, got %q %v", m.Token(), err)
	}
}

// Store whose saves fail while failSaves is above zero.
type flakySaveStore struct {
	memTokenStore
	fmu       sync.Mutex
	failSaves int
}

func (s *flakySaveStore) save(ctx context.Context, f tokenFile, epoch int64) error {
	s.fmu.Lock()
	fail := s.failSaves > 0
	if fail {
		s.failSaves--
	}
	s.fmu.Unlock()

	if fail {
		return errors.New("db down")
	}

	return s.memTokenStore.save(ctx, f, epoch)
}

func TestFailedSharedSaveKeepsThePairIssuedByPVMBG(t *testing.T) {
	t.Parallel()

	srv, fake := newPVMBGServer(t, 60)
	st := &flakySaveStore{}
	m := replicaManager(t, srv, st)

	m.mu.Lock()
	m.epoch = 1
	m.mu.Unlock()

	if err := m.load(); err != nil {
		t.Fatal(err)
	}

	st.fmu.Lock()
	st.failSaves = 1
	st.fmu.Unlock()

	if err := m.rotate(); err != nil {
		t.Fatalf("a failed shared save must not lose the pair PVMBG issued, got %v", err)
	}

	if m.Token() != "access-2" {
		t.Fatalf("the new pair must be current, got %q", m.Token())
	}

	if err := m.rotate(); err != nil || m.Token() != "access-3" {
		t.Fatalf("the next rotate must use the kept refresh token, got %q %v", m.Token(), err)
	}

	fake.mu.Lock()
	codes := fake.codeCalls
	fake.mu.Unlock()

	if codes != 1 {
		t.Fatalf("the one time code must stay unused after the bootstrap, got %d calls", codes)
	}

	if got, _, ok, _ := st.memTokenStore.load(context.Background()); !ok || got.AccessToken != "access-3" {
		t.Fatalf("the shared store must catch up, got %+v", got)
	}
}

func TestInvalidGrantReloadsTheSharedPairBeforeTheCode(t *testing.T) {
	t.Parallel()

	srv, fake := newPVMBGServer(t, 60)
	st := &memTokenStore{}
	old, next := replicaManager(t, srv, st), replicaManager(t, srv, st)

	for _, m := range []*tokenManager{old, next} {
		m.mu.Lock()
		m.epoch = 1
		m.mu.Unlock()
	}

	if err := old.load(); err != nil {
		t.Fatal(err)
	}

	if err := next.load(); err != nil || next.rotate() != nil {
		t.Fatal("the other holder must rotate the shared pair")
	}

	if err := old.rotate(); err != nil || old.Token() != "access-3" {
		t.Fatalf("a refresh rejected with invalid_grant must retry with the stored pair, got %q %v", old.Token(), err)
	}

	fake.mu.Lock()
	codes := fake.codeCalls
	fake.mu.Unlock()

	if codes != 1 {
		t.Fatalf("the spent one time code must not be called again, got %d calls", codes)
	}
}
