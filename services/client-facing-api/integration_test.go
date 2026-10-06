package main

// Service-to-service suite: this Client-Facing API against the real Auth Service
// and its User DB, over the real network. Run it with tests/integration/run.sh
// cfa-auth; without IT_CFA_URL every test here skips, so `go test ./...` stays offline.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// itEnv is where the suite talks to; set by run.sh.
type itEnv struct {
	url, operatorEmail, operatorPassword string
	ttl                                  time.Duration // the Auth Service's short access token TTL
}

func integrationEnv(t *testing.T) itEnv {
	t.Helper()

	url := os.Getenv("IT_CFA_URL")
	if url == "" {
		t.Skip("IT_CFA_URL not set (run tests/integration/run.sh cfa-auth)")
	}

	secs, err := strconv.Atoi(os.Getenv("IT_TOKEN_TTL_S"))
	if err != nil || secs <= 0 {
		t.Fatalf("IT_TOKEN_TTL_S must be the Auth Service's ACCESS_TOKEN_TTL_S, got %q", os.Getenv("IT_TOKEN_TTL_S"))
	}

	return itEnv{
		url:              url,
		operatorEmail:    os.Getenv("IT_OPERATOR_EMAIL"),
		operatorPassword: os.Getenv("IT_OPERATOR_PASSWORD"),
		ttl:              time.Duration(secs) * time.Second,
	}
}

// reply is one HTTP response with its cookies, body read.
type reply struct {
	code    int
	body    string
	cookies map[string]*http.Cookie
}

// cookies is a client's cookie jar kept by hand: the cookies are Secure, which a
// real jar would refuse to send over http, and the suite wants to see each one.
type cookies map[string]string

func (e itEnv) do(t *testing.T, method, path, body string, jar cookies) reply {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), method, e.url+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}

	for name, value := range jar {
		req.AddCookie(&http.Cookie{Name: name, Value: value}) //nolint:gosec // a test client replaying the cookies it was given
	}

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Errorf("%s %s: %v", method, path, err) // not Fatal: the race test calls this from goroutines

		return reply{cookies: map[string]*http.Cookie{}}
	}

	defer func() { _ = resp.Body.Close() }()

	raw, _ := io.ReadAll(resp.Body)
	r := reply{code: resp.StatusCode, body: string(raw), cookies: map[string]*http.Cookie{}}

	for _, c := range resp.Cookies() {
		r.cookies[c.Name] = c
	}

	return r
}

// keep stores the cookies a reply set into jar (and drops the ones it cleared).
func (r reply) keep(jar cookies) {
	for name, c := range r.cookies {
		if c.MaxAge < 0 {
			delete(jar, name)
		} else {
			jar[name] = c.Value
		}
	}
}

func (e itEnv) login(t *testing.T, email, password string) (reply, cookies) {
	t.Helper()

	r := e.do(t, http.MethodPost, "/auth/login", fmt.Sprintf(`{"email":%q,"password":%q}`, email, password), nil)
	jar := cookies{}
	r.keep(jar)

	return r, jar
}

func (e itEnv) operator(t *testing.T) cookies {
	t.Helper()

	r, jar := e.login(t, e.operatorEmail, e.operatorPassword)
	if r.code != http.StatusOK {
		t.Fatalf("the seeded Operator could not log in: %d %s", r.code, r.body)
	}

	return jar
}

var uniq = struct {
	sync.Mutex
	n int
}{}

// freshEmail is unique per call and per run, so reruns never hit "already registered".
func freshEmail(prefix string) string {
	uniq.Lock()
	defer uniq.Unlock()

	uniq.n++

	return fmt.Sprintf("%s-%d-%d@it.example", prefix, time.Now().UnixNano(), uniq.n)
}

var accountRPCsOnce sync.Once

// requireAccountRPCs fails the test run clearly when the Auth Service lacks the
// account RPCs (#13): the Client-Facing API turns an unimplemented RPC into a 502.
func (e itEnv) requireAccountRPCs(t *testing.T) {
	t.Helper()

	accountRPCsOnce.Do(func() {
		r := e.do(t, http.MethodPost, "/auth/register", fmt.Sprintf(`{"email":%q,"password":"pw-1234"}`, freshEmail("probe")), nil)
		if r.code != http.StatusCreated {
			t.Fatalf("POST /auth/register returned %d %s: the Auth Service seems to lack the Register/CreateAccount RPCs (#13)", r.code, r.body)
		}
	})
}

// newLapangan creates a Tim Lapangan account (its access token is the short one),
// logs it in and returns its cookies.
func (e itEnv) newLapangan(t *testing.T) cookies {
	t.Helper()

	email, password := freshEmail("lapangan"), "pw-lapangan-1" //nolint:gosec // a throwaway test account

	r := e.do(t, http.MethodPost, "/accounts", fmt.Sprintf(`{"email":%q,"password":%q,"role":"Lapangan"}`, email, password), e.operator(t))
	if r.code != http.StatusCreated {
		t.Fatalf("Operator could not create a Lapangan account: %d %s", r.code, r.body)
	}

	login, jar := e.login(t, email, password)
	if login.code != http.StatusOK {
		t.Fatalf("new Lapangan account could not log in: %d %s", login.code, login.body)
	}

	return jar
}

// expire waits until a short-lived access token is past its exp.
func (e itEnv) expire() { time.Sleep(e.ttl + 1500*time.Millisecond) }

// claimsOf reads the payload of a JWT without checking it (the suite only inspects it).
func claimsOf(t *testing.T, token string) map[string]any {
	t.Helper()

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("not a JWT: %q", token)
	}

	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}

	var c map[string]any
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}

	return c
}

// compose runs a docker compose command against the suite's project.
func compose(t *testing.T, args ...string) {
	t.Helper()

	base := strings.Fields(os.Getenv("IT_COMPOSE"))
	if len(base) == 0 {
		t.Fatal("IT_COMPOSE not set")
	}

	var stderr strings.Builder

	cmd := exec.CommandContext(context.Background(), base[0], append(base[1:], args...)...) //nolint:gosec // IT_COMPOSE is set by tests/integration/run.sh
	// compose prints warnings on stderr; only stdout is the answer.
	cmd.Stderr = &stderr

	_, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}
}

//nolint:paralleltest // shares the docker stack
func TestIntegrationLoginSetsHttpOnlyCookiesAndRejectsGenerically(t *testing.T) {
	e := integrationEnv(t)
	e.requireAccountRPCs(t)

	ok, jar := e.login(t, e.operatorEmail, e.operatorPassword)
	if ok.code != http.StatusOK {
		t.Fatalf("login: expected 200, got %d %s", ok.code, ok.body)
	}

	for _, name := range []string{"jwt", "refresh_token"} {
		c := ok.cookies[name]
		if c == nil || c.Value == "" || !c.HttpOnly || !c.Secure {
			t.Fatalf("cookie %s must be set HttpOnly and Secure, got %+v", name, c)
		}
	}

	if strings.Contains(ok.body, jar["jwt"]) || strings.Contains(ok.body, jar["refresh_token"]) {
		t.Fatalf("tokens must travel only in cookies, body was %s", ok.body)
	}

	// A wrong password and an unknown email must be indistinguishable.
	wrongPassword := e.do(t, http.MethodPost, "/auth/login", fmt.Sprintf(`{"email":%q,"password":"nope"}`, e.operatorEmail), nil)
	unknownEmail := e.do(t, http.MethodPost, "/auth/login", `{"email":"nobody@it.example","password":"nope"}`, nil)

	if wrongPassword.code != http.StatusUnauthorized || unknownEmail.code != http.StatusUnauthorized ||
		wrongPassword.body != unknownEmail.body || len(wrongPassword.cookies)+len(unknownEmail.cookies) != 0 {
		t.Fatalf("expected the same 401 and no cookies, got %d %q and %d %q", wrongPassword.code, wrongPassword.body, unknownEmail.code, unknownEmail.body)
	}
}

//nolint:paralleltest // shares the docker stack
func TestIntegrationLogoutKillsTheRefreshToken(t *testing.T) {
	e := integrationEnv(t)
	e.requireAccountRPCs(t)

	jar := e.newLapangan(t)
	stolen := cookies{"jwt": jar["jwt"], "refresh_token": jar["refresh_token"]} // what a thief would hold

	out := e.do(t, http.MethodPost, "/auth/logout", "", jar)
	if out.code != http.StatusNoContent || out.cookies["jwt"] == nil || out.cookies["jwt"].MaxAge >= 0 ||
		out.cookies["refresh_token"] == nil || out.cookies["refresh_token"].MaxAge >= 0 {
		t.Fatalf("logout: expected 204 with both cookies cleared, got %d %+v", out.code, out.cookies)
	}

	// With the session expired, only the refresh token could keep it alive, and it is gone.
	e.expire()

	if r := e.do(t, http.MethodPost, "/accounts", "{}", stolen); r.code != http.StatusUnauthorized {
		t.Fatalf("a revoked refresh token must not revive the session, got %d %s", r.code, r.body)
	}
}

//nolint:paralleltest // shares the docker stack
func TestIntegrationAuthServiceTokenPassesAndSurvivesAuthServiceDown(t *testing.T) {
	e := integrationEnv(t)
	e.requireAccountRPCs(t)

	jar := e.operator(t)

	// A token the Auth Service signed passes the Client-Facing API's own check.
	body := fmt.Sprintf(`{"email":%q,"password":"pw-1234","role":"Media"}`, freshEmail("up"))
	if r := e.do(t, http.MethodPost, "/accounts", body, jar); r.code != http.StatusCreated {
		t.Fatalf("a freshly issued Operator token must pass the check, got %d %s", r.code, r.body)
	}

	compose(t, "stop", "auth-service")
	t.Cleanup(func() {
		compose(t, "start", "auth-service")
		waitForLogin(t, e)
	})

	// With the Auth Service gone the token is still verified locally: the request
	// gets past the middleware and only the account RPC behind it fails.
	body = fmt.Sprintf(`{"email":%q,"password":"pw-1234","role":"Media"}`, freshEmail("down"))

	r := e.do(t, http.MethodPost, "/accounts", body, jar)
	if r.code == http.StatusUnauthorized || (r.code != http.StatusBadGateway && r.code != http.StatusGatewayTimeout) {
		t.Fatalf("expected the valid token to pass and the RPC to fail with 502/504, got %d %s", r.code, r.body)
	}
}

// waitForLogin waits until the Auth Service answers logins again after a restart.
func waitForLogin(t *testing.T, e itEnv) {
	t.Helper()

	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, e.url+"/auth/login",
			bytes.NewBufferString(fmt.Sprintf(`{"email":%q,"password":%q}`, e.operatorEmail, e.operatorPassword)))
		if err != nil {
			t.Fatal(err)
		}

		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				return
			}
		}
	}

	t.Fatal("the Auth Service did not come back")
}

//nolint:paralleltest // shares the docker stack
func TestIntegrationExpiredSessionRefreshesTransparently(t *testing.T) {
	e := integrationEnv(t)
	e.requireAccountRPCs(t)

	jar := e.newLapangan(t)
	oldJWT, oldRefresh := jar["jwt"], jar["refresh_token"]

	e.expire()

	// A Lapangan session is authenticated but not allowed to create accounts: a 403
	// (not a 401) proves the expired token was refreshed behind the request.
	r := e.do(t, http.MethodPost, "/accounts", "{}", jar)
	if r.code != http.StatusForbidden {
		t.Fatalf("expected the refreshed session to reach the handler (403), got %d %s", r.code, r.body)
	}

	r.keep(jar)

	if jar["jwt"] == oldJWT || jar["refresh_token"] == oldRefresh || jar["jwt"] == "" {
		t.Fatalf("the response must carry a new jwt and a rotated refresh token")
	}

	if got := claimsOf(t, jar["jwt"])["role"]; got != "Lapangan" {
		t.Fatalf("the refreshed token must keep the role, got %v", got)
	}

	// The rotated refresh token is the only one that works now.
	if r := e.do(t, http.MethodPost, "/accounts", "{}", jar); r.code != http.StatusForbidden || len(r.cookies) != 0 {
		t.Fatalf("the new session must work without another refresh, got %d with %d cookies", r.code, len(r.cookies))
	}
}

//nolint:paralleltest // shares the docker stack
func TestIntegrationLostRefreshRaceIsACleanUnauthorized(t *testing.T) {
	e := integrationEnv(t)
	e.requireAccountRPCs(t)

	jar := e.newLapangan(t)

	e.expire()

	// Two requests carry the same expired session. The Auth Service rotates a
	// refresh token once, so one wins and the other must lose cleanly.
	replies := make([]reply, 2)

	var wg sync.WaitGroup

	for i := range replies {
		wg.Go(func() { replies[i] = e.do(t, http.MethodPost, "/accounts", "{}", jar) })
	}

	wg.Wait()

	won, lost := replies[0], replies[1]
	if won.code == http.StatusUnauthorized {
		won, lost = lost, won
	}

	if won.code != http.StatusForbidden || lost.code != http.StatusUnauthorized {
		t.Fatalf("expected one request through (403) and one clean 401, got %d and %d", replies[0].code, replies[1].code)
	}

	if lost.cookies["jwt"] == nil || lost.cookies["jwt"].MaxAge >= 0 || lost.cookies["refresh_token"] == nil || lost.cookies["refresh_token"].MaxAge >= 0 {
		t.Fatalf("the loser must be told to clear both cookies, got %+v", lost.cookies)
	}
}

//nolint:paralleltest // shares the docker stack
func TestIntegrationRegisterAlwaysGivesMedia(t *testing.T) {
	e := integrationEnv(t)
	e.requireAccountRPCs(t)

	email := freshEmail("self")

	// Asking for a higher role in the body changes nothing.
	r := e.do(t, http.MethodPost, "/auth/register", fmt.Sprintf(`{"email":%q,"password":"pw-1234","role":"Operator"}`, email), nil)
	if r.code != http.StatusCreated {
		t.Fatalf("register: expected 201, got %d %s", r.code, r.body)
	}

	login, jar := e.login(t, email, "pw-1234")
	if login.code != http.StatusOK {
		t.Fatalf("the new account could not log in: %d %s", login.code, login.body)
	}

	if got := claimsOf(t, jar["jwt"])["role"]; got != "Media" {
		t.Fatalf("a self-registered account must be Media, got %v", got)
	}

	if dup := e.do(t, http.MethodPost, "/auth/register", fmt.Sprintf(`{"email":%q,"password":"pw-1234"}`, email), nil); dup.code != http.StatusConflict {
		t.Fatalf("registering the same email twice: expected 409, got %d", dup.code)
	}
}

//nolint:paralleltest // shares the docker stack
func TestIntegrationAccountsOnlyWorkForAnOperator(t *testing.T) {
	e := integrationEnv(t)
	e.requireAccountRPCs(t)

	newAccount := func() string {
		return fmt.Sprintf(`{"email":%q,"password":"pw-1234","role":"Lapangan"}`, freshEmail("made"))
	}

	if r := e.do(t, http.MethodPost, "/accounts", newAccount(), nil); r.code != http.StatusUnauthorized {
		t.Fatalf("no session: expected 401, got %d", r.code)
	}

	email := freshEmail("media")
	if r := e.do(t, http.MethodPost, "/auth/register", fmt.Sprintf(`{"email":%q,"password":"pw-1234"}`, email), nil); r.code != http.StatusCreated {
		t.Fatalf("register: %d %s", r.code, r.body)
	}

	_, media := e.login(t, email, "pw-1234")
	if r := e.do(t, http.MethodPost, "/accounts", newAccount(), media); r.code != http.StatusForbidden {
		t.Fatalf("a Media session must not create accounts, expected 403, got %d", r.code)
	}

	if r := e.do(t, http.MethodPost, "/accounts", newAccount(), e.operator(t)); r.code != http.StatusCreated {
		t.Fatalf("an Operator creates accounts, expected 201, got %d %s", r.code, r.body)
	}
}
