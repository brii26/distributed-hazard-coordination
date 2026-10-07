package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/authpb"
)

const (
	oldRefresh = "old-refresh"
	newRefresh = "new-refresh"
	newEmail   = "new@x.example"
)

// What the protected handler saw of a request that reached it.
type seen struct {
	reached      bool
	claims       jwtClaims
	jwt, refresh string
}

func tokenFor(t *testing.T, secret, role, email string, exp time.Time) string {
	t.Helper()

	c := claimsExp(role, exp)
	c.Email = email

	return sign(t, secret, hs256, c)
}

func expiredToken(t *testing.T) string {
	t.Helper()

	return tokenFor(t, testSecret, roleLapangan, "old@x.example", testNow.Add(-time.Second))
}

func freshToken(t *testing.T) string {
	t.Helper()

	return tokenFor(t, testSecret, roleLapangan, newEmail, testNow.Add(time.Minute))
}

// Sends one request through the verifier with transparent refresh on.
func run(t *testing.T, f fakeAuth, cookies ...*http.Cookie) (*httptest.ResponseRecorder, *seen) {
	t.Helper()

	return runOn(t, &authAPI{auth: f, timeout: time.Second, secure: true}, cookies...)
}

// Same, on a given API, so requests share its refresh state like they do in the server.
func runOn(t *testing.T, api *authAPI, cookies ...*http.Cookie) (*httptest.ResponseRecorder, *seen) {
	t.Helper()

	s := &seen{}

	protected := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.reached = true
		s.claims, _ = claimsFrom(r.Context())

		if c, err := r.Cookie(cookieJWT); err == nil {
			s.jwt = c.Value
		}

		if c, err := r.Cookie(cookieRefresh); err == nil {
			s.refresh = c.Value
		}

		_, _ = w.Write([]byte("the protected response"))
	})

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/protected", nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}

	rec := httptest.NewRecorder()
	newVerifier().withRefresh(api).require(protected).ServeHTTP(rec, req)

	return rec, s
}

func refreshReturning(jwt, refresh string, err error) func(*authpb.RefreshRequest) (*authpb.TokenResponse, error) {
	return func(*authpb.RefreshRequest) (*authpb.TokenResponse, error) {
		if err != nil {
			return nil, err
		}

		return &authpb.TokenResponse{Jwt: jwt, RefreshToken: refresh, ExpiresIn: 60}, nil
	}
}

func TestExpiredSessionIsRefreshedAndTheRequestForwarded(t *testing.T) {
	t.Parallel()

	fresh := freshToken(t)

	var exchanged string

	f := fakeAuth{refresh: func(in *authpb.RefreshRequest) (*authpb.TokenResponse, error) {
		exchanged = in.GetRefreshToken()

		return refreshReturning(fresh, newRefresh, nil)(in)
	}}

	rec, s := run(t, f, sessionCookie(cookieJWT, expiredToken(t)), sessionCookie(cookieRefresh, oldRefresh))

	if exchanged != oldRefresh {
		t.Fatalf("the cookie's refresh token must be exchanged, got %q", exchanged)
	}

	if rec.Code != http.StatusOK || rec.Body.String() != "the protected response" || !s.reached {
		t.Fatalf("expected the protected response forwarded, got %d %q reached=%v", rec.Code, rec.Body.String(), s.reached)
	}

	if s.jwt != fresh || s.refresh != newRefresh || s.claims.Email != newEmail || s.claims.Role != roleLapangan {
		t.Fatalf("the forwarded request must carry the new tokens and claims, got %+v", s)
	}

	wantSessionCookie(t, rec, cookieJWT, fresh)
	wantSessionCookie(t, rec, cookieRefresh, newRefresh)
}

func TestRefreshedSessionKeepsWorkingWithoutAnotherRefresh(t *testing.T) {
	t.Parallel()

	fresh := freshToken(t)

	var calls int

	f := fakeAuth{refresh: func(in *authpb.RefreshRequest) (*authpb.TokenResponse, error) {
		calls++

		return refreshReturning(fresh, newRefresh, nil)(in)
	}}

	rec, _ := run(t, f, sessionCookie(cookieJWT, expiredToken(t)), sessionCookie(cookieRefresh, oldRefresh))

	next, s := run(t, f, sessionCookie(cookieJWT, cookie(rec, cookieJWT).Value), sessionCookie(cookieRefresh, cookie(rec, cookieRefresh).Value))

	if next.Code != http.StatusOK || !s.reached || calls != 1 {
		t.Fatalf("the new session must work with no second refresh, got %d reached=%v calls=%d", next.Code, s.reached, calls)
	}
}

func TestUnknownRefreshTokenIs401AndClearsBothCookies(t *testing.T) {
	t.Parallel()

	f := fakeAuth{refresh: refreshReturning("", "", status.Error(codes.Unauthenticated, "invalid refresh token"))}

	rec, s := run(t, f, sessionCookie(cookieJWT, expiredToken(t)), sessionCookie(cookieRefresh, oldRefresh))

	if rec.Code != http.StatusUnauthorized || s.reached {
		t.Fatalf("expected 401 without reaching the handler, got %d reached=%v", rec.Code, s.reached)
	}

	for _, name := range []string{cookieJWT, cookieRefresh} {
		if c := cookie(rec, name); c == nil || c.MaxAge >= 0 || c.Value != "" {
			t.Fatalf("cookie %s must be cleared, got %+v", name, c)
		}
	}
}

func TestAuthServiceTroubleDuringRefreshKeepsTheCookies(t *testing.T) {
	t.Parallel()

	for err, want := range map[error]int{
		status.Error(codes.Unavailable, "down"):          http.StatusBadGateway,
		status.Error(codes.DeadlineExceeded, "too slow"): http.StatusGatewayTimeout,
		context.DeadlineExceeded:                         http.StatusGatewayTimeout,
	} {
		rec, s := run(t, fakeAuth{refresh: refreshReturning("", "", err)},
			sessionCookie(cookieJWT, expiredToken(t)), sessionCookie(cookieRefresh, oldRefresh))

		if rec.Code != want || s.reached || len(rec.Result().Cookies()) != 0 {
			t.Fatalf("%v: expected %d, cookies untouched, got %d cookies=%v", err, want, rec.Code, rec.Result().Cookies())
		}
	}
}

func TestValidTokenNeverTouchesTheAuthService(t *testing.T) {
	t.Parallel()

	f := fakeAuth{refresh: func(*authpb.RefreshRequest) (*authpb.TokenResponse, error) {
		t.Error("a valid jwt must not trigger a refresh")

		return nil, status.Error(codes.Internal, "unexpected")
	}}

	rec, s := run(t, f, sessionCookie(cookieJWT, freshToken(t)), sessionCookie(cookieRefresh, oldRefresh))

	if rec.Code != http.StatusOK || !s.reached || len(rec.Result().Cookies()) != 0 {
		t.Fatalf("expected a plain pass-through, got %d reached=%v cookies=%v", rec.Code, s.reached, rec.Result().Cookies())
	}
}

func TestForgedExpiredTokenNeverTriggersARefresh(t *testing.T) {
	t.Parallel()

	f := fakeAuth{refresh: func(*authpb.RefreshRequest) (*authpb.TokenResponse, error) {
		t.Error("a token with a bad signature must not be refreshed")

		return nil, status.Error(codes.Internal, "unexpected")
	}}

	forged := tokenFor(t, "another-secret", roleOperator, "evil@x.example", testNow.Add(-time.Second))

	rec, s := run(t, f, sessionCookie(cookieJWT, forged), sessionCookie(cookieRefresh, oldRefresh))
	if rec.Code != http.StatusUnauthorized || s.reached {
		t.Fatalf("expected 401, got %d reached=%v", rec.Code, s.reached)
	}
}

func TestNewTokenThatDoesNotVerifyIsNotForwarded(t *testing.T) {
	t.Parallel()

	bad := tokenFor(t, "another-secret", roleLapangan, newEmail, testNow.Add(time.Minute))

	rec, s := run(t, fakeAuth{refresh: refreshReturning(bad, newRefresh, nil)},
		sessionCookie(cookieJWT, expiredToken(t)), sessionCookie(cookieRefresh, oldRefresh))

	if rec.Code != http.StatusBadGateway || s.reached || len(rec.Result().Cookies()) != 0 {
		t.Fatalf("expected 502 with no cookies set, got %d reached=%v cookies=%v", rec.Code, s.reached, rec.Result().Cookies())
	}
}

// Twenty requests sharing one expired session refresh it together. Auth sees one exchange, every request gets the same new pair and is forwarded, and nothing corrupts state (-race).
func TestConcurrentRefreshesOfOneTokenAllSucceedWithOneExchange(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	fresh := freshToken(t)
	f := fakeAuth{refresh: func(in *authpb.RefreshRequest) (*authpb.TokenResponse, error) {
		if calls.Add(1) > 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid refresh token")
		}

		time.Sleep(50 * time.Millisecond) // long enough for the others to arrive mid-exchange

		return refreshReturning(fresh, newRefresh, nil)(in)
	}}
	api := &authAPI{auth: f, timeout: time.Second, secure: true}

	const n = 20

	var wg sync.WaitGroup

	codesSeen := make(chan int, n)

	for range n {
		wg.Go(func() {
			rec, _ := runOn(t, api, sessionCookie(cookieJWT, expiredToken(t)), sessionCookie(cookieRefresh, oldRefresh))
			codesSeen <- rec.Code
		})
	}

	wg.Wait()
	close(codesSeen)

	for c := range codesSeen {
		if c != http.StatusOK {
			t.Fatalf("a request sharing the session got %d, want 200", c)
		}
	}

	// A request that arrives after the exchange, still with the old cookies, gets the same pair.
	if rec, _ := runOn(t, api, sessionCookie(cookieJWT, expiredToken(t)), sessionCookie(cookieRefresh, oldRefresh)); rec.Code != http.StatusOK {
		t.Fatalf("late request got %d, want 200", rec.Code)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("Auth Service saw %d refreshes, want 1", got)
	}
}
