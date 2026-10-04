package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/authpb"
)

const (
	testJWT     = "the.jwt"
	testRefresh = "the-refresh"
)

// sessionCookie is a request cookie as a browser would send it.
func sessionCookie(name, value string) *http.Cookie {
	return &http.Cookie{Name: name, Value: value, Path: "/", HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode}
}

// fakeAuth stands in for the Auth Service; the embedded interface makes any RPC
// a test doesn't set up panic if it is called.
type fakeAuth struct {
	authpb.AuthClient

	login  func(*authpb.LoginRequest) (*authpb.TokenResponse, error)
	logout func(*authpb.LogoutRequest) error
}

func (f fakeAuth) Login(_ context.Context, in *authpb.LoginRequest, _ ...grpc.CallOption) (*authpb.TokenResponse, error) {
	return f.login(in)
}

func (f fakeAuth) Logout(_ context.Context, in *authpb.LogoutRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, f.logout(in)
}

func send(f fakeAuth, path, body string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, strings.NewReader(body))
	for _, c := range cookies {
		req.AddCookie(c)
	}

	rec := httptest.NewRecorder()
	newMux(&authAPI{auth: f, timeout: time.Second, secure: true}).ServeHTTP(rec, req)

	return rec
}

func cookie(rec *httptest.ResponseRecorder, name string) *http.Cookie {
	for _, c := range rec.Result().Cookies() {
		if c.Name == name {
			return c
		}
	}

	return nil
}

// wantSessionCookie checks a cookie is set as an HttpOnly, Secure session cookie at /.
func wantSessionCookie(t *testing.T, rec *httptest.ResponseRecorder, name, value string) {
	t.Helper()

	c := cookie(rec, name)
	if c == nil || c.Value != value || !c.HttpOnly || !c.Secure || c.Path != "/" || c.MaxAge != 0 {
		t.Fatalf("cookie %s: want HttpOnly Secure session cookie %q at /, got %+v", name, value, c)
	}
}

func TestLoginSetsBothCookiesHttpOnly(t *testing.T) {
	t.Parallel()

	var got *authpb.LoginRequest

	f := fakeAuth{login: func(in *authpb.LoginRequest) (*authpb.TokenResponse, error) {
		got = in

		return &authpb.TokenResponse{Jwt: testJWT, RefreshToken: testRefresh, ExpiresIn: 60}, nil
	}}

	rec := send(f, "/auth/login", `{"email":"a@x.example","password":"pw"}`)

	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"expires_in":60`) {
		t.Fatalf("expected 200 with expires_in, got %d %s", rec.Code, rec.Body.String())
	}

	if got.GetEmail() != "a@x.example" || got.GetPassword() != "pw" {
		t.Fatalf("credentials not forwarded: %v", got)
	}

	wantSessionCookie(t, rec, cookieJWT, testJWT)
	wantSessionCookie(t, rec, cookieRefresh, testRefresh)

	if body := rec.Body.String(); strings.Contains(body, testJWT) || strings.Contains(body, testRefresh) {
		t.Fatalf("tokens must travel only in cookies, not the body: %s", body)
	}
}

func TestLoginRejectionIsGenericAndSetsNoCookies(t *testing.T) {
	t.Parallel()

	f := fakeAuth{login: func(*authpb.LoginRequest) (*authpb.TokenResponse, error) {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}}

	rec := send(f, "/auth/login", `{"email":"a@x.example","password":"bad"}`)

	if rec.Code != http.StatusUnauthorized || len(rec.Result().Cookies()) != 0 {
		t.Fatalf("expected 401 and no cookies, got %d %v", rec.Code, rec.Result().Cookies())
	}
}

func TestLoginWhenAuthServiceIsDownIsNot401(t *testing.T) {
	t.Parallel()

	for err, want := range map[error]int{
		status.Error(codes.Unavailable, "down"):          http.StatusBadGateway,
		status.Error(codes.DeadlineExceeded, "too slow"): http.StatusGatewayTimeout,
		context.DeadlineExceeded:                         http.StatusGatewayTimeout,
	} {
		f := fakeAuth{login: func(*authpb.LoginRequest) (*authpb.TokenResponse, error) { return nil, err }}

		rec := send(f, "/auth/login", `{"email":"a@x.example","password":"pw"}`)
		if rec.Code != want || len(rec.Result().Cookies()) != 0 {
			t.Fatalf("%v: expected %d and no cookies, got %d", err, want, rec.Code)
		}
	}
}

func TestLoginRejectsBadBodies(t *testing.T) {
	t.Parallel()

	f := fakeAuth{login: func(*authpb.LoginRequest) (*authpb.TokenResponse, error) {
		t.Error("the Auth Service must not be called for a bad body")

		return nil, nil
	}}

	for _, body := range []string{"", "not json", `{"email":`, `{"email":"` + strings.Repeat("a", maxAuthBody) + `"}`} {
		if rec := send(f, "/auth/login", body); rec.Code != http.StatusBadRequest {
			t.Fatalf("body %.20q: expected 400, got %d", body, rec.Code)
		}
	}
}

func TestLogoutRevokesTheRefreshTokenAndClearsBothCookies(t *testing.T) {
	t.Parallel()

	var revoked []string

	f := fakeAuth{logout: func(in *authpb.LogoutRequest) error {
		revoked = append(revoked, in.GetRefreshToken())

		return nil
	}}

	rec := send(f, "/auth/logout", "",
		sessionCookie(cookieJWT, testJWT), sessionCookie(cookieRefresh, testRefresh))

	if rec.Code != http.StatusNoContent || len(revoked) != 1 || revoked[0] != testRefresh {
		t.Fatalf("expected 204 and one revoke of the cookie's token, got %d %v", rec.Code, revoked)
	}

	for _, name := range []string{cookieJWT, cookieRefresh} {
		if c := cookie(rec, name); c == nil || c.MaxAge >= 0 || c.Value != "" || !c.HttpOnly {
			t.Fatalf("cookie %s must be cleared, got %+v", name, c)
		}
	}
}

func TestLogoutWithoutACookieStillClearsAndSkipsTheRPC(t *testing.T) {
	t.Parallel()

	f := fakeAuth{logout: func(*authpb.LogoutRequest) error {
		t.Error("nothing to revoke, so the Auth Service must not be called")

		return nil
	}}

	rec := send(f, "/auth/logout", "")
	if rec.Code != http.StatusNoContent || cookie(rec, cookieJWT) == nil || cookie(rec, cookieRefresh) == nil {
		t.Fatalf("expected 204 with both cookies cleared, got %d %v", rec.Code, rec.Result().Cookies())
	}
}

func TestLogoutFailureIsReportedButCookiesAreStillCleared(t *testing.T) {
	t.Parallel()

	f := fakeAuth{logout: func(*authpb.LogoutRequest) error { return status.Error(codes.Unavailable, "down") }}

	rec := send(f, "/auth/logout", "", sessionCookie(cookieRefresh, testRefresh))
	if rec.Code != http.StatusBadGateway || cookie(rec, cookieRefresh) == nil || cookie(rec, cookieRefresh).MaxAge >= 0 {
		t.Fatalf("expected 502 with the cookie still cleared, got %d %v", rec.Code, rec.Result().Cookies())
	}
}

func TestAuthRoutesOnlyAcceptPost(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/auth/login", nil)
	rec := httptest.NewRecorder()
	newMux(&authAPI{auth: fakeAuth{}, timeout: time.Second, secure: true}).ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}
