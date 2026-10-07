package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/authpb"
)

const newUserBody = `{"email":"new@x.example","password":"pw"}`

// Posts to the real mux, with the JWT middleware and transparent refresh on.
func call(f fakeAuth, path, body string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	api := &authAPI{auth: f, timeout: time.Second, secure: true}
	api.verifier = newVerifier().withRefresh(api)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, strings.NewReader(body))
	for _, c := range cookies {
		req.AddCookie(c)
	}

	rec := httptest.NewRecorder()
	newMux(api).ServeHTTP(rec, req)

	return rec
}

func jwtCookie(t *testing.T, role string) *http.Cookie {
	t.Helper()

	return sessionCookie(cookieJWT, tokenFor(t, testSecret, role, "caller@x.example", testNow.Add(time.Minute)))
}

// Fails the test if either account RPC is called.
func noCreates(t *testing.T) fakeAuth {
	t.Helper()

	return fakeAuth{
		register: func(*authpb.RegisterRequest) error {
			t.Error("Register must not have been called")

			return nil
		},
		createAccount: func(*authpb.CreateAccountRequest) error {
			t.Error("CreateAccount must not have been called")

			return nil
		},
	}
}

func TestSelfRegistrationNeedsNoAuthAndNeverEscalates(t *testing.T) {
	t.Parallel()

	var got *authpb.RegisterRequest

	f := noCreates(t)
	f.register = func(in *authpb.RegisterRequest) error {
		got = in

		return nil
	}

	rec := call(f, "/auth/register", `{"email":"new@x.example","password":"pw","role":"Operator"}`)

	if rec.Code != http.StatusCreated || got.GetEmail() != "new@x.example" || got.GetPassword() != "pw" {
		t.Fatalf("expected 201 and the credentials forwarded, got %d %v", rec.Code, got)
	}
}

func TestSelfRegistrationErrors(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		err  error
		want int
	}{
		{status.Error(codes.AlreadyExists, "taken"), http.StatusConflict},
		{status.Error(codes.InvalidArgument, "bad"), http.StatusBadRequest},
		{status.Error(codes.Unavailable, "down"), http.StatusBadGateway},
		{status.Error(codes.DeadlineExceeded, "slow"), http.StatusGatewayTimeout},
	} {
		f := fakeAuth{register: func(*authpb.RegisterRequest) error { return c.err }}

		if rec := call(f, "/auth/register", newUserBody); rec.Code != c.want {
			t.Fatalf("%v: expected %d, got %d", c.err, c.want, rec.Code)
		}
	}

	for _, body := range []string{"", "nope", `{"email":`} {
		if rec := call(noCreates(t), "/auth/register", body); rec.Code != http.StatusBadRequest {
			t.Fatalf("body %q: expected 400, got %d", body, rec.Code)
		}
	}
}

func TestOnlyAnOperatorMayCreateAccounts(t *testing.T) {
	t.Parallel()

	body := `{"email":"x@x.example","password":"pw","role":"Operator"}`

	forged := sessionCookie(cookieJWT, tokenFor(t, "another-secret", roleOperator, "evil@x.example", testNow.Add(time.Minute)))

	for name, c := range map[string]struct {
		cookies []*http.Cookie
		want    int
	}{
		"no cookie":      {nil, http.StatusUnauthorized},
		"forged jwt":     {[]*http.Cookie{forged}, http.StatusUnauthorized},
		"media caller":   {[]*http.Cookie{jwtCookie(t, roleMedia)}, http.StatusForbidden},
		"lapangan":       {[]*http.Cookie{jwtCookie(t, roleLapangan)}, http.StatusForbidden},
		"only a refresh": {[]*http.Cookie{sessionCookie(cookieRefresh, "r")}, http.StatusUnauthorized},
	} {
		if rec := call(noCreates(t), "/accounts", body, c.cookies...); rec.Code != c.want {
			t.Fatalf("%s: expected %d, got %d", name, c.want, rec.Code)
		}
	}
}

func TestOperatorCanCreateEveryRole(t *testing.T) {
	t.Parallel()

	for _, role := range []string{roleMedia, roleLapangan, roleOperator} {
		var got *authpb.CreateAccountRequest

		f := noCreates(t)
		f.createAccount = func(in *authpb.CreateAccountRequest) error {
			got = in

			return nil
		}

		rec := call(f, "/accounts", `{"email":"x@x.example","password":"pw","role":"`+role+`"}`, jwtCookie(t, roleOperator))

		if rec.Code != http.StatusCreated || got.GetRole() != role || got.GetEmail() != "x@x.example" || got.GetPassword() != "pw" {
			t.Fatalf("role %s: expected 201 with the request forwarded, got %d %v", role, rec.Code, got)
		}
	}
}

func TestAccountCreationErrors(t *testing.T) {
	t.Parallel()

	op := jwtCookie(t, roleOperator)

	for _, c := range []struct {
		err  error
		want int
	}{
		{status.Error(codes.AlreadyExists, "taken"), http.StatusConflict},
		{status.Error(codes.InvalidArgument, "bad role"), http.StatusBadRequest},
		{status.Error(codes.Unavailable, "down"), http.StatusBadGateway},
	} {
		f := fakeAuth{createAccount: func(*authpb.CreateAccountRequest) error { return c.err }}

		if rec := call(f, "/accounts", `{"email":"x@x.example","password":"pw","role":"Media"}`, op); rec.Code != c.want {
			t.Fatalf("%v: expected %d, got %d", c.err, c.want, rec.Code)
		}
	}

	if rec := call(noCreates(t), "/accounts", "nope", op); rec.Code != http.StatusBadRequest {
		t.Fatalf("a bad body: expected 400, got %d", rec.Code)
	}
}

// An Operator whose JWT expired mid-session is refreshed transparently and the account is still created, in the same round trip.
func TestExpiredOperatorSessionRefreshesThenCreates(t *testing.T) {
	t.Parallel()

	created := false
	fresh := tokenFor(t, testSecret, roleOperator, "op@x.example", testNow.Add(time.Minute))

	f := fakeAuth{
		refresh:       refreshReturning(fresh, newRefresh, nil),
		createAccount: func(*authpb.CreateAccountRequest) error { created = true; return nil },
	}

	expired := sessionCookie(cookieJWT, tokenFor(t, testSecret, roleOperator, "op@x.example", testNow.Add(-time.Second)))
	rec := call(f, "/accounts", `{"email":"x@x.example","password":"pw","role":"Lapangan"}`, expired, sessionCookie(cookieRefresh, oldRefresh))

	if rec.Code != http.StatusCreated || !created || cookie(rec, cookieJWT) == nil || cookie(rec, cookieJWT).Value != fresh {
		t.Fatalf("expected the session refreshed and the account created, got %d created=%v cookies=%v", rec.Code, created, rec.Result().Cookies())
	}
}
