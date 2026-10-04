package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/authpb"
)

const (
	testSecret = "jwt-secret"
	hs256      = `{"alg":"HS256","typ":"JWT"}`
)

var testNow = time.Unix(1_700_000_000, 0)

// sign builds header.payload.signature the way the Auth Service does.
func sign(t *testing.T, secret, header string, c jwtClaims) string {
	t.Helper()

	payload, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}

	signing := b64.EncodeToString([]byte(header)) + "." + b64.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signing))

	return signing + "." + b64.EncodeToString(mac.Sum(nil))
}

func claimsExp(role string, exp time.Time) jwtClaims {
	return jwtClaims{Name: "N", Email: "a@x.example", Role: role, Iat: testNow.Add(-time.Minute).Unix(), Exp: exp.Unix()}
}

func validToken(t *testing.T, role string) string {
	t.Helper()

	return sign(t, testSecret, hs256, claimsExp(role, testNow.Add(time.Minute)))
}

func newVerifier() *jwtVerifier {
	v := newJWTVerifier([]byte(testSecret))
	v.now = func() time.Time { return testNow }

	return v
}

// get sends a GET to a protected route with the given cookies; reached reports
// whether the protected handler ran, and seen is the claims it saw.
func get(v *jwtVerifier, cookies ...*http.Cookie) (rec *httptest.ResponseRecorder, reached bool, seen jwtClaims) {
	protected := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		seen, _ = claimsFrom(r.Context())

		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/protected", nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}

	rec = httptest.NewRecorder()
	v.require(protected).ServeHTTP(rec, req)

	return rec, reached, seen
}

func TestValidTokenReachesTheHandlerWithItsClaims(t *testing.T) {
	t.Parallel()

	for _, role := range []string{roleMedia, roleLapangan, roleOperator} {
		rec, reached, seen := get(newVerifier(), sessionCookie(cookieJWT, validToken(t, role)))

		if rec.Code != http.StatusOK || !reached || seen.Role != role || seen.Email != "a@x.example" || seen.Name != "N" {
			t.Fatalf("role %s: expected 200 with its claims, got %d reached=%v claims=%+v", role, rec.Code, reached, seen)
		}
	}
}

func TestValidTokenWorksWhileTheAuthServiceIsDown(t *testing.T) {
	t.Parallel()

	down := func(string) error {
		t.Error("the Auth Service must not be called to verify a token")

		return status.Error(codes.Unavailable, "down")
	}
	api := &authAPI{timeout: time.Second, secure: true, auth: fakeAuth{
		login:  func(*authpb.LoginRequest) (*authpb.TokenResponse, error) { return nil, down("login") },
		logout: func(*authpb.LogoutRequest) error { return down("logout") },
	}}

	mux := newMux(api)
	mux.Handle("GET /protected", newVerifier().require(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/protected", nil)
	req.AddCookie(sessionCookie(cookieJWT, validToken(t, roleOperator)))

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("a valid token must pass with the Auth Service down, got %d", rec.Code)
	}
}

func TestTamperedOrForgedTokensAreRejectedWhateverTheirExp(t *testing.T) {
	t.Parallel()

	good := validToken(t, roleMedia)
	parts := strings.Split(good, ".")

	elevated, err := json.Marshal(claimsExp(roleOperator, testNow.Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}

	expiredLongAgo := claimsExp(roleMedia, testNow.Add(-24*time.Hour))

	cases := map[string]string{
		"payload swapped, old signature kept": parts[0] + "." + b64.EncodeToString(elevated) + "." + parts[2],
		"signature stripped":                  parts[0] + "." + parts[1] + ".",
		"wrong secret":                        sign(t, "another-secret", hs256, claimsExp(roleMedia, testNow.Add(time.Minute))),
		"wrong secret and expired":            sign(t, "another-secret", hs256, expiredLongAgo),
		"alg none":                            b64.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + parts[1] + ".",
		"another algorithm":                   sign(t, testSecret, `{"alg":"HS512","typ":"JWT"}`, claimsExp(roleMedia, testNow.Add(time.Minute))),
		"no exp":                              sign(t, testSecret, hs256, claimsExp(roleMedia, time.Unix(0, 0))),
		"unknown role":                        sign(t, testSecret, hs256, claimsExp("Admin", testNow.Add(time.Minute))),
		"not a jwt":                           "garbage",
		"two parts":                           parts[0] + "." + parts[1],
		"not base64":                          "!!!.???.***",
	}

	for name, token := range cases {
		v := newVerifier()
		v.refresh = func(http.ResponseWriter, *http.Request, http.Handler) {
			t.Errorf("%s: a bad token must never be handed to refresh", name)
		}

		rec, reached, _ := get(v, sessionCookie(cookieJWT, token), sessionCookie(cookieRefresh, "r"))
		if rec.Code != http.StatusUnauthorized || reached {
			t.Fatalf("%s: expected 401 without reaching the handler, got %d reached=%v", name, rec.Code, reached)
		}
	}
}

func TestNoTokenIs401(t *testing.T) {
	t.Parallel()

	for name, cookies := range map[string][]*http.Cookie{
		"no cookies":        nil,
		"empty jwt cookie":  {sessionCookie(cookieJWT, "")},
		"only the refresh":  {sessionCookie(cookieRefresh, "r")},
		"jwt in wrong name": {sessionCookie("token", validToken(t, roleMedia))},
	} {
		if rec, reached, _ := get(newVerifier(), cookies...); rec.Code != http.StatusUnauthorized || reached {
			t.Fatalf("%s: expected 401, got %d reached=%v", name, rec.Code, reached)
		}
	}
}

func TestExpiredButValidlySignedTokenGoesToRefreshWhenARefreshCookieExists(t *testing.T) {
	t.Parallel()

	expired := sign(t, testSecret, hs256, claimsExp(roleLapangan, testNow.Add(-time.Second)))

	var handedOff bool

	v := newVerifier()
	v.refresh = func(w http.ResponseWriter, _ *http.Request, _ http.Handler) {
		handedOff = true

		w.WriteHeader(http.StatusTeapot) // stands in for the #26 flow's response
	}

	rec, reached, _ := get(v, sessionCookie(cookieJWT, expired), sessionCookie(cookieRefresh, "r"))
	if !handedOff || rec.Code != http.StatusTeapot || reached {
		t.Fatalf("expected a handoff to refresh, got %d handedOff=%v reached=%v", rec.Code, handedOff, reached)
	}

	// Without a refresh cookie, or before the refresh flow exists, it is a plain 401.
	if rec, reached, _ := get(v, sessionCookie(cookieJWT, expired)); rec.Code != http.StatusUnauthorized || reached {
		t.Fatalf("expired without a refresh cookie: expected 401, got %d", rec.Code)
	}

	v.refresh = nil

	if rec, _, _ := get(v, sessionCookie(cookieJWT, expired), sessionCookie(cookieRefresh, "r")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("expired with no refresh flow: expected 401, got %d", rec.Code)
	}
}

func TestExpiryIsExclusive(t *testing.T) {
	t.Parallel()

	v := newVerifier()

	if _, err := v.verify(sign(t, testSecret, hs256, claimsExp(roleMedia, testNow))); !errors.Is(err, errExpiredToken) {
		t.Fatalf("a token whose exp is now must be expired, got %v", err)
	}

	if _, err := v.verify(sign(t, testSecret, hs256, claimsExp(roleMedia, testNow.Add(time.Second)))); err != nil {
		t.Fatalf("a token one second before exp must be valid, got %v", err)
	}
}
