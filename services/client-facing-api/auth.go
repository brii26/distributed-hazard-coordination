package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/authpb"
)

const (
	cookieJWT     = "jwt"
	cookieRefresh = "refresh_token"

	maxAuthBody = 4 << 10
)

// authAPI is the public login/logout proxy in front of the Auth Service
// (doc/TRD_M1.md section 4.1). Browsers never talk to the Auth Service directly.
type authAPI struct {
	auth    authpb.AuthClient
	timeout time.Duration // UPSTREAM_TIMEOUT_MS, applied to every Auth Service call
	secure  bool          // COOKIE_SECURE: mark the cookies Secure (HTTPS only)

	// verifier protects the routes that need a logged-in caller. When nil those
	// routes are not registered.
	verifier *jwtVerifier
}

func (a *authAPI) register(mux *http.ServeMux) {
	mux.HandleFunc("POST /auth/login", a.login)
	mux.HandleFunc("POST /auth/logout", a.logout)
	mux.HandleFunc("POST /auth/register", a.signup)

	if a.verifier != nil {
		mux.Handle("POST /accounts", a.verifier.require(http.HandlerFunc(a.createAccount)))
	}
}

// setCookie sets a cookie that scripts can't read (HttpOnly). With maxAge 0 it is
// a session cookie, on purpose: transparent refresh (#26) needs the expired jwt
// cookie to still arrive with the request, so it must outlive the token's exp.
// A negative maxAge clears it.
func (a *authAPI) setCookie(w http.ResponseWriter, name, value string, maxAge int) {
	http.SetCookie(w, &http.Cookie{ //nolint:gosec // G124: Secure is COOKIE_SECURE, true by default; off only for plain-HTTP demos
		Name: name, Value: value, Path: "/", MaxAge: maxAge,
		HttpOnly: true, Secure: a.secure, SameSite: http.SameSiteLaxMode,
	})
}

func errBody(msg string) map[string]string {
	return map[string]string{"error": msg}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("failed to write response: %v", err)
	}
}

// login forwards the credentials to the Auth Service's Login RPC. A rejected
// login is one generic 401, so it never says whether the email or the password
// was wrong; an Auth Service that can't answer is a 502/504, not a 401.
func (a *authAPI) login(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxAuthBody)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid request body"))

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.timeout)
	defer cancel()

	resp, err := a.auth.Login(ctx, &authpb.LoginRequest{Email: body.Email, Password: body.Password})
	if err != nil {
		a.fail(r.Context(), w, "login", err)

		return
	}

	a.setCookie(w, cookieJWT, resp.GetJwt(), 0)
	a.setCookie(w, cookieRefresh, resp.GetRefreshToken(), 0)
	writeJSON(w, http.StatusOK, map[string]int64{"expires_in": resp.GetExpiresIn()})
}

// logout calls the Auth Service's Logout RPC with the refresh_token cookie and
// always clears both cookies. If the RPC fails the client is told (502), since
// the refresh token is then still valid on the server.
func (a *authAPI) logout(w http.ResponseWriter, r *http.Request) {
	a.setCookie(w, cookieJWT, "", -1)
	a.setCookie(w, cookieRefresh, "", -1)

	c, err := r.Cookie(cookieRefresh)
	if err != nil || c.Value == "" {
		w.WriteHeader(http.StatusNoContent) // nothing to revoke

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.timeout)
	defer cancel()

	if _, err := a.auth.Logout(ctx, &authpb.LogoutRequest{RefreshToken: c.Value}); err != nil {
		a.fail(r.Context(), w, "logout", err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// fail turns an Auth Service error into the public response.
func (a *authAPI) fail(ctx context.Context, w http.ResponseWriter, op string, err error) {
	switch {
	case status.Code(err) == codes.Unauthenticated:
		writeJSON(w, http.StatusUnauthorized, errBody("invalid credentials"))
	case errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded:
		logFor(ctx).Warn("auth service timed out", "op", op)
		writeJSON(w, http.StatusGatewayTimeout, errBody("authentication service timed out"))
	default:
		logFor(ctx).Warn("auth service error", "op", op, "error", err)
		writeJSON(w, http.StatusBadGateway, errBody("authentication service unavailable"))
	}
}
