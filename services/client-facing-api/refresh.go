package main

import (
	"context"
	"errors"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/authpb"
)

// withRefresh turns on transparent refresh for the verifier's protected routes:
// a validly signed but expired jwt plus a refresh_token cookie is exchanged at the
// Auth Service and the request carries on, all in one round trip for the client
// (doc/TRD_M1.md section 4.2).
func (v *jwtVerifier) withRefresh(a *authAPI) *jwtVerifier {
	v.refresh = func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		a.refreshAndForward(v, w, r, next)
	}

	return v
}

// refreshAndForward holds the request while it swaps the refresh token for a new
// pair, puts the new JWT into the held request, runs next, and re-sets both cookies
// on the response.
//
// Known race: two concurrent requests with the same expired jwt both try this, but
// the Auth Service rotates a refresh token only once. The loser's token is already
// gone, so it takes the invalid-token path below: 401 with both cookies cleared. A
// client that gets that 401 starts over from login (or, if the winner's new cookies
// reached it first, simply retries with them). Single-flight per refresh token would
// let every parallel request succeed. ponytail: not built, the spec only asks that
// the loser's outcome be defined; it also only helps within one instance.
func (a *authAPI) refreshAndForward(v *jwtVerifier, w http.ResponseWriter, r *http.Request, next http.Handler) {
	old, err := r.Cookie(cookieRefresh)
	if err != nil {
		unauthorized(w) // the middleware only hands over requests that have one

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.timeout)
	defer cancel()

	resp, err := a.auth.Refresh(ctx, &authpb.RefreshRequest{RefreshToken: old.Value})
	if err != nil {
		a.refreshFailed(r.Context(), w, err)

		return
	}

	// The new JWT is signed by our own Auth Service with the shared secret, so it
	// must verify; if it does not, the two services disagree on JWT_SIGNING_SECRET.
	claims, err := v.verify(resp.GetJwt())
	if err != nil {
		logFor(r.Context()).Error("transparent refresh", "result", "failure", "reason", "the Auth Service issued a token that does not verify", "error", err)
		writeJSON(w, http.StatusBadGateway, errBody("authentication service unavailable"))

		return
	}

	a.setCookie(w, cookieJWT, resp.GetJwt(), 0)
	a.setCookie(w, cookieRefresh, resp.GetRefreshToken(), 0)
	logFor(r.Context()).Info("transparent refresh", "client_id", claims.Email, "result", "rotated")

	held := withSession(r, resp.GetJwt(), resp.GetRefreshToken())
	next.ServeHTTP(w, held.WithContext(context.WithValue(held.Context(), claimsKey{}, claims)))
}

// refreshFailed answers a failed exchange. An unknown refresh token is a 401 and
// both cookies are cleared. An Auth Service that is down or too slow is a 502 or
// 504 and the cookies stay: the refresh token may still be valid.
func (a *authAPI) refreshFailed(ctx context.Context, w http.ResponseWriter, err error) {
	switch {
	case status.Code(err) == codes.Unauthenticated:
		logFor(ctx).Info("transparent refresh", "result", "rejected", "reason", "unknown refresh token")
		a.setCookie(w, cookieJWT, "", -1)
		a.setCookie(w, cookieRefresh, "", -1)
		unauthorized(w)
	case errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded:
		logFor(ctx).Warn("transparent refresh", "result", "timeout")
		writeJSON(w, http.StatusGatewayTimeout, errBody("authentication service timed out"))
	default:
		logFor(ctx).Warn("transparent refresh", "result", "failure", "error", err)
		writeJSON(w, http.StatusBadGateway, errBody("authentication service unavailable"))
	}
}

// withSession returns a copy of r whose jwt and refresh_token cookies carry the
// new values, so what runs next sees the fresh session, not the expired one.
func withSession(r *http.Request, jwt, refresh string) *http.Request {
	held := r.Clone(r.Context())
	cookies := r.Cookies()

	held.Header.Del("Cookie")

	for _, c := range cookies {
		switch c.Name {
		case cookieJWT:
			c.Value = jwt
		case cookieRefresh:
			c.Value = refresh
		}

		held.AddCookie(c)
	}

	return held
}
