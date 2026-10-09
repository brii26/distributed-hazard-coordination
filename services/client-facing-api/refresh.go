package main

import (
	"context"
	"errors"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/authpb"
)

// Turns on transparent refresh for the protected routes: a validly signed but expired jwt plus a refresh_token cookie is exchanged at Auth and the request carries on, in one round trip.
func (v *jwtVerifier) withRefresh(a *authAPI) *jwtVerifier {
	v.refresh = func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		a.refreshAndForward(v, w, r, next)
	}

	v.clear = func(w http.ResponseWriter) {
		a.setCookie(w, cookieJWT, "", -1)
		a.setCookie(w, cookieRefresh, "", -1)
	}

	return v
}

type refreshCall struct {
	done chan struct{}
	resp *authpb.TokenResponse
	err  error
}

// Rotates old at the Auth Service once: requests arriving while that call is in flight share its result. The call is not tied to any one request, so a client that disconnects can't fail the others, and an old token is rejected once the call is done.
// ponytail: per instance only, two Client-Facing API copies would each rotate once and one would lose.
func (a *authAPI) exchange(ctx context.Context, old string) (*authpb.TokenResponse, error) {
	a.refreshMu.Lock()

	c, ok := a.refreshes[old]
	if !ok {
		c = &refreshCall{done: make(chan struct{})}

		if a.refreshes == nil {
			a.refreshes = map[string]*refreshCall{}
		}

		a.refreshes[old] = c

		go a.rotate(ctx, old, c)
	}

	a.refreshMu.Unlock()

	select {
	case <-c.done:
		return c.resp, c.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// The real Auth Service call behind exchange, on a context that keeps the trace but not the first caller's cancellation.
func (a *authAPI) rotate(ctx context.Context, old string, c *refreshCall) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.timeout)
	defer cancel()

	resp, err := a.auth.Refresh(ctx, &authpb.RefreshRequest{RefreshToken: old})

	a.refreshMu.Lock()
	c.resp, c.err = resp, err

	delete(a.refreshes, old)
	a.refreshMu.Unlock()
	close(c.done)
}

// Holds the request while swapping the refresh token for a new pair, puts the new JWT into it, runs next and re-sets both cookies. Requests sharing one expired session all get the same new pair.
func (a *authAPI) refreshAndForward(v *jwtVerifier, w http.ResponseWriter, r *http.Request, next http.Handler) {
	old, err := r.Cookie(cookieRefresh)
	if err != nil {
		unauthorized(w)

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.timeout)
	defer cancel()

	resp, err := a.exchange(ctx, old.Value)
	if err != nil {
		a.refreshFailed(r.Context(), w, err)

		return
	}

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

// Answers a failed exchange. An unknown refresh token is 401 with both cookies cleared. Auth down or too slow is 502/504 and the cookies stay.
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

// A copy of r whose jwt and refresh_token cookies carry the new values, so what runs next sees the fresh session.
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
