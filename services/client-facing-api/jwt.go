package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

const (
	roleMedia    = "Media"
	roleLapangan = "Lapangan"
	roleOperator = "Operator"
)

var validRoles = map[string]bool{roleMedia: true, roleLapangan: true, roleOperator: true}

var b64 = base64.RawURLEncoding

var (
	errInvalidToken = errors.New("invalid token")
	errExpiredToken = errors.New("token expired")
)

// jwtClaims is the payload the Auth Service signs (doc/TRD_M1.md section 4.1).
type jwtClaims struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Role  string `json:"role"`
	Iat   int64  `json:"iat"`
	Exp   int64  `json:"exp"`
}

type claimsKey struct{}

// claimsFrom returns the verified claims the middleware put on the request, for
// the handlers behind it (role-based field filtering, account-creation gating).
func claimsFrom(ctx context.Context) (jwtClaims, bool) {
	c, ok := ctx.Value(claimsKey{}).(jwtClaims)

	return c, ok
}

// jwtVerifier checks the jwt cookie locally, with the shared HMAC secret and no
// call to the Auth Service, so a valid token works even while it is down.
type jwtVerifier struct {
	secret []byte
	now    func() time.Time
	// refresh takes over when the token's signature is valid but it has expired
	// and a refresh_token cookie is present: the transparent refresh (#26). Until
	// that exists it is nil and an expired token is simply a 401.
	refresh func(w http.ResponseWriter, r *http.Request, next http.Handler)
}

func newJWTVerifier(secret []byte) *jwtVerifier {
	return &jwtVerifier{secret: secret, now: time.Now}
}

// verify returns the claims of a well-formed, correctly signed token. A token that
// is signed correctly but past its exp returns errExpiredToken with its claims; any
// other problem (shape, algorithm, signature, missing exp, unknown role) is
// errInvalidToken. The signature is checked before the claims are trusted.
func (v *jwtVerifier) verify(token string) (jwtClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return jwtClaims{}, errInvalidToken
	}

	var header struct {
		Alg string `json:"alg"`
	}

	if raw, err := b64.DecodeString(parts[0]); err != nil || json.Unmarshal(raw, &header) != nil || header.Alg != "HS256" {
		return jwtClaims{}, errInvalidToken // also rejects alg "none" and any other algorithm
	}

	sig, err := b64.DecodeString(parts[2])
	if err != nil {
		return jwtClaims{}, errInvalidToken
	}

	mac := hmac.New(sha256.New, v.secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))

	if !hmac.Equal(mac.Sum(nil), sig) {
		return jwtClaims{}, errInvalidToken
	}

	var c jwtClaims

	if raw, err := b64.DecodeString(parts[1]); err != nil || json.Unmarshal(raw, &c) != nil {
		return jwtClaims{}, errInvalidToken
	}

	// A token with no exp would never expire, and handlers rely on a known role.
	if c.Exp <= 0 || !validRoles[c.Role] {
		return jwtClaims{}, errInvalidToken
	}

	if !v.now().Before(time.Unix(c.Exp, 0)) {
		return c, errExpiredToken
	}

	return c, nil
}

// require protects next: a valid jwt cookie lets the request through with its
// claims in the context; a validly signed but expired one is handed to refresh when
// a refresh_token cookie exists; everything else is a 401.
func (v *jwtVerifier) require(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(cookieJWT)
		if err != nil || cookie.Value == "" {
			unauthorized(w)

			return
		}

		claims, err := v.verify(cookie.Value)

		switch {
		case err == nil:
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), claimsKey{}, claims)))
		case errors.Is(err, errExpiredToken) && v.refresh != nil && hasCookie(r, cookieRefresh):
			v.refresh(w, r, next)
		default:
			unauthorized(w)
		}
	})
}

func hasCookie(r *http.Request, name string) bool {
	c, err := r.Cookie(name)

	return err == nil && c.Value != ""
}

func unauthorized(w http.ResponseWriter) {
	writeJSON(w, http.StatusUnauthorized, errBody("unauthorized"))
}
