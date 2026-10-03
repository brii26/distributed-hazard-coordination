package main

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

const (
	grantAuthCode     = "authorization_code"
	grantRefreshToken = "refresh_token"
	tokenTTLSeconds   = 60
	maxTokenBodyBytes = 4096
)

type tokenPair struct {
	access  string
	refresh string
}

// tokenStore is the mock's OAuth state: one registered client, a one-time
// bootstrap code, and the single currently valid token pair per client_id.
// ponytail: one global mutex is plenty for a single-client mock; shard per
// client_id if this ever serves many.
type tokenStore struct {
	clientID     string
	clientSecret string
	oneTimeCode  string

	mu       sync.Mutex
	codeUsed bool
	tokens   map[string]tokenPair
}

func newTokenStore(clientID, clientSecret, oneTimeCode string) *tokenStore {
	return &tokenStore{
		clientID:     clientID,
		clientSecret: clientSecret,
		oneTimeCode:  oneTimeCode,
		tokens:       map[string]tokenPair{},
	}
}

func equal(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}

// exchange validates the grant and, on success, replaces the client's token
// pair (so the previous refresh token stops working). It returns an OAuth
// error code, or "" on success.
func (s *tokenStore) exchange(grant, code, refresh string) (tokenPair, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch grant {
	case grantAuthCode:
		if s.codeUsed || !equal(code, s.oneTimeCode) {
			return tokenPair{}, "invalid_grant"
		}
	case grantRefreshToken:
		cur, ok := s.tokens[s.clientID]
		if !ok || !equal(refresh, cur.refresh) {
			return tokenPair{}, "invalid_grant"
		}
	default:
		return tokenPair{}, "unsupported_grant_type"
	}

	access, err := randomToken()
	if err != nil {
		return tokenPair{}, "server_error"
	}

	newRefresh, err := randomToken()
	if err != nil {
		return tokenPair{}, "server_error"
	}

	// The one-time code is only consumed once a token was actually issued.
	if grant == grantAuthCode {
		s.codeUsed = true
	}

	pair := tokenPair{access: access, refresh: newRefresh}
	s.tokens[s.clientID] = pair

	return pair, ""
}

// loggableGrant maps the client-supplied grant_type onto known constants so
// arbitrary request input never reaches the log.
func loggableGrant(grant string) string {
	switch grant {
	case grantAuthCode:
		return grantAuthCode
	case grantRefreshToken:
		return grantRefreshToken
	default:
		return "unsupported"
	}
}

func writeOAuthError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(map[string]string{"error": code}); err != nil {
		log.Printf("failed to write oauth error response: %v", err)
	}
}

func (s *tokenStore) tokenHandler(w http.ResponseWriter, r *http.Request) {
	id, secret, ok := r.BasicAuth()
	if !ok || !equal(id, s.clientID) || !equal(secret, s.clientSecret) {
		log.Print("oauth: result=invalid_client")
		w.Header().Set("WWW-Authenticate", `Basic realm="pvmbg"`)
		writeOAuthError(w, http.StatusUnauthorized, "invalid_client")

		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxTokenBodyBytes)

	if err := r.ParseForm(); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request")

		return
	}

	grant := r.PostFormValue("grant_type")
	logGrant := loggableGrant(grant)
	pair, errCode := s.exchange(grant, r.PostFormValue("code"), r.PostFormValue("refresh_token"))

	if errCode != "" {
		status := http.StatusBadRequest
		if errCode == "server_error" {
			status = http.StatusInternalServerError
		}

		log.Printf("oauth: client_id=%q grant_type=%s result=%s", s.clientID, logGrant, errCode)
		writeOAuthError(w, status, errCode)

		return
	}

	log.Printf("oauth: client_id=%q grant_type=%s result=issued", s.clientID, logGrant)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")

	resp := map[string]any{
		"access_token":  pair.access,
		"refresh_token": pair.refresh,
		"token_type":    "Bearer",
		"expires_in":    tokenTTLSeconds,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("failed to write token response: %v", err)
	}
}
