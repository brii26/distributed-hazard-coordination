package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/auth-service/authpb"
)

const (
	lapEmail   = "lap@x.example"
	testSalt   = "salt"
	testSecret = "jwt-secret"
)

var testNow = time.Unix(1_700_000_000, 0)

type fakeStore struct {
	mu     sync.Mutex
	users  map[string]user // by email
	tokens map[string]string
}

func (f *fakeStore) UserByEmail(_ context.Context, email string) (user, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if u, ok := f.users[email]; ok {
		return u, nil
	}

	return user{}, errNotFound
}

func (f *fakeStore) AddRefreshToken(_ context.Context, token, userID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.tokens[token] = userID

	return nil
}

func (f *fakeStore) RotateRefreshToken(_ context.Context, oldToken, newToken string) (user, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id, ok := f.tokens[oldToken]
	if !ok {
		return user{}, errNotFound
	}

	delete(f.tokens, oldToken)

	f.tokens[newToken] = id

	for _, u := range f.users {
		if u.ID == id {
			return u, nil
		}
	}

	return user{}, errNotFound
}

func (f *fakeStore) DeleteRefreshToken(_ context.Context, token string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.tokens, token)

	return nil
}

func newTestServer() *authServer {
	mk := func(id, role string) user {
		return user{ID: id, Email: id + "@x.example", PasswordHash: hashPassword("pw-"+id, testSalt), Role: role}
	}

	return &authServer{
		store: &fakeStore{
			users:  map[string]user{lapEmail: mk("lap", "Lapangan"), "op@x.example": mk("op", "Operator")},
			tokens: map[string]string{},
		},
		salt:        testSalt,
		secret:      []byte(testSecret),
		lapanganTTL: 60 * time.Second,
		now:         func() time.Time { return testNow },
	}
}

func claimsOf(t *testing.T, jwt string) jwtClaims {
	t.Helper()

	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		t.Fatalf("malformed jwt %q", jwt)
	}

	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write([]byte(parts[0] + "." + parts[1]))

	if !hmac.Equal(mac.Sum(nil), mustDecode(t, parts[2])) {
		t.Fatal("jwt signature does not verify")
	}

	var c jwtClaims
	if err := json.Unmarshal(mustDecode(t, parts[1]), &c); err != nil {
		t.Fatal(err)
	}

	return c
}

func mustDecode(t *testing.T, s string) []byte {
	t.Helper()

	b, err := b64.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}

	return b
}

func TestLoginIssuesVerifiableJWT(t *testing.T) {
	t.Parallel()

	s := newTestServer()

	lap, err := s.Login(context.Background(), &authpb.LoginRequest{Email: lapEmail, Password: "pw-lap"})
	if err != nil {
		t.Fatal(err)
	}

	c := claimsOf(t, lap.GetJwt())
	if c.Role != "Lapangan" || c.Email != lapEmail || c.Exp-c.Iat != 60 || lap.GetExpiresIn() != 60 || len(lap.GetRefreshToken()) != 64 {
		t.Fatalf("unexpected Lapangan claims/response: %+v %v", c, lap)
	}

	op, err := s.Login(context.Background(), &authpb.LoginRequest{Email: "op@x.example", Password: "pw-op"})
	if err != nil {
		t.Fatal(err)
	}

	if c := claimsOf(t, op.GetJwt()); c.Role != "Operator" || c.Exp-c.Iat != int64(otherRolesTTL.Seconds()) {
		t.Fatalf("unexpected Operator claims: %+v", c)
	}
}

func TestLoginFailureDoesNotLeakWhichPartWasWrong(t *testing.T) {
	t.Parallel()

	s := newTestServer()

	_, wrongPW := s.Login(context.Background(), &authpb.LoginRequest{Email: lapEmail, Password: "nope"})
	_, wrongEmail := s.Login(context.Background(), &authpb.LoginRequest{Email: "ghost@x.example", Password: "pw-lap"})

	if status.Code(wrongPW) != codes.Unauthenticated || wrongPW.Error() != wrongEmail.Error() {
		t.Fatalf("errors must be identical: %v vs %v", wrongPW, wrongEmail)
	}
}

func TestRefreshRotatesAndOldTokenFails(t *testing.T) {
	t.Parallel()

	s := newTestServer()
	ctx := context.Background()

	first, err := s.Login(ctx, &authpb.LoginRequest{Email: lapEmail, Password: "pw-lap"})
	if err != nil {
		t.Fatal(err)
	}

	second, err := s.Refresh(ctx, &authpb.RefreshRequest{RefreshToken: first.GetRefreshToken()})
	if err != nil || second.GetRefreshToken() == first.GetRefreshToken() || claimsOf(t, second.GetJwt()).Role != "Lapangan" {
		t.Fatalf("refresh should rotate and issue a valid jwt: %v %v", second, err)
	}

	if _, err := s.Refresh(ctx, &authpb.RefreshRequest{RefreshToken: first.GetRefreshToken()}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("reusing the old refresh token must fail, got %v", err)
	}

	if _, err := s.Refresh(ctx, &authpb.RefreshRequest{RefreshToken: second.GetRefreshToken()}); err != nil {
		t.Fatalf("the new refresh token must work: %v", err)
	}
}

func TestLogoutDeletesRefreshToken(t *testing.T) {
	t.Parallel()

	s := newTestServer()
	ctx := context.Background()

	resp, err := s.Login(ctx, &authpb.LoginRequest{Email: "op@x.example", Password: "pw-op"})
	if err != nil {
		t.Fatal(err)
	}

	for range 2 { // second logout of the same token is a no-op, not an error
		if _, err := s.Logout(ctx, &authpb.LogoutRequest{RefreshToken: resp.GetRefreshToken()}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := s.Refresh(ctx, &authpb.RefreshRequest{RefreshToken: resp.GetRefreshToken()}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("refresh after logout must fail, got %v", err)
	}
}
