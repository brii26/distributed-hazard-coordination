package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
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

	testPassword = "pw-12345"
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

func (f *fakeStore) CreateUser(_ context.Context, u user) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.users[u.Email]; ok {
		return errExists
	}

	f.users[u.Email] = u

	return nil
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
			users:  map[string]user{lapEmail: mk("lap", roleLapangan), "op@x.example": mk("op", roleOperator)},
			tokens: map[string]string{},
		},
		salt:      testSalt,
		secret:    []byte(testSecret),
		accessTTL: 60 * time.Second,
		now:       func() time.Time { return testNow },
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
	if c.Role != roleLapangan || c.Email != lapEmail || c.Exp-c.Iat != 60 || lap.GetExpiresIn() != 60 || len(lap.GetRefreshToken()) != 64 {
		t.Fatalf("unexpected Lapangan claims/response: %+v %v", c, lap)
	}

	op, err := s.Login(context.Background(), &authpb.LoginRequest{Email: "op@x.example", Password: "pw-op"})
	if err != nil {
		t.Fatal(err)
	}

	if c := claimsOf(t, op.GetJwt()); c.Role != roleOperator || c.Exp-c.Iat != 60 {
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
	if err != nil || second.GetRefreshToken() == first.GetRefreshToken() || claimsOf(t, second.GetJwt()).Role != roleLapangan {
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

	for range 2 {
		if _, err := s.Logout(ctx, &authpb.LogoutRequest{RefreshToken: resp.GetRefreshToken()}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := s.Refresh(ctx, &authpb.RefreshRequest{RefreshToken: resp.GetRefreshToken()}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("refresh after logout must fail, got %v", err)
	}
}

// Logs in with the given credentials and returns the role in the JWT.
func loginRole(t *testing.T, s *authServer, email, password string) string {
	t.Helper()

	resp, err := s.Login(context.Background(), &authpb.LoginRequest{Email: email, Password: password})
	if err != nil {
		t.Fatalf("login as %s: %v", email, err)
	}

	return claimsOf(t, resp.GetJwt()).Role
}

func TestRegisterAlwaysCreatesMedia(t *testing.T) {
	t.Parallel()

	s := newTestServer()

	if _, err := s.Register(context.Background(), &authpb.RegisterRequest{Email: "new@x.example", Password: testPassword}); err != nil {
		t.Fatal(err)
	}

	if got := loginRole(t, s, "new@x.example", testPassword); got != roleMedia {
		t.Fatalf("Register must create a Media account, got %q", got)
	}
}

func TestCreateAccountCanCreateEveryRole(t *testing.T) {
	t.Parallel()

	s := newTestServer()

	for _, role := range []string{roleMedia, roleLapangan, roleOperator} {
		email := role + "@x.example"

		if _, err := s.CreateAccount(context.Background(), &authpb.CreateAccountRequest{Email: email, Password: testPassword, Role: role}); err != nil {
			t.Fatalf("create %s: %v", role, err)
		}

		if got := loginRole(t, s, email, testPassword); got != role {
			t.Fatalf("created %s but logged in as %q", role, got)
		}
	}
}

func TestCreateAccountRejectsUnknownRole(t *testing.T) {
	t.Parallel()

	s := newTestServer()

	for _, role := range []string{"", "admin", "operator"} {
		_, err := s.CreateAccount(context.Background(), &authpb.CreateAccountRequest{Email: "r@x.example", Password: testPassword, Role: role})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("role %q: expected InvalidArgument, got %v", role, err)
		}
	}

	if _, err := s.store.UserByEmail(context.Background(), "r@x.example"); !errors.Is(err, errNotFound) {
		t.Fatal("a rejected role must not leave an account behind")
	}
}

func TestDuplicateEmailIsRejectedOnBothPaths(t *testing.T) {
	t.Parallel()

	s := newTestServer()
	ctx := context.Background()

	_, errRegister := s.Register(ctx, &authpb.RegisterRequest{Email: lapEmail, Password: "other-pass"})
	_, errCreate := s.CreateAccount(ctx, &authpb.CreateAccountRequest{Email: lapEmail, Password: "other-pass", Role: roleOperator})

	for _, err := range []error{errRegister, errCreate} {
		if status.Code(err) != codes.AlreadyExists {
			t.Fatalf("expected AlreadyExists, got %v", err)
		}
	}

	if got := loginRole(t, s, lapEmail, "pw-lap"); got != roleLapangan {
		t.Fatalf("existing account changed: role %q", got)
	}

	if _, err := s.Login(ctx, &authpb.LoginRequest{Email: lapEmail, Password: "other-pass"}); err == nil {
		t.Fatal("a rejected duplicate must not overwrite the password")
	}
}

func TestCreateRejectsEmptyOrOversizedInput(t *testing.T) {
	t.Parallel()

	s := newTestServer()

	for _, c := range []struct{ email, password string }{
		{"", testPassword},
		{"a@x.example", ""},
		{"a@x.example", "short"},
		{strings.Repeat("a", maxEmailLen) + "@x.example", testPassword},
		{"notanemail", testPassword},
		{"' OR '1'='1", testPassword},
		{"a\x00b@x.example", testPassword},
		{"Name <a@x.example>", testPassword},
	} {
		_, err := s.Register(context.Background(), &authpb.RegisterRequest{Email: c.email, Password: c.password})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("email %.12q password %q: expected InvalidArgument, got %v", c.email, c.password, err)
		}
	}
}

func TestEmailIsCaseInsensitiveAndNULNeverReachesTheStore(t *testing.T) {
	t.Parallel()

	s := newTestServer()
	ctx := context.Background()

	_, err := s.Register(ctx, &authpb.RegisterRequest{Email: strings.ToUpper(lapEmail), Password: testPassword})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("same email in upper case: expected AlreadyExists, got %v", err)
	}

	if got := loginRole(t, s, strings.ToUpper(lapEmail), "pw-lap"); got != roleLapangan {
		t.Fatalf("upper-case login: role %q", got)
	}

	if _, err := s.Login(ctx, &authpb.LoginRequest{Email: "a\x00b", Password: "x"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("NUL in login email: expected Unauthenticated, got %v", err)
	}
}

func TestEveryRoleGetsTheConfiguredTokenLifetime(t *testing.T) {
	t.Parallel()

	s := newTestServer()

	if _, err := s.Register(context.Background(), &authpb.RegisterRequest{Email: "media@x.example", Password: testPassword}); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct{ email, password, role string }{
		{"media@x.example", testPassword, roleMedia},
		{lapEmail, "pw-lap", roleLapangan},
		{"op@x.example", "pw-op", roleOperator},
	} {
		tok, err := s.Login(context.Background(), &authpb.LoginRequest{Email: c.email, Password: c.password})
		if err != nil {
			t.Fatalf("%s login: %v", c.role, err)
		}

		if cl := claimsOf(t, tok.GetJwt()); cl.Role != c.role || cl.Exp-cl.Iat != 60 || tok.GetExpiresIn() != 60 {
			t.Errorf("%s token lifetime: %+v expires_in=%d, want 60", c.role, cl, tok.GetExpiresIn())
		}
	}
}
