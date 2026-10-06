package main

// The Auth Service store against a real User DB. Run them with
// tests/integration/storage.sh; without USER_DB_TEST_URL they skip, so
// `go test ./...` stays offline.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	schemaOnce sync.Once
	schemaErr  error
	seq        atomic.Int64
)

// testStore connects to USER_DB_TEST_URL and makes sure the schema exists. The
// migration runs once per test binary: parallel CREATE TABLEs on an empty
// database race each other in Postgres.
func testStore(t *testing.T) pgStore {
	t.Helper()

	url := os.Getenv("USER_DB_TEST_URL")
	if url == "" {
		t.Skip("USER_DB_TEST_URL not set (run tests/integration/storage.sh)")
	}

	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = db.Close() })

	schemaOnce.Do(func() {
		var ddl []byte
		if ddl, schemaErr = os.ReadFile("migrations/001_users_refresh_tokens.sql"); schemaErr == nil {
			_, schemaErr = db.ExecContext(context.Background(), string(ddl))
		}
	})

	if schemaErr != nil {
		t.Fatal(schemaErr)
	}

	return pgStore{db: db}
}

// newUser inserts a user unique to this test and removes it, and its refresh
// tokens, afterwards.
func newUser(t *testing.T, s pgStore, role string) user {
	t.Helper()

	n := seq.Add(1)
	u := user{
		ID:           fmt.Sprintf("it-%d-%d", time.Now().UnixNano(), n),
		Email:        fmt.Sprintf("it-%d-%d@store.example", time.Now().UnixNano(), n),
		PasswordHash: hashPassword("pw", "salt"),
		Role:         role,
	}

	if err := s.CreateUser(context.Background(), u); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = s.db.ExecContext(ctx, `DELETE FROM refresh_tokens WHERE user_id = $1`, u.ID)
		_, _ = s.db.ExecContext(ctx, `DELETE FROM users WHERE user_id = $1`, u.ID)
	})

	return u
}

func token(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), seq.Add(1))
}

func tokensOf(t *testing.T, s pgStore, userID string) int {
	t.Helper()

	var n int
	if err := s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM refresh_tokens WHERE user_id = $1`, userID).Scan(&n); err != nil {
		t.Fatal(err)
	}

	return n
}

func TestStoreLoginLookup(t *testing.T) {
	t.Parallel()

	s := testStore(t)
	u := newUser(t, s, roleLapangan)

	got, err := s.UserByEmail(context.Background(), u.Email)
	if err != nil || got != u {
		t.Fatalf("expected %+v, got %+v %v", u, got, err)
	}

	if _, err := s.UserByEmail(context.Background(), "nobody-"+u.Email); !errors.Is(err, errNotFound) {
		t.Fatalf("an unknown email must be errNotFound, got %v", err)
	}
}

// TestStoreSeededOperatorMatchesLogin checks the seed script and the Login RPC
// hash passwords the same way, so the seeded Operator can actually log in.
func TestStoreSeededOperatorMatchesLogin(t *testing.T) {
	t.Parallel()

	s := testStore(t)

	email, password, salt := os.Getenv("SEED_OPERATOR_EMAIL"), os.Getenv("SEED_OPERATOR_PASSWORD"), os.Getenv("SECRET_SALT")
	if email == "" {
		t.Skip("SEED_OPERATOR_EMAIL not set")
	}

	u, err := s.UserByEmail(context.Background(), email)
	if err != nil {
		t.Fatalf("the seeded Operator is missing: %v", err)
	}

	if u.Role != roleOperator || u.PasswordHash != hashPassword(password, salt) {
		t.Fatalf("the seeded Operator must be an Operator whose hash Login accepts, got role %s", u.Role)
	}
}

func TestStoreCreateUserRejectsADuplicateEmail(t *testing.T) {
	t.Parallel()

	s := testStore(t)
	u := newUser(t, s, roleMedia)

	dup := user{ID: u.ID + "-dup", Email: u.Email, PasswordHash: u.PasswordHash, Role: roleMedia}
	if err := s.CreateUser(context.Background(), dup); !errors.Is(err, errExists) {
		t.Fatalf("a taken email must be errExists, got %v", err)
	}

	bad := user{ID: u.ID + "-bad", Email: "other-" + u.Email, PasswordHash: u.PasswordHash, Role: "Admin"}
	if err := s.CreateUser(context.Background(), bad); err == nil || errors.Is(err, errExists) {
		t.Fatalf("an unknown role must fail, and not as errExists, got %v", err)
	}
}

func TestStoreRotationReplacesTheTokenAndRejectsTheOldOne(t *testing.T) {
	t.Parallel()

	s := testStore(t)
	ctx := context.Background()
	u := newUser(t, s, roleLapangan)

	first, second, third := token("r1"), token("r2"), token("r3")
	if err := s.AddRefreshToken(ctx, first, u.ID); err != nil {
		t.Fatal(err)
	}

	got, err := s.RotateRefreshToken(ctx, first, second)
	if err != nil || got != u {
		t.Fatalf("rotation must return the owner, got %+v %v", got, err)
	}

	if _, err := s.RotateRefreshToken(ctx, first, third); !errors.Is(err, errNotFound) {
		t.Fatalf("the rotated-out token must be rejected, got %v", err)
	}

	if _, err := s.RotateRefreshToken(ctx, second, third); err != nil {
		t.Fatalf("the new token must rotate, got %v", err)
	}

	if n := tokensOf(t, s, u.ID); n != 1 {
		t.Fatalf("expected exactly one live refresh token, found %d", n)
	}
}

func TestStoreConcurrentReuseOfOneTokenRotatesOnce(t *testing.T) {
	t.Parallel()

	s := testStore(t)
	ctx := context.Background()
	u := newUser(t, s, roleLapangan)

	shared := token("shared")
	if err := s.AddRefreshToken(ctx, shared, u.ID); err != nil {
		t.Fatal(err)
	}

	const racers = 10

	var won, lost, other atomic.Int64

	var wg sync.WaitGroup

	for i := range racers {
		wg.Go(func() {
			_, err := s.RotateRefreshToken(ctx, shared, token(fmt.Sprintf("racer%d", i)))

			switch {
			case err == nil:
				won.Add(1)
			case errors.Is(err, errNotFound):
				lost.Add(1)
			default:
				other.Add(1)
				t.Errorf("unexpected error: %v", err)
			}
		})
	}

	wg.Wait()

	if won.Load() != 1 || lost.Load() != racers-1 || other.Load() != 0 {
		t.Fatalf("expected one winner and %d errNotFound, got %d won, %d lost, %d other", racers-1, won.Load(), lost.Load(), other.Load())
	}

	if n := tokensOf(t, s, u.ID); n != 1 {
		t.Fatalf("expected one live refresh token after the race, found %d", n)
	}
}

func TestStoreLogoutDeletesTheToken(t *testing.T) {
	t.Parallel()

	s := testStore(t)
	ctx := context.Background()
	u := newUser(t, s, roleMedia)

	tok := token("logout")
	if err := s.AddRefreshToken(ctx, tok, u.ID); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteRefreshToken(ctx, tok); err != nil {
		t.Fatal(err)
	}

	if _, err := s.RotateRefreshToken(ctx, tok, token("after")); !errors.Is(err, errNotFound) {
		t.Fatalf("a logged-out token must not rotate, got %v", err)
	}

	if err := s.DeleteRefreshToken(ctx, tok); err != nil {
		t.Fatalf("deleting a missing token must not be an error, got %v", err)
	}
}
