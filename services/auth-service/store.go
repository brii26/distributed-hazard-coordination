package main

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	errNotFound = errors.New("not found")
	errExists   = errors.New("already exists")
)

type user struct {
	ID           string
	Email        string
	PasswordHash string
	Role         string
}

// userStore is the Auth Service's view of the User DB (doc/TRD_M1.md section 5).
type userStore interface {
	// UserByEmail returns errNotFound for an unknown email.
	UserByEmail(ctx context.Context, email string) (user, error)
	// CreateUser inserts u; an email that is already taken returns errExists.
	CreateUser(ctx context.Context, u user) error
	// AddRefreshToken stores a new refresh token row for userID.
	AddRefreshToken(ctx context.Context, token, userID string) error
	// RotateRefreshToken atomically replaces oldToken with newToken and returns
	// the owning user, or errNotFound if oldToken doesn't exist (so a reused,
	// already-rotated token fails).
	RotateRefreshToken(ctx context.Context, oldToken, newToken string) (user, error)
	// DeleteRefreshToken removes the row; deleting a missing token is not an error.
	DeleteRefreshToken(ctx context.Context, token string) error
}

// logDB logs one User DB call. "No such row" and "already exists" are answers
// the caller acts on, not failed calls, so they log as a success.
func logDB(ctx context.Context, target string, start time.Time, err error) {
	if errors.Is(err, errNotFound) || errors.Is(err, errExists) {
		err = nil
	}

	logCall(ctx, target, start, err)
}

type pgStore struct {
	db *sql.DB
}

func (s pgStore) UserByEmail(ctx context.Context, email string) (_ user, err error) {
	start := time.Now()
	defer func() { logDB(ctx, "user-db select user", start, err) }()

	var u user

	err = s.db.QueryRowContext(ctx,
		`SELECT user_id, email, password_hash, role FROM users WHERE email = $1`, email,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Role)
	if errors.Is(err, sql.ErrNoRows) {
		return user{}, errNotFound
	}

	return u, err
}

func (s pgStore) CreateUser(ctx context.Context, u user) (err error) {
	start := time.Now()
	defer func() { logDB(ctx, "user-db insert user", start, err) }()

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO users (user_id, email, password_hash, role) VALUES ($1, $2, $3, $4)`,
		u.ID, u.Email, u.PasswordHash, u.Role)

	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "23505" { // unique_violation: the email is taken
		return errExists
	}

	return err
}

func (s pgStore) AddRefreshToken(ctx context.Context, token, userID string) (err error) {
	start := time.Now()
	defer func() { logDB(ctx, "user-db insert refresh token", start, err) }()

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO refresh_tokens (refresh_token, user_id) VALUES ($1, $2)`, token, userID)

	return err
}

func (s pgStore) RotateRefreshToken(ctx context.Context, oldToken, newToken string) (_ user, err error) {
	start := time.Now()
	defer func() { logDB(ctx, "user-db rotate refresh token", start, err) }()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return user{}, err
	}

	defer func() { _ = tx.Rollback() }()

	// DELETE ... RETURNING makes concurrent reuse of one token race-safe: only
	// one caller gets the row (doc/TRD_M1.md section 4.2 known limitation).
	var u user

	err = tx.QueryRowContext(ctx,
		`WITH old AS (DELETE FROM refresh_tokens WHERE refresh_token = $1 RETURNING user_id)
		 SELECT u.user_id, u.email, u.password_hash, u.role FROM users u JOIN old ON old.user_id = u.user_id`,
		oldToken,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Role)
	if errors.Is(err, sql.ErrNoRows) {
		return user{}, errNotFound
	}

	if err != nil {
		return user{}, err
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO refresh_tokens (refresh_token, user_id) VALUES ($1, $2)`, newToken, u.ID); err != nil {
		return user{}, err
	}

	return u, tx.Commit()
}

func (s pgStore) DeleteRefreshToken(ctx context.Context, token string) (err error) {
	start := time.Now()
	defer func() { logDB(ctx, "user-db delete refresh token", start, err) }()

	_, err = s.db.ExecContext(ctx, `DELETE FROM refresh_tokens WHERE refresh_token = $1`, token)

	return err
}
