package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/brii26/distributed-hazard-coordination/services/auth-service/authpb"
)

const (
	roleMedia    = "Media"
	roleLapangan = "Lapangan"
	roleOperator = "Operator"

	maxEmailLen = 255 // users.email is VARCHAR(255)
)

var validRoles = map[string]bool{roleMedia: true, roleLapangan: true, roleOperator: true}

// otherRolesTTL applies to Media and Operator. The TRD only fixes Tim
// Lapangan's short TTL (ACCESS_TOKEN_TTL_S); keeping it short is what makes the
// transparent-refresh flow demoable.
const otherRolesTTL = 15 * time.Minute

type authServer struct {
	authpb.UnimplementedAuthServer

	store       userStore
	ping        func(context.Context) error // User DB reachability, for /health
	salt        string
	secret      []byte
	lapanganTTL time.Duration
	now         func() time.Time
}

// hashPassword is hex(sha256(password + salt)), matching the Operator seed in
// migrations/002_seed_operator.sh and doc/TRD_M1.md section 4.1.
func hashPassword(password, salt string) string {
	sum := sha256.Sum256([]byte(password + salt))

	return hex.EncodeToString(sum[:])
}

func newRefreshToken() (string, error) {
	b := make([]byte, 32) // 64 hex chars = refresh_tokens.refresh_token VARCHAR(64)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}

func (s *authServer) ttlFor(role string) time.Duration {
	if role == roleLapangan {
		return s.lapanganTTL
	}

	return otherRolesTTL
}

// tokenResponse signs a JWT for u; the caller supplies the refresh token.
func (s *authServer) tokenResponse(ctx context.Context, u user, refreshToken string) (*authpb.TokenResponse, error) {
	iat := s.now()
	exp := iat.Add(s.ttlFor(u.Role))

	// The schema has no display name, so the email doubles as the name claim.
	jwt, err := signJWT(s.secret, jwtClaims{Name: u.Email, Email: u.Email, Role: u.Role, Iat: iat.Unix(), Exp: exp.Unix()})
	if err != nil {
		return nil, err
	}

	logFor(ctx).Info("jwt issued", "role", u.Role, "exp", exp.Unix())

	return &authpb.TokenResponse{Jwt: jwt, RefreshToken: refreshToken, ExpiresIn: int64(exp.Sub(iat).Seconds())}, nil
}

var errInvalidCredentials = status.Error(codes.Unauthenticated, "invalid credentials")

func (s *authServer) Login(ctx context.Context, req *authpb.LoginRequest) (*authpb.TokenResponse, error) {
	u, err := s.store.UserByEmail(ctx, req.GetEmail())
	if err != nil && !errors.Is(err, errNotFound) {
		logFor(ctx).Warn("login store error", "error", err)

		return nil, status.Error(codes.Internal, "internal error")
	}

	// Always hash and compare, so an unknown email costs the same as a wrong
	// password, and return one error for both so neither leaks.
	got := hashPassword(req.GetPassword(), s.salt)
	if err != nil || subtle.ConstantTimeCompare([]byte(got), []byte(u.PasswordHash)) != 1 {
		log.Print("login: result=failed")

		return nil, errInvalidCredentials
	}

	refresh, err := newRefreshToken()
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}

	if err := s.store.AddRefreshToken(ctx, refresh, u.ID); err != nil {
		logFor(ctx).Warn("login store error", "error", err)

		return nil, status.Error(codes.Internal, "internal error")
	}

	resp, err := s.tokenResponse(ctx, u, refresh)
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}

	logFor(ctx).Info("login", "user_id", u.ID, "result", "ok")

	return resp, nil
}

func (s *authServer) Refresh(ctx context.Context, req *authpb.RefreshRequest) (*authpb.TokenResponse, error) {
	next, err := newRefreshToken()
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}

	u, err := s.store.RotateRefreshToken(ctx, req.GetRefreshToken(), next)
	if errors.Is(err, errNotFound) {
		log.Print("refresh: result=invalid_token")

		return nil, status.Error(codes.Unauthenticated, "invalid refresh token")
	}

	if err != nil {
		logFor(ctx).Warn("refresh store error", "error", err)

		return nil, status.Error(codes.Internal, "internal error")
	}

	resp, err := s.tokenResponse(ctx, u, next)
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}

	logFor(ctx).Info("refresh", "user_id", u.ID, "result", "rotated")

	return resp, nil
}

func (s *authServer) Logout(ctx context.Context, req *authpb.LogoutRequest) (*emptypb.Empty, error) {
	if err := s.store.DeleteRefreshToken(ctx, req.GetRefreshToken()); err != nil {
		logFor(ctx).Warn("logout store error", "error", err)

		return nil, status.Error(codes.Internal, "internal error")
	}

	log.Print("logout: done")

	return &emptypb.Empty{}, nil
}

// newUserID returns a random (version 4) UUID.
func newUserID() (string, error) {
	var b [16]byte

	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}

	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

// create validates the input and inserts the account; Register and CreateAccount
// both go through it, so a duplicate email is rejected the same way on either path.
func (s *authServer) create(ctx context.Context, email, password, role string) error {
	if email == "" || len(email) > maxEmailLen || password == "" {
		return status.Error(codes.InvalidArgument, "email and password are required")
	}

	if !validRoles[role] {
		return status.Error(codes.InvalidArgument, "invalid role")
	}

	id, err := newUserID()
	if err != nil {
		return status.Error(codes.Internal, "internal error")
	}

	err = s.store.CreateUser(ctx, user{ID: id, Email: email, PasswordHash: hashPassword(password, s.salt), Role: role})

	switch {
	case errors.Is(err, errExists):
		return status.Error(codes.AlreadyExists, "email already registered")
	case err != nil:
		logFor(ctx).Warn("create account store error", "error", err)

		return status.Error(codes.Internal, "internal error")
	}

	logFor(ctx).Info("account created", "user_id", id, "role", role)

	return nil
}

// Register creates a Media account. The request has no role, so there is nothing
// to escalate.
func (s *authServer) Register(ctx context.Context, req *authpb.RegisterRequest) (*emptypb.Empty, error) {
	if err := s.create(ctx, req.GetEmail(), req.GetPassword(), roleMedia); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// CreateAccount creates an account with the requested role. It trusts its caller:
// the Operator check happens in the Client-Facing API (doc/TRD_M1.md section 5).
func (s *authServer) CreateAccount(ctx context.Context, req *authpb.CreateAccountRequest) (*emptypb.Empty, error) {
	if err := s.create(ctx, req.GetEmail(), req.GetPassword(), req.GetRole()); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}
