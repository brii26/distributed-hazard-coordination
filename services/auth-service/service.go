package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/brii26/distributed-hazard-coordination/services/auth-service/authpb"
)

const roleLapangan = "Lapangan"

// otherRolesTTL applies to Media and Operator. The TRD only fixes Tim
// Lapangan's short TTL (ACCESS_TOKEN_TTL_S); keeping it short is what makes the
// transparent-refresh flow demoable.
const otherRolesTTL = 15 * time.Minute

type authServer struct {
	authpb.UnimplementedAuthServer

	store       userStore
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
func (s *authServer) tokenResponse(u user, refreshToken string) (*authpb.TokenResponse, error) {
	iat := s.now()
	exp := iat.Add(s.ttlFor(u.Role))

	// The schema has no display name, so the email doubles as the name claim.
	jwt, err := signJWT(s.secret, jwtClaims{Name: u.Email, Email: u.Email, Role: u.Role, Iat: iat.Unix(), Exp: exp.Unix()})
	if err != nil {
		return nil, err
	}

	log.Printf("jwt issued: role=%s exp=%d", u.Role, exp.Unix())

	return &authpb.TokenResponse{Jwt: jwt, RefreshToken: refreshToken, ExpiresIn: int64(exp.Sub(iat).Seconds())}, nil
}

var errInvalidCredentials = status.Error(codes.Unauthenticated, "invalid credentials")

func (s *authServer) Login(ctx context.Context, req *authpb.LoginRequest) (*authpb.TokenResponse, error) {
	u, err := s.store.UserByEmail(ctx, req.GetEmail())
	if err != nil && !errors.Is(err, errNotFound) {
		log.Printf("login: store error: %v", err)

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
		log.Printf("login: store error: %v", err)

		return nil, status.Error(codes.Internal, "internal error")
	}

	resp, err := s.tokenResponse(u, refresh)
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}

	log.Printf("login: user_id=%s result=ok", u.ID)

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
		log.Printf("refresh: store error: %v", err)

		return nil, status.Error(codes.Internal, "internal error")
	}

	resp, err := s.tokenResponse(u, next)
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}

	log.Printf("refresh: user_id=%s result=rotated", u.ID)

	return resp, nil
}

func (s *authServer) Logout(ctx context.Context, req *authpb.LogoutRequest) (*emptypb.Empty, error) {
	if err := s.store.DeleteRefreshToken(ctx, req.GetRefreshToken()); err != nil {
		log.Printf("logout: store error: %v", err)

		return nil, status.Error(codes.Internal, "internal error")
	}

	log.Print("logout: done")

	return &emptypb.Empty{}, nil
}
