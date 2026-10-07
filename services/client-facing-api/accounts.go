package main

import (
	"context"
	"encoding/json"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/authpb"
)

// POST /auth/register: anyone may self-register, always as Media. It proxies to Register, which has no role parameter, so a "role" in the body is ignored.
func (a *authAPI) signup(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	if !decodeBody(w, r, &body) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.timeout)
	defer cancel()

	if _, err := a.auth.Register(ctx, &authpb.RegisterRequest{Email: body.Email, Password: body.Password}); err != nil {
		a.createFailed(r.Context(), w, "register", err)

		return
	}

	w.WriteHeader(http.StatusCreated)
}

// POST /accounts: only an authenticated Operator may create an account with a chosen role. The check is here because Auth's CreateAccount trusts its caller. The JWT middleware already handled a missing or invalid token.
func (a *authAPI) createAccount(w http.ResponseWriter, r *http.Request) {
	if claims, ok := claimsFrom(r.Context()); !ok || claims.Role != roleOperator {
		writeJSON(w, http.StatusForbidden, errBody("forbidden"))

		return
	}

	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}

	if !decodeBody(w, r, &body) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.timeout)
	defer cancel()

	req := &authpb.CreateAccountRequest{Email: body.Email, Password: body.Password, Role: body.Role}
	if _, err := a.auth.CreateAccount(ctx, req); err != nil {
		a.createFailed(r.Context(), w, "create account", err)

		return
	}

	w.WriteHeader(http.StatusCreated)
}

// Reads a small JSON body into v, answering 400 itself when it can't.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxAuthBody)).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid request body"))

		return false
	}

	return true
}

// Tells the caller who the verified session is (email and role from the JWT), so a browser that can't read its HttpOnly cookies can show only what its role may use. Role checks are still enforced server-side.
func me(w http.ResponseWriter, r *http.Request) {
	claims, ok := claimsFrom(r.Context())
	if !ok {
		writeJSON(w, http.StatusUnauthorized, errBody("unauthorized"))

		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"email": claims.Email, "role": claims.Role})
}

// Answers a failed account creation: a taken email is 409, a value Auth refuses (empty or oversized input, unknown role) is 400, anything else is the usual 502/504.
func (a *authAPI) createFailed(ctx context.Context, w http.ResponseWriter, op string, err error) {
	switch {
	case status.Code(err) == codes.AlreadyExists:
		writeJSON(w, http.StatusConflict, errBody("email already registered"))
	case status.Code(err) == codes.InvalidArgument:
		writeJSON(w, http.StatusBadRequest, errBody("invalid email, password or role"))
	default:
		a.fail(ctx, w, op, err)
	}
}
