package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
)

// jwtHeader is the fixed HS256 header from doc/TRD_M1.md section 4.1.
const jwtHeader = `{"alg":"HS256","typ":"JWT"}`

type jwtClaims struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Role  string `json:"role"`
	Iat   int64  `json:"iat"`
	Exp   int64  `json:"exp"`
}

var b64 = base64.RawURLEncoding

// signJWT returns Base64(header).Base64(payload).Base64(HMAC-SHA256(secret, first two parts)).
func signJWT(secret []byte, c jwtClaims) (string, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return "", err
	}

	signing := b64.EncodeToString([]byte(jwtHeader)) + "." + b64.EncodeToString(payload)

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signing))

	return signing + "." + b64.EncodeToString(mac.Sum(nil)), nil
}
