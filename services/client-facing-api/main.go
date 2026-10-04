// Command client-facing-api is a scaffold for the client-facing-api service.
// See doc/TRD_M1.md for the full specification this service must implement.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/authpb"
)

const (
	defaultPort              = "8080"
	defaultUpstreamTimeoutMS = 1000
)

var portPattern = regexp.MustCompile(`^[0-9]{1,5}$`)

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		log.Printf("failed to write health response: %v", err)
	}
}

func newMux(auth *authAPI) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	auth.register(mux)
	// TODO: implement the remaining endpoints specified for client-facing-api in doc/TRD_M1.md
	return mux
}

// envPositiveInt reads a positive integer env var, falling back to def when unset.
func envPositiveInt(name string, def int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid %s value: %q", name, raw)
	}

	return n, nil
}

// newAuthAPI dials the Auth Service (AUTH_SERVICE_GRPC_ADDR). The connection is
// lazy, so the service starts even while the Auth Service is still coming up.
func newAuthAPI() (*authAPI, error) {
	addr := os.Getenv("AUTH_SERVICE_GRPC_ADDR")
	if addr == "" {
		return nil, errors.New("AUTH_SERVICE_GRPC_ADDR must be set")
	}

	timeoutMS, err := envPositiveInt("UPSTREAM_TIMEOUT_MS", defaultUpstreamTimeoutMS)
	if err != nil {
		return nil, err
	}

	// Secure by default: the cookies carry the session. Set COOKIE_SECURE=false only
	// for a plain-HTTP demo on a host that is not localhost.
	secure := true

	if raw := os.Getenv("COOKIE_SECURE"); raw != "" {
		if secure, err = strconv.ParseBool(raw); err != nil {
			return nil, fmt.Errorf("invalid COOKIE_SECURE value: %q", raw)
		}
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &authAPI{auth: authpb.NewAuthClient(conn), timeout: time.Duration(timeoutMS) * time.Millisecond, secure: secure}, nil
}

// resolvePort validates a raw port value (as read from the PORT env var),
// falling back to defaultPort when raw is empty.
func resolvePort(raw string) (string, error) {
	if raw == "" {
		raw = defaultPort
	}

	if !portPattern.MatchString(raw) {
		return "", fmt.Errorf("invalid PORT value: %q", raw)
	}

	return raw, nil
}

func main() {
	port, err := resolvePort(os.Getenv("PORT"))
	if err != nil {
		log.Fatal(err)
	}

	auth, err := newAuthAPI()
	if err != nil {
		log.Fatal(err)
	}

	// Reparsed as an int purely so the log line below carries a numeric
	// argument instead of a string derived from environment input.
	portNum, err := strconv.Atoi(port)
	if err != nil {
		log.Fatal(err)
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           newMux(auth),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("client-facing-api listening on :%d", portNum)

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
