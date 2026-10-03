// Command auth-service is a scaffold for the auth-service service.
// See doc/TRD_M1.md for the full specification this service must implement.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"google.golang.org/grpc"

	"github.com/brii26/distributed-hazard-coordination/services/auth-service/authpb"
)

const (
	defaultPort     = "8092"
	defaultGRPCPort = "9092"
	defaultTTLSecs  = 60
)

var portPattern = regexp.MustCompile(`^[0-9]{1,5}$`)

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		log.Printf("failed to write health response: %v", err)
	}
}

func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	// TODO: implement the endpoints specified for auth-service in doc/TRD_M1.md
	return mux
}

// resolvePort validates a raw port value (as read from the PORT env var),
// falling back to fallback when raw is empty.
func resolvePort(raw, fallback string) (string, error) {
	if raw == "" {
		raw = fallback
	}

	if !portPattern.MatchString(raw) {
		return "", fmt.Errorf("invalid PORT value: %q", raw)
	}

	return raw, nil
}

// startGRPC serves the internal Auth RPCs on :port in the background.
func startGRPC(port string, srv *authServer) error {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", ":"+port)
	if err != nil {
		return err
	}

	g := grpc.NewServer()
	authpb.RegisterAuthServer(g, srv)

	go func() {
		log.Printf("auth-service gRPC listening on %s", lis.Addr())

		if err := g.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	return nil
}

func newAuthServer() (*authServer, error) {
	dbURL, secret, salt := os.Getenv("USER_DB_URL"), os.Getenv("JWT_SIGNING_SECRET"), os.Getenv("SECRET_SALT")
	if dbURL == "" || secret == "" || salt == "" {
		return nil, errors.New("USER_DB_URL, JWT_SIGNING_SECRET and SECRET_SALT must be set")
	}

	ttl := defaultTTLSecs

	if raw := os.Getenv("ACCESS_TOKEN_TTL_S"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid ACCESS_TOKEN_TTL_S value: %q", raw)
		}

		ttl = n
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		return nil, err
	}

	return &authServer{
		store:       pgStore{db: db},
		salt:        salt,
		secret:      []byte(secret),
		lapanganTTL: time.Duration(ttl) * time.Second,
		now:         time.Now,
	}, nil
}

func main() {
	grpcPort, err := resolvePort(os.Getenv("GRPC_PORT"), defaultGRPCPort)
	if err != nil {
		log.Fatal(err)
	}

	auth, err := newAuthServer()
	if err != nil {
		log.Fatal(err)
	}

	if err := startGRPC(grpcPort, auth); err != nil {
		log.Fatal(err)
	}

	port, err := resolvePort(os.Getenv("PORT"), defaultPort)
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
		Handler:           newMux(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("auth-service listening on :%d", portNum)

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
