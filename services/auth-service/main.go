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

const dbPingTimeout = 2 * time.Second

// healthHandler serves GET /health: liveness plus User DB connectivity. The service
// can do nothing without its DB, so an unreachable one is "down" and a 503
// (doc/TRD_M1.md 5).
func healthHandler(ping func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status, db, code := "ok", "connected", http.StatusOK

		if err := ping(r.Context()); err != nil {
			status, db, code = "down", "disconnected", http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)

		if err := json.NewEncoder(w).Encode(map[string]string{"status": status, "database": db}); err != nil {
			log.Printf("failed to write health response: %v", err)
		}
	}
}

func newMux(ping func(context.Context) error) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler(ping))

	return mux
}

// resolvePort validates a raw port value (as read from the HEALTH_PORT or GRPC_PORT env var),
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

	ping := func(ctx context.Context) (err error) {
		start := time.Now()
		defer func() { logCall(ctx, "user-db ping", start, err) }()

		ctx, cancel := context.WithTimeout(ctx, dbPingTimeout)
		defer cancel()

		return db.PingContext(ctx)
	}

	return &authServer{
		ping:        ping,
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

	port, err := resolvePort(os.Getenv("HEALTH_PORT"), defaultPort)
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
		Handler:           newMux(auth.ping),
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
