// Command auth-service handles registration, login and token refresh against the User DB.
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

	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver with database/sql
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
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

// GET /health: liveness plus User DB connectivity. The service can do nothing without its DB, so unreachable is "down" (503).
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

// Validates a port from HEALTH_PORT or GRPC_PORT, fallback if empty.
func resolvePort(raw, fallback string) (string, error) {
	if raw == "" {
		raw = fallback
	}

	if !portPattern.MatchString(raw) {
		return "", fmt.Errorf("invalid PORT value: %q", raw)
	}

	return raw, nil
}

// Serves the internal Auth RPCs on :port in the background.
func startGRPC(port string, srv *authServer) error {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", ":"+port)
	if err != nil {
		return err
	}

	g := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	authpb.RegisterAuthServer(g, srv)

	go func() {
		log.Printf("auth-service gRPC listening on %s", lis.Addr())

		if err := g.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	return nil
}

// Refuses a seed Operator email that login would reject, since that Operator could never log in.
func checkSeedEmail(raw string) error {
	if raw != "" && !validEmail(normalizeEmail(raw)) {
		return fmt.Errorf("SEED_OPERATOR_EMAIL %q is not a valid email address", raw)
	}

	return nil
}

func newAuthServer() (*authServer, error) {
	dbURL, secret, salt := os.Getenv("USER_DB_URL"), os.Getenv("JWT_SIGNING_SECRET"), os.Getenv("SECRET_SALT")
	if dbURL == "" || secret == "" || salt == "" {
		return nil, errors.New("USER_DB_URL, JWT_SIGNING_SECRET and SECRET_SALT must be set")
	}

	if err := checkSeedEmail(os.Getenv("SEED_OPERATOR_EMAIL")); err != nil {
		return nil, err
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
	shutdownTracing, err := setupTracing(context.Background())
	if err != nil {
		log.Fatal(err)
	}

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

	portNum, err := strconv.Atoi(port)
	if err != nil {
		log.Fatal(err)
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           otelhttp.NewHandler(newMux(auth.ping), "http", otelhttp.WithFilter(func(r *http.Request) bool { return r.URL.Path != "/health" })),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("auth-service listening on :%d", portNum)

	err = srv.ListenAndServe()
	_ = shutdownTracing(context.Background())

	log.Fatal(err)
}
