// Command client-facing-api is a scaffold for the client-facing-api service.
// See doc/TRD_M1.md for the full specification this service must implement.
package main

import (
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

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/aggregatorpb"
	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/authpb"
)

const (
	defaultPort              = "8080"
	defaultUpstreamTimeoutMS = 1000
)

var portPattern = regexp.MustCompile(`^[0-9]{1,5}$`)

// newMux has the API routes; /health is added by main, since it needs the
// dependency addresses.
func newMux(auth *authAPI) *http.ServeMux {
	mux := http.NewServeMux()
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

	secret := os.Getenv("JWT_SIGNING_SECRET")
	if secret == "" {
		return nil, errors.New("JWT_SIGNING_SECRET must be set")
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(logOutbound(addr)))
	if err != nil {
		return nil, err
	}

	api := &authAPI{auth: authpb.NewAuthClient(conn), timeout: time.Duration(timeoutMS) * time.Millisecond, secure: secure}

	rps, err := envPositiveInt("RATE_LIMIT_RPS", defaultRateRPS)
	if err != nil {
		return nil, err
	}

	api.verifier = newJWTVerifier([]byte(secret)).withRefresh(api)
	api.verifier.limiter = newRateLimiter(rps)

	return api, nil
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

// newHealthAPI sets up the two checks behind /health.
func newHealthAPI() (healthAPI, error) {
	agg := os.Getenv("AGGREGATOR_GRPC_ADDR")
	if agg == "" {
		return healthAPI{}, errors.New("AGGREGATOR_GRPC_ADDR must be set")
	}

	timeoutMS, err := envPositiveInt("UPSTREAM_TIMEOUT_MS", defaultUpstreamTimeoutMS)
	if err != nil {
		return healthAPI{}, err
	}

	// Lazy like the Auth Service connection: the service starts while the Aggregator is still coming up.
	conn, err := grpc.NewClient(agg, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(logOutbound(agg)))
	if err != nil {
		return healthAPI{}, err
	}

	return healthAPI{
		aggregator: aggregatorpb.NewAggregatorClient(conn),
		authAddr:   os.Getenv("AUTH_SERVICE_GRPC_ADDR"),
		timeout:    time.Duration(timeoutMS) * time.Millisecond,
	}, nil
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

	health, err := newHealthAPI()
	if err != nil {
		log.Fatal(err)
	}

	mux := newMux(auth)
	mux.Handle("GET /health", health)

	maxInflight, err := envPositiveInt("MAX_INFLIGHT_REQUESTS", defaultMaxInflight)
	if err != nil {
		log.Fatal(err)
	}

	// /hazards shares the health check's Aggregator connection and timeout.
	mux.Handle("GET /hazards", auth.verifier.require(hazardsAPI{agg: health.aggregator, timeout: health.timeout}))

	// Reparsed as an int purely so the log line below carries a numeric
	// argument instead of a string derived from environment input.
	portNum, err := strconv.Atoi(port)
	if err != nil {
		log.Fatal(err)
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           withCorrelation(capInflight(maxInflight, mux)),
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
