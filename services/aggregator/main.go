// Command aggregator is a scaffold for the aggregator service.
// See doc/TRD_M1.md for the full specification this service must implement.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
)

const (
	defaultPort     = "8091"
	defaultGRPCPort = "9091"
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
	// TODO: implement the endpoints specified for aggregator in doc/TRD_M1.md
	return mux
}

// resolvePort validates a raw port value (as read from the PORT or GRPC_PORT env
// var), falling back to def when raw is empty.
func resolvePort(raw, def string) (string, error) {
	if raw == "" {
		raw = def
	}

	if !portPattern.MatchString(raw) {
		return "", fmt.Errorf("invalid port value: %q", raw)
	}

	return raw, nil
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

// loadPollConfig reads the polling and circuit breaker settings from the
// environment and takes the PVMBG credentials from the token manager.
func loadPollConfig(tokens *tokenManager) (pollConfig, error) {
	var firstErr error

	// ms reads a positive millisecond env var as a Duration, remembering the first error.
	ms := func(name string, def time.Duration) time.Duration {
		n, err := envPositiveInt(name, int(def/time.Millisecond))
		if err != nil && firstErr == nil {
			firstErr = err
		}

		return time.Duration(n) * time.Millisecond
	}

	c := pollConfig{
		interval:  ms("POLL_INTERVAL_MS", defaultPollInterval),
		timeout:   ms("POLL_TIMEOUT_MS", defaultPollTimeout),
		cbOpenFor: ms("CB_OPEN_DURATION_MS", defaultCBOpenDuration),
		bmkgURL:   os.Getenv("BMKG_BASE_URL"),
		bmkgKey:   os.Getenv("BMKG_API_KEY"),
		pvmbgURL:  tokens.baseURL,
		token:     tokens.Token,
		tokenErr:  tokens.LastError,
	}

	var err error
	if c.cbThreshold, err = envPositiveInt("CB_FAILURE_THRESHOLD", defaultCBFailureThreshold); err != nil {
		return pollConfig{}, err
	}

	if firstErr != nil {
		return pollConfig{}, firstErr
	}

	if c.bmkgURL == "" || c.bmkgKey == "" {
		return pollConfig{}, errors.New("BMKG_BASE_URL and BMKG_API_KEY must be set")
	}

	return c, nil
}

const defaultDBPoolSize = 20

// loadStore opens the Canonical Store from DATABASE_URL with DB_POOL_SIZE connections.
// An unreachable database stops the service at start; compose waits for it to be healthy.
func loadStore() (*store, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, errors.New("DATABASE_URL must be set")
	}

	poolSize, err := envPositiveInt("DB_POOL_SIZE", defaultDBPoolSize)
	if err != nil {
		return nil, err
	}

	return openStore(url, poolSize)
}

// startReadAPI binds the gRPC read API on GRPC_PORT, so a taken port stops the
// service at start. MAX_INFLIGHT_REQUESTS caps its concurrent requests.
func startReadAPI(db *store, pub *publisher, breakers map[string]*breaker) (*grpc.Server, net.Listener, error) {
	port, err := resolvePort(os.Getenv("GRPC_PORT"), defaultGRPCPort)
	if err != nil {
		return nil, nil, err
	}

	maxInflight, err := envPositiveInt("MAX_INFLIGHT_REQUESTS", defaultMaxInflight)
	if err != nil {
		return nil, nil, err
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return nil, nil, err
	}

	log.Printf("aggregator read api listening on :%s", port)

	rabbitUp := func(context.Context) error {
		if !pub.connected() {
			return errors.New("rabbitmq not connected")
		}

		return nil
	}

	return newGRPCServer(newReadAPI(db, rabbitUp, breakers, maxInflight)), lis, nil
}

// run starts the Aggregator and returns when it has stopped or failed to start.
func run() error {
	port, err := resolvePort(os.Getenv("PORT"), defaultPort)
	if err != nil {
		return err
	}

	// Reparsed as an int purely so the log line below carries a numeric
	// argument instead of a string derived from environment input.
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	tokens := newTokenManager(
		os.Getenv("PVMBG_BASE_URL"),
		os.Getenv("PVMBG_OAUTH_CLIENT_ID"),
		os.Getenv("PVMBG_OAUTH_CLIENT_SECRET"),
		os.Getenv("PVMBG_ONE_TIME_CODE"),
		os.Getenv("PVMBG_TOKEN_VOLUME_PATH"),
	)

	if tokens.baseURL == "" || tokens.clientID == "" || tokens.clientSecret == "" || tokens.path == "" {
		return errors.New("PVMBG_BASE_URL, PVMBG_OAUTH_CLIENT_ID, PVMBG_OAUTH_CLIENT_SECRET and PVMBG_TOKEN_VOLUME_PATH must be set")
	}

	// A failed bootstrap must not stop the service (BMKG still works): run()
	// keeps retrying and the failure shows up through LastError().
	_ = tokens.load()

	// On SIGINT or SIGTERM the pollers stop and ingest writes what it holds.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go tokens.run(ctx)

	pc, err := loadPollConfig(tokens)
	if err != nil {
		return err
	}

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		return errors.New("RABBITMQ_URL must be set")
	}

	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	if exchange == "" {
		exchange = defaultExchange
	}

	db, err := loadStore()
	if err != nil {
		return err
	}

	defer func() { _ = db.close() }()

	// One poller and one ingest goroutine per source; both share the one pool.
	var wg sync.WaitGroup

	breakers := map[string]*breaker{}

	// The publisher outlives the pipelines, so events they hand over while draining
	// at shutdown still get their flush attempt.
	pubCtx, stopPub := context.WithCancel(context.WithoutCancel(ctx))
	defer stopPub()

	pub := newPublisher(dialAMQP(rabbitURL, exchange), defaultPublishBuffer)

	var pubWG sync.WaitGroup

	pubWG.Go(func() { pub.run(pubCtx) })

	for _, p := range newPipelines(pc, db.upsert, pub.publish) {
		breakers[p.poller.name] = p.poller.breaker

		wg.Go(func() { p.run(ctx) })
	}

	grpcSrv, grpcLis, err := startReadAPI(db, pub, breakers)
	if err != nil {
		return err
	}

	go func() {
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Printf("read api stopped: %v", err)
			stop()
		}
	}()

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           newMux(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		<-ctx.Done()

		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = srv.Shutdown(shutdown)

		grpcSrv.GracefulStop()
	}()

	log.Printf("aggregator listening on :%d", portNum)

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	wg.Wait() // the pipelines have drained
	stopPub()
	pubWG.Wait() // the publisher has flushed what it could
	log.Print("aggregator stopped")

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
