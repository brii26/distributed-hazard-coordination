// Command aggregator polls BMKG and PVMBG, stores to the Canonical Store and publishes to the broker.
package main

import (
	"context"
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

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
)

const (
	defaultPort     = "8091"
	defaultGRPCPort = "9091"
)

var portPattern = regexp.MustCompile(`^[0-9]{1,5}$`)

// REST side on its own port, apart from gRPC, so the healthcheck never hits the concurrency cap.
func newMux(api *readAPI) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", api.healthHandler)

	return mux
}

// Validates a port from env, def if empty.
func resolvePort(raw, def string) (string, error) {
	if raw == "" {
		raw = def
	}

	if !portPattern.MatchString(raw) {
		return "", fmt.Errorf("invalid port value: %q", raw)
	}

	return raw, nil
}

// Reads a positive integer env var, def if unset.
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

// Reads polling and circuit breaker settings from env, PVMBG credentials from the token manager.
func loadPollConfig(tokens *tokenManager) (pollConfig, error) {
	var firstErr error

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

// Opens the Canonical Store from DATABASE_URL. An unreachable database stops the service at start.
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

// Binds the gRPC read API on GRPC_PORT. A taken port stops the service at start.
func startReadAPI(db *store, pub *publisher, breakers map[string]*breaker) (*readAPI, *grpc.Server, net.Listener, error) {
	port, err := resolvePort(os.Getenv("GRPC_PORT"), defaultGRPCPort)
	if err != nil {
		return nil, nil, nil, err
	}

	maxInflight, err := envPositiveInt("MAX_INFLIGHT_REQUESTS", defaultMaxInflight)
	if err != nil {
		return nil, nil, nil, err
	}

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", ":"+port)
	if err != nil {
		return nil, nil, nil, err
	}

	log.Printf("aggregator read api listening on :%s", port) //nolint:gosec // port is validated by resolvePort

	rabbitUp := func(context.Context) error {
		if !pub.connected() {
			return errors.New("rabbitmq not connected")
		}

		return nil
	}

	api := newReadAPI(db, rabbitUp, breakers, maxInflight)

	return api, newGRPCServer(api), lis, nil
}

// Runs the Aggregator until it stops or fails to start.
func run() error { //nolint:gocyclo // start-up wiring, one step after another
	port, err := resolvePort(os.Getenv("HEALTH_PORT"), defaultPort)
	if err != nil {
		return err
	}

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

	_ = tokens.load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := setupTracing(ctx)
	if err != nil {
		return err
	}

	defer shutdownTracing()

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

	var wg sync.WaitGroup

	breakers := map[string]*breaker{}

	pubCtx, stopPub := context.WithCancel(context.WithoutCancel(ctx))
	defer stopPub()

	pub := newPublisher(dialAMQP(rabbitURL, exchange), defaultPublishBuffer)

	var pubWG sync.WaitGroup

	pubWG.Go(func() { pub.run(pubCtx) })

	for _, p := range newPipelines(pc, db.upsert, pub.publish) {
		breakers[p.poller.name] = p.poller.breaker

		wg.Go(func() { p.run(ctx) })
	}

	api, grpcSrv, grpcLis, err := startReadAPI(db, pub, breakers)
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
		Handler:           otelhttp.NewHandler(newMux(api), "http", otelhttp.WithFilter(func(r *http.Request) bool { return r.URL.Path != "/health" })),
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

	wg.Wait()
	stopPub()
	pubWG.Wait()
	log.Print("aggregator stopped")

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
