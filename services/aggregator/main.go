// Command aggregator polls BMKG, PVMBG and Sensor Pantai, stores to the Canonical Store and publishes to the broker.
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
	mux.HandleFunc("GET /internal/audit/events", api.auditEvents)
	mux.HandleFunc("GET /internal/audit/checksum", api.auditChecksum)
	mux.HandleFunc("POST /internal/admin/replay", api.adminReplay)

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
		interval:    ms("POLL_INTERVAL_MS", defaultPollInterval),
		timeout:     ms("POLL_TIMEOUT_MS", defaultPollTimeout),
		cbOpenFor:   ms("CB_OPEN_DURATION_MS", defaultCBOpenDuration),
		bmkgURL:     os.Getenv("BMKG_BASE_URL"),
		bmkgKey:     os.Getenv("BMKG_API_KEY"),
		pvmbgURL:    tokens.baseURL,
		token:       tokens.Token,
		tokenErr:    tokens.LastError,
		rejectToken: tokens.Reject,
		pantaiURL:   os.Getenv("PANTAI_BASE_URL"),
		pantaiToken: os.Getenv("PANTAI_AUTH_TOKEN"),
	}

	var err error
	if c.cbThreshold, err = envPositiveInt("CB_FAILURE_THRESHOLD", defaultCBFailureThreshold); err != nil {
		return pollConfig{}, err
	}

	if firstErr != nil {
		return pollConfig{}, firstErr
	}

	if c.sensor, err = loadSensorConfig(); err != nil {
		return pollConfig{}, err
	}

	if c.bmkgURL == "" || c.bmkgKey == "" {
		return pollConfig{}, errors.New("BMKG_BASE_URL and BMKG_API_KEY must be set")
	}

	// Sensor Pantai is optional until the sensor runs everywhere, but half a setting is a mistake.
	if (c.pantaiURL == "") != (c.pantaiToken == "") {
		return pollConfig{}, errors.New("PANTAI_BASE_URL and PANTAI_AUTH_TOKEN must be set together")
	}

	return c, nil
}

const (
	defaultDBPoolSize    = 20
	defaultCoordPoolSize = 5
)

// Opens the Canonical Store from DATABASE_URL and, when COORDINATION_DATABASE_URL differs, the coordination pool. An unreachable database stops the service at start.
func loadStore() (*store, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, errors.New("DATABASE_URL must be set")
	}

	poolSize, err := envPositiveInt("DB_POOL_SIZE", defaultDBPoolSize)
	if err != nil {
		return nil, err
	}

	st, err := openStore(url, poolSize)
	if err != nil {
		return nil, err
	}

	if err := st.openCoordination(os.Getenv("COORDINATION_DATABASE_URL"), url, defaultCoordPoolSize); err != nil {
		_ = st.close()

		return nil, err
	}

	return st, nil
}

// Picks where writes go. With STORE_SERVICE set, membership keeps a node list fresh every 2 s and writes go to two owners; without it, to the single DATABASE_URL store.
func chooseUpsert(ctx context.Context, db *store) (func(context.Context, HazardEvent) (bool, error), *cluster, error) {
	service := os.Getenv("STORE_SERVICE")
	if service == "" {
		return db.upsert, nil, nil
	}

	tmpl := os.Getenv("STORE_DSN_TEMPLATE")
	if tmpl == "" {
		return nil, nil, errors.New("STORE_DSN_TEMPLATE must be set when STORE_SERVICE is")
	}

	m := newMembership(dnsResolver(service), pgNodeName(tmpl), 30*time.Second)
	m.syncState = func(ctx context.Context, n *node) (map[int]bool, error) {
		ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
		defer cancel()

		return syncedShards(ctx, n.db)
	}

	m.refresh(ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				m.refresh(ctx)
			}
		}
	}()

	cl := newCluster(m.live)
	cl.isSynced = m.isSynced

	return cl.upsert, cl, nil
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
func run() error { //nolint:gocyclo,funlen // start-up wiring, one step after another
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

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Background work outlives the signal until the servers have drained, then it is cancelled and the leases are released.
	ctx, cancelWork := context.WithCancel(context.WithoutCancel(sigCtx))
	defer cancelWork()

	sc, err := loadShutdownConfig()
	if err != nil {
		return err
	}

	shutdownTracing, err := setupTracing(ctx)
	if err != nil {
		return err
	}

	defer shutdownTracing()

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

	if pc.pantaiURL != "" {
		pc.pantai = newSensorClient(pc, db)
	}

	upsert, cl, err := chooseUpsert(ctx, db)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	breakers := map[string]*breaker{}

	pubCtx, stopPub := context.WithCancel(context.WithoutCancel(ctx))
	defer stopPub()

	pub := newPublisher(dialAMQP(rabbitURL, exchange), defaultPublishBuffer)

	var pubWG sync.WaitGroup

	pubWG.Go(func() { pub.run(pubCtx) })

	criticalQueue := os.Getenv("CRITICAL_QUEUE_NAME")
	if criticalQueue == "" {
		criticalQueue = defaultCriticalQueue
	}

	relay := newCriticalRelay(pgOutbox{db: db.db}, dialCritical(rabbitURL, criticalQueue))

	pubWG.Go(func() { relay.run(pubCtx) })

	lc, err := loadLeaseConfig()
	if err != nil {
		return err
	}

	rc, err := loadRetryConfig()
	if err != nil {
		return err
	}

	worker := &retryWorker{q: db, upsert: upsert, publish: pub.publish, cfg: rc}

	wg.Go(func() { worker.run(ctx) })

	var electors []*elector

	for _, p := range newPipelines(pc, upsert, pub.publish) {
		breakers[p.poller.name] = p.poller.breaker
		p.ingest.fail, p.ingest.retry = db.insertFailed, rc

		p.ingest.stage = relay.stage
		p.lease = newElector(p.poller.name, lc, pgLease{db: db.coordDB()})

		if p.poller.name == sourcePVMBG {
			tokens.shared = pgTokenStore{db: db.coordDB(), source: sourcePVMBG}
			p.lead = tokens.lead
		}

		electors = append(electors, p.lease)
		p.poller.loadSince = func(ctx context.Context) (time.Time, error) { return db.lastOccurred(ctx, p.poller.name) }

		wg.Go(func() { p.run(ctx) })
	}

	var rb *rebalancer

	var rbLease *elector

	if cl != nil {
		interval, err := envDuration("STORE_REBALANCE_INTERVAL", defaultRebalanceInterval)
		if err != nil {
			return err
		}

		pause, err := envDuration("REBALANCE_PAGE_PAUSE", defaultPagePause)
		if err != nil {
			return err
		}

		del, err := strconv.ParseBool(os.Getenv("REBALANCE_DELETE"))
		if err != nil && os.Getenv("REBALANCE_DELETE") != "" {
			return fmt.Errorf("invalid REBALANCE_DELETE value: %q", os.Getenv("REBALANCE_DELETE"))
		}

		rb = newRebalancer(cl, pause, del)
		rbLease = newElector("rebalance", lc, pgLease{db: db.coordDB()})
		electors = append(electors, rbLease)

		wg.Go(func() { rbLease.run(ctx, func(wctx context.Context) { rb.run(wctx, interval) }) })
	}

	api, grpcSrv, grpcLis, err := startReadAPI(db, pub, breakers)
	if err != nil {
		return err
	}

	api.cl = cl
	api.auditTok = os.Getenv("AUDIT_TOKEN")

	if api.auditTok == "" {
		log.Print("WARNING: AUDIT_TOKEN is empty, every audit request will be refused")
	}

	api.adminTok = os.Getenv("ADMIN_TOKEN")

	if api.adminTok == "" {
		log.Print("WARNING: ADMIN_TOKEN is empty, every admin request will be refused")
	}

	batch, err := envPositiveInt("REPLAY_BATCH", replayDefaultBatch)
	if err != nil {
		return err
	}

	api.replay = &replayer{q: db, m: newMapper(), upsert: upsert, publish: pub.publish, batch: batch}

	go api.watchStore(ctx, storeWatchEvery)

	go func() {
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Printf("read api stopped: %v", err)
			stop()
		}
	}()

	mux := newMux(api)
	leaseMetrics := leaseMetricsHandler(electors)
	failed := &failedMetrics{counts: db.failedCounts, w: worker}
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		leaseMetrics(w, r)
		relay.writeMetrics(w)
		failed.write(r.Context(), w)
		api.replay.writeMetrics(w)

		if rb != nil {
			rb.writeMetrics(w, rbLease.isLeader.Load() == 1)
		}

		if pc.pantai != nil {
			pc.pantai.rejects.writeMetrics(w)
			pc.pantai.stats.writeMetrics(w)
		}
	})

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           otelhttp.NewHandler(mux, "http", otelhttp.WithFilter(func(r *http.Request) bool { return r.URL.Path != "/health" && r.URL.Path != "/metrics" })),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	drained := make(chan struct{})

	go func() {
		defer close(drained)

		<-sigCtx.Done()

		gracefulShutdown(sc, api, srv, grpcSrv, func() {
			cancelWork()
			wg.Wait()
		})
	}()

	log.Printf("aggregator listening on :%d", portNum)

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	<-drained
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
