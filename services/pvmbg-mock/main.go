// Command pvmbg-mock is the mock PVMBG source API: OAuth token endpoint and volcanic reports.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const defaultPort = "8082"

var portPattern = regexp.MustCompile(`^[0-9]{1,5}$`)

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		log.Printf("failed to write health response: %v", err)
	}
}

// Makes /volcanic-reports fail fast with 503 (no hang, no auto-recovery). /health still says ok, only the simulated upstream is down.
var outageActive atomic.Bool

func outageHandler(w http.ResponseWriter, _ *http.Request) {
	active := !outageActive.Load()
	outageActive.Store(active)

	log.Printf("outage toggled: %t -> %t at %s", !active, active, time.Now().UTC().Format(time.RFC3339))

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(map[string]bool{"outage_active": active}); err != nil {
		log.Printf("failed to write outage response: %v", err)
	}
}

func withOutage(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if outageActive.Load() && r.URL.Path == "/volcanic-reports" {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)

			return
		}

		next.ServeHTTP(w, r)
	})
}

// Read by the /volcanic-reports generator and handler to decide whether reports carry confidence_level.
var schemaExtended atomic.Bool

func schemaVersionHandler(w http.ResponseWriter, _ *http.Request) {
	extended := !schemaExtended.Load()
	schemaExtended.Store(extended)
	version := schemaVersion(extended)

	log.Printf("schema-version toggled: %s -> %s at %s",
		schemaVersion(!extended), version, time.Now().UTC().Format(time.RFC3339))

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(map[string]string{"schema_version": version}); err != nil {
		log.Printf("failed to write schema-version response: %v", err)
	}
}

func schemaVersion(extended bool) string {
	if extended {
		return "extended"
	}

	return "legacy"
}

// Guards /admin/* with the X-Admin-Key header: missing is 401, wrong is 403.
func requireAdminKey(adminKey string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Admin-Key")
		if key == "" {
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		if !equal(key, adminKey) {
			w.WriteHeader(http.StatusForbidden)

			return
		}

		next(w, r)
	}
}

func newMux(store *tokenStore, adminKey string, reports *reportStore, minDelay, maxDelay time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /oauth/token", store.tokenHandler)
	mux.HandleFunc("POST /admin/outage", requireAdminKey(adminKey, outageHandler))
	mux.HandleFunc("POST /admin/schema-version", requireAdminKey(adminKey, schemaVersionHandler))
	mux.HandleFunc("GET /volcanic-reports", requireBearer(store, reportsHandler(reports, minDelay, maxDelay)))

	return withRequestLog(withOutage(mux))
}

// Validates a port from PORT, defaultPort if empty.
func resolvePort(raw string) (string, error) {
	if raw == "" {
		raw = defaultPort
	}

	if !portPattern.MatchString(raw) {
		return "", fmt.Errorf("invalid PORT value: %q", raw)
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

func main() {
	clientID := os.Getenv("OAUTH_CLIENT_ID")
	clientSecret := os.Getenv("OAUTH_CLIENT_SECRET")
	oneTimeCode := os.Getenv("PVMBG_ONE_TIME_CODE")
	adminKey := os.Getenv("ADMIN_KEY")

	if clientID == "" || clientSecret == "" || oneTimeCode == "" || adminKey == "" {
		log.Fatal("OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, PVMBG_ONE_TIME_CODE and ADMIN_KEY must be set")
	}

	port, err := resolvePort(os.Getenv("PORT"))
	if err != nil {
		log.Fatal(err)
	}

	shutdownTracing, err := setupTracing(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	portNum, err := strconv.Atoi(port)
	if err != nil {
		log.Fatal(err)
	}

	store := &reportStore{}

	seedCount, err := envPositiveInt("SEED_COUNT", defaultSeedCount)
	if err != nil {
		log.Fatal(err)
	}

	intervalS, err := envPositiveInt("EVENT_INTERVAL_S", int(defaultEventInterval/time.Second))
	if err != nil {
		log.Fatal(err)
	}

	delayMinMS, err := envPositiveInt("PVMBG_DELAY_MIN_MS", defaultDelayMinMS)
	if err != nil {
		log.Fatal(err)
	}

	delayMaxMS, err := envPositiveInt("PVMBG_DELAY_MAX_MS", defaultDelayMaxMS)
	if err != nil {
		log.Fatal(err)
	}

	if delayMinMS > delayMaxMS {
		log.Fatalf("PVMBG_DELAY_MIN_MS (%d) must not exceed PVMBG_DELAY_MAX_MS (%d)", delayMinMS, delayMaxMS)
	}

	interval := time.Duration(intervalS) * time.Second
	store.seed(seedCount, interval, time.Now(), schemaExtended.Load())

	go store.run(context.Background(), interval, schemaExtended.Load)

	srv := &http.Server{
		Addr: ":" + port,
		Handler: otelhttp.NewHandler(newMux(newTokenStore(clientID, clientSecret, oneTimeCode), adminKey, store,
			time.Duration(delayMinMS)*time.Millisecond, time.Duration(delayMaxMS)*time.Millisecond), "http", otelhttp.WithFilter(func(r *http.Request) bool { return r.URL.Path != "/health" })),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("pvmbg-mock listening on :%d", portNum)

	err = srv.ListenAndServe()
	_ = shutdownTracing(context.Background())

	log.Fatal(err)
}
