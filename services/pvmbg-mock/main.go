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

	body := map[string]any{"status": "ok", "outage_active": outageActive.Load()}

	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("failed to write health response: %v", err)
	}
}

// Makes /volcanic-reports fail fast with 503 (no hang, no auto-recovery). /health stays 200 and reports outage_active, only the simulated upstream is down.
var outageActive atomic.Bool

func outageHandler(w http.ResponseWriter, r *http.Request) {
	_, l := adminCall(r)
	active := !outageActive.Load()
	outageActive.Store(active)

	l.Info("outage toggled", "from", !active, "to", active)

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

// The schema the generator stamps on new reports. One atomic value, so a report never sees half a switch.
var schemaState atomic.Uint32

func currentSchema() schemaMode { return schemaMode(schemaState.Load() & 0xff) }

// Sets the schema from the body. Version 1 is legacy, 2 or more (so 3 too) incompatible, with all three changes unless "change" (rename, type or missing, trimmed, any case) picks one. A "change" we do not understand means all three and a warning. Without a version (empty body, or anything that is not a positive integer version) it toggles legacy and extended (confidence_level), and incompatible goes back to legacy. The mock never answers 4xx for an odd body.
func schemaVersionHandler(w http.ResponseWriter, r *http.Request) {
	raw, l := adminCall(r)

	var req struct {
		Version *int            `json:"version"`
		Change  json.RawMessage `json:"change"`
	}

	if json.Unmarshal(raw, &req) != nil {
		req.Version = nil // any body is fine, unknown means toggle
	}

	was := currentSchema()
	next := was.toggled()

	if req.Version != nil && *req.Version >= 1 {
		next = modeLegacy

		if *req.Version >= 2 {
			var warn string

			if next, warn = incompatibleMode(req.Change); warn != "" {
				l.Warn("schema-version "+warn, "change", string(req.Change))
			}
		}
	}

	if !next.incompatible() && req.Change != nil {
		l.Info("schema-version change ignored", "change", string(req.Change))
	}

	schemaState.Store(uint32(next))

	l.Info("schema-version changed", "version", req.Version, "from", was.name(), "to", next.name())

	w.Header().Set("Content-Type", "application/json")

	body := map[string]string{"schema_version": next.name()}
	if next.incompatible() {
		body["change"] = next.changeName()
	}

	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("failed to write schema-version response: %v", err)
	}
}

func newMux(store *tokenStore, reports *reportStore, minDelay, maxDelay time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /oauth/token", store.tokenHandler)
	mux.HandleFunc("POST /admin/outage", outageHandler)
	mux.HandleFunc("POST /admin/schema-version", schemaVersionHandler)
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

	if clientID == "" || clientSecret == "" || oneTimeCode == "" {
		log.Fatal("OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET and PVMBG_ONE_TIME_CODE must be set")
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

	store := &reportStore{startID: time.Now().UTC().Format("20060102T150405")}

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
	store.seed(seedCount, interval, time.Now(), currentSchema())

	go store.run(context.Background(), interval, currentSchema)

	srv := &http.Server{
		Addr: ":" + port,
		Handler: otelhttp.NewHandler(newMux(newTokenStore(clientID, clientSecret, oneTimeCode), store,
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
