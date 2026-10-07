// Command bmkg-mock is the mock BMKG source API: seismic events and tsunami warnings.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const (
	defaultPort       = "8081"
	defaultDelayMinMS = 50
	defaultDelayMaxMS = 150
)

var portPattern = regexp.MustCompile(`^[0-9]{1,5}$`)

func requireAPIKey(expectedKey string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-BMKG-Key")
		if key == "" {
			logFrom(r.Context()).Warn("api key rejected", "reason", "missing X-BMKG-Key")
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		if key != expectedKey {
			logFrom(r.Context()).Warn("api key rejected", "reason", "invalid X-BMKG-Key")
			w.WriteHeader(http.StatusForbidden)

			return
		}

		logFrom(r.Context()).Info("api key accepted")
		next(w, r)
	}
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		log.Printf("failed to write health response: %v", err)
	}
}

// Reads the optional ?since=<ISO8601> filter, absent means all records.
func parseSince(r *http.Request) (time.Time, error) {
	raw := r.URL.Query().Get("since")
	if raw == "" {
		return time.Time{}, nil
	}

	return time.Parse(time.RFC3339, raw)
}

// Serves the records get returns for the request's since filter.
func sinceHandler[T any](get func(time.Time) []T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		since, err := parseSince(r)
		if err != nil {
			http.Error(w, "invalid since: want ISO8601 (RFC 3339)", http.StatusBadRequest)

			return
		}

		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(get(since)); err != nil {
			log.Printf("failed to write response: %v", err)
		}
	}
}

// Sleeps a random time in [minDelay, maxDelay] before next, so BMKG answers with its low, consistent latency. A client that disconnects mid delay frees the handler at once.
func withDelay(minDelay, maxDelay time.Duration, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		delay := minDelay + time.Duration(rand.Int64N(int64(maxDelay-minDelay)+1)) //nolint:gosec // mock latency, not security-sensitive
		logFrom(r.Context()).Info("delay injected", "delay_ms", delay.Milliseconds())

		select {
		case <-r.Context().Done():
			return
		case <-time.After(delay):
		}

		next(w, r)
	}
}

// Wires the routes. Data endpoints check the API key first, so rejected requests fail fast and only served ones pay the delay. /health isn't delayed.
func newMux(apiKey string, store *eventStore, logger *slog.Logger, minDelay, maxDelay time.Duration) http.Handler {
	data := func(h http.HandlerFunc) http.HandlerFunc {
		return requireAPIKey(apiKey, withDelay(minDelay, maxDelay, h))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("GET /seismic-events", data(sinceHandler(store.eventsSince)))
	mux.HandleFunc("GET /tsunami-warnings", data(sinceHandler(store.warningsSince)))

	return withRequestLog(logger, mux)
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

func main() {
	logger := newLogger(os.Stdout)
	slog.SetDefault(logger.With("correlation_id", "-"))

	port, err := resolvePort(os.Getenv("PORT"))
	if err != nil {
		log.Fatal(err)
	}

	shutdownTracing, err := setupTracing(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	apiKey := os.Getenv("BMKG_API_KEY")

	if len(apiKey) == 0 {
		log.Fatal("API key is not provided")
	}

	portNum, err := strconv.Atoi(port)
	if err != nil {
		log.Fatal(err)
	}

	store := &eventStore{}

	seedCount, err := envPositiveInt("SEED_COUNT", defaultSeedCount)
	if err != nil {
		log.Fatal(err)
	}

	intervalS, err := envPositiveInt("EVENT_INTERVAL_S", int(defaultEventInterval/time.Second))
	if err != nil {
		log.Fatal(err)
	}

	delayMinMS, err := envPositiveInt("BMKG_DELAY_MIN_MS", defaultDelayMinMS)
	if err != nil {
		log.Fatal(err)
	}

	delayMaxMS, err := envPositiveInt("BMKG_DELAY_MAX_MS", defaultDelayMaxMS)
	if err != nil {
		log.Fatal(err)
	}

	if delayMinMS > delayMaxMS {
		log.Fatalf("BMKG_DELAY_MIN_MS (%d) must not exceed BMKG_DELAY_MAX_MS (%d)", delayMinMS, delayMaxMS)
	}

	interval := time.Duration(intervalS) * time.Second
	store.seed(seedCount, interval, time.Now())

	go store.run(context.Background(), interval)

	srv := &http.Server{
		Addr: ":" + port,
		Handler: otelhttp.NewHandler(newMux(apiKey, store, logger,
			time.Duration(delayMinMS)*time.Millisecond, time.Duration(delayMaxMS)*time.Millisecond), "http", otelhttp.WithFilter(func(r *http.Request) bool { return r.URL.Path != "/health" })),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("bmkg-mock listening on :%d", portNum)

	err = srv.ListenAndServe()
	_ = shutdownTracing(context.Background())

	log.Fatal(err)
}
