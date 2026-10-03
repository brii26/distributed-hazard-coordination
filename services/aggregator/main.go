// Command aggregator is a scaffold for the aggregator service.
// See doc/TRD_M1.md for the full specification this service must implement.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"
)

const defaultPort = "8091"

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

func main() {
	port, err := resolvePort(os.Getenv("PORT"))
	if err != nil {
		log.Fatal(err)
	}

	// Reparsed as an int purely so the log line below carries a numeric
	// argument instead of a string derived from environment input.
	portNum, err := strconv.Atoi(port)
	if err != nil {
		log.Fatal(err)
	}

	tokens := newTokenManager(
		os.Getenv("PVMBG_BASE_URL"),
		os.Getenv("PVMBG_OAUTH_CLIENT_ID"),
		os.Getenv("PVMBG_OAUTH_CLIENT_SECRET"),
		os.Getenv("PVMBG_ONE_TIME_CODE"),
		os.Getenv("PVMBG_TOKEN_VOLUME_PATH"),
	)

	if tokens.baseURL == "" || tokens.clientID == "" || tokens.clientSecret == "" || tokens.path == "" {
		log.Fatal("PVMBG_BASE_URL, PVMBG_OAUTH_CLIENT_ID, PVMBG_OAUTH_CLIENT_SECRET and PVMBG_TOKEN_VOLUME_PATH must be set")
	}

	// A failed bootstrap must not stop the service (BMKG still works): run()
	// keeps retrying and the failure shows up through LastError().
	_ = tokens.load()

	ctx := context.Background()

	go tokens.run(ctx)

	pc, err := loadPollConfig(tokens)
	if err != nil {
		log.Fatal(err)
	}

	// One goroutine per source: a slow PVMBG poll never delays the BMKG loop.
	for _, p := range newPollers(pc) {
		go p.run(ctx)
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           newMux(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("aggregator listening on :%d", portNum)

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
