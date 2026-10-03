// Command aggregator is a scaffold for the aggregator service.
// See doc/TRD_M1.md for the full specification this service must implement.
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

	intervalMS, err := envPositiveInt("POLL_INTERVAL_MS", int(defaultPollInterval/time.Millisecond))
	if err != nil {
		log.Fatal(err)
	}

	timeoutMS, err := envPositiveInt("POLL_TIMEOUT_MS", int(defaultPollTimeout/time.Millisecond))
	if err != nil {
		log.Fatal(err)
	}

	bmkgURL, bmkgKey := os.Getenv("BMKG_BASE_URL"), os.Getenv("BMKG_API_KEY")
	if bmkgURL == "" || bmkgKey == "" {
		log.Fatal("BMKG_BASE_URL and BMKG_API_KEY must be set")
	}

	// One goroutine per source: a slow PVMBG poll never delays the BMKG loop.
	for _, p := range newPollers(
		time.Duration(intervalMS)*time.Millisecond, time.Duration(timeoutMS)*time.Millisecond,
		bmkgURL, bmkgKey, tokens.baseURL, tokens.Token,
	) {
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
