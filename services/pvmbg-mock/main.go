// Command pvmbg-mock is a scaffold for the pvmbg-mock service.
// See doc/TRD_M1.md for the full specification this service must implement.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"
)

const defaultPort = "8082"

var portPattern = regexp.MustCompile(`^[0-9]{1,5}$`)

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		log.Printf("failed to write health response: %v", err)
	}
}

// outageActive makes /volcanic-reports fail fast with 503 (no hang, no
// auto-recovery). /health keeps reporting ok: the process is up, only the
// simulated upstream is down.
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

func newMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /admin/outage", outageHandler)
	// TODO: implement the endpoints specified for pvmbg-mock in doc/TRD_M1.md
	return withOutage(mux)
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

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           newMux(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("pvmbg-mock listening on :%d", portNum)

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
