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

func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	// TODO: implement the endpoints specified for pvmbg-mock in doc/TRD_M1.md
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
