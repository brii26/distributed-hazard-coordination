// Command dashboard-updater consumes HazardEvents from the fanout exchange and keeps the internal dashboard's in-memory view.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"
)

const (
	defaultPort     = "8093"
	defaultExchange = "hazard.events"
	defaultQueue    = "dashboard-updater"
)

var portPattern = regexp.MustCompile(`^[0-9]{1,5}$`)

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("failed to write response: %v", err)
	}
}

// Liveness plus the broker connection. Without the broker it still serves its last known state, so "degraded" and still 200.
func healthHandler(connected func() bool) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		status, rabbit := "ok", "connected"
		if !connected() {
			status, rabbit = "degraded", "disconnected"
		}

		writeJSON(w, http.StatusOK, map[string]string{"status": status, "rabbitmq": rabbit})
	}
}

func newMux(s *state, connected func() bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler(connected))
	mux.HandleFunc("GET /state", func(w http.ResponseWriter, _ *http.Request) {
		events := s.snapshot()
		writeJSON(w, http.StatusOK, map[string]any{"count": len(events), "events": events})
	})
	mux.HandleFunc("GET /stream", s.hub.stream)

	return mux
}

// Validates a port from HEALTH_PORT, defaultPort if empty.
func resolvePort(raw string) (string, error) {
	if raw == "" {
		raw = defaultPort
	}

	if !portPattern.MatchString(raw) {
		return "", fmt.Errorf("invalid HEALTH_PORT value: %q", raw)
	}

	return raw, nil
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return def
}

func run() error {
	port, err := resolvePort(os.Getenv("HEALTH_PORT"))
	if err != nil {
		return err
	}

	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		return errors.New("RABBITMQ_URL must be set")
	}

	s := newState()
	c := &consumer{url: url, exchange: envOr("RABBITMQ_EXCHANGE", defaultExchange), queue: envOr("QUEUE_NAME", defaultQueue), handle: s.handle}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := setupTracing(ctx)
	if err != nil {
		return err
	}

	defer func() {
		flush, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = shutdownTracing(flush)
	}()

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.run(ctx)
	}()

	portNum, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           newMux(s, c.connected),
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
	}()

	log.Printf("dashboard-updater listening on :%d", portNum)

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	<-done

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
