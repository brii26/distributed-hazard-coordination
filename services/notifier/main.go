// Command notifier consumes HazardEvents from the fanout exchange and alerts the
// field teams once per (hazard_id, severity) (doc/TRD_M1.md 8.2).
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
	defaultPort      = "8094"
	defaultExchange  = "hazard.events"
	defaultQueue     = "notifier"
	defaultDedupPath = "/var/lib/notifier/dedup.log"
)

var portPattern = regexp.MustCompile(`^[0-9]{1,5}$`)

// healthHandler is liveness, the broker connection and whether the dedup store
// could be read at start (doc/TRD_M1.md 8). Without its dedup store the Notifier
// does not consume at all (it could only send duplicate alerts), so that is
// "down" and a 503; a lost broker is "degraded", it reconnects by itself.
func healthHandler(dedupReadable bool, connected func() bool) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		status, code, rabbit, store := "ok", http.StatusOK, "connected", "readable"

		if !connected() {
			status, rabbit = "degraded", "disconnected"
		}

		if !dedupReadable {
			status, code, store = "down", http.StatusServiceUnavailable, "unreadable"
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)

		if err := json.NewEncoder(w).Encode(map[string]string{"status": status, "rabbitmq": rabbit, "dedup_store": store}); err != nil {
			log.Printf("failed to write health response: %v", err)
		}
	}
}

func newMux(dedupReadable bool, connected func() bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler(dedupReadable, connected))

	return mux
}

// resolvePort validates a raw port value (as read from the HEALTH_PORT env var),
// falling back to defaultPort when raw is empty.
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := &consumer{url: url, exchange: envOr("RABBITMQ_EXCHANGE", defaultExchange), queue: envOr("QUEUE_NAME", defaultQueue)}
	done := make(chan struct{})

	dedup, err := openDedup(envOr("DEDUP_STORE_PATH", defaultDedupPath))
	if err != nil {
		// Stay up and report it, rather than crash-loop: /health shows the store is unreadable.
		log.Printf("dedup store unreadable, not consuming: %v", err)
		close(done)
	} else {
		defer func() { _ = dedup.close() }()

		c.handle = (&notifier{dedup: dedup, send: logAlert}).handle

		go func() {
			defer close(done)

			c.run(ctx)
		}()
	}

	// Reparsed as an int purely so the log line below carries a numeric
	// argument instead of a string derived from environment input.
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           newMux(dedup != nil, c.connected),
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

	log.Printf("notifier listening on :%d", portNum)

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	<-done // unacked deliveries go back to the queue when the connection closes

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
