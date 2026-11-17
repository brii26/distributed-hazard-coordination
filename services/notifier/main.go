// Command notifier consumes HazardEvents from the fanout exchange and alerts the field teams once per (hazard_id, severity). Critical coastal alerts come from their own queue and are pushed over a server-sent event stream.
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
	"sync"
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

// Liveness, broker connection and whether the dedup store could be read. Without it the Notifier doesn't consume (it could only send duplicate alerts), so "down" (503). A lost broker is "degraded" and reconnects on its own.
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

	c := &consumer{url: url, exchange: envOr("RABBITMQ_EXCHANGE", defaultExchange), queue: envOr("QUEUE_NAME", defaultQueue)}
	cc := &consumer{url: url, queue: envOr("CRITICAL_QUEUE_NAME", defaultCriticalQueue), requeueWait: criticalRequeueWait}
	hub, metrics := newAlertHub(), newCriticalMetrics()
	done := make(chan struct{})

	dedup, err := openDedup(envOr("DEDUP_STORE_PATH", defaultDedupPath))
	if err != nil {
		log.Printf("dedup store unreadable, not consuming: %v", err)
		close(done)
	} else {
		defer func() { _ = dedup.close() }()

		c.handle = (&notifier{dedup: dedup, send: logAlert}).handle

		cc.handle = newCriticalAlerts(dedup, hub, metrics).handle

		var wg sync.WaitGroup

		wg.Go(func() { c.run(ctx) })
		wg.Go(func() { cc.run(ctx) })

		go func() {
			defer close(done)

			wg.Wait()
		}()
	}

	portNum, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	mux := newMux(dedup != nil, func() bool { return c.connected() && cc.connected() })
	mux.Handle("GET /alerts/stream", hub)
	mux.Handle("GET /metrics", metrics)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
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

	<-done

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
