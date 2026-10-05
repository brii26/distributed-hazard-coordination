// Command audit-logger consumes HazardEvents from the fanout exchange into an
// append-only log (doc/TRD_M1.md 8.3). It was added on top of the running system
// without any change to the Aggregator, the P5.4 demonstration.
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
	defaultPort      = "8095"
	defaultExchange  = "hazard.events"
	defaultQueue     = "audit-logger"
	defaultDedupPath = "/var/lib/audit-logger/dedup.log"
	defaultAuditPath = "/var/lib/audit-logger/audit.log"
)

var portPattern = regexp.MustCompile(`^[0-9]{1,5}$`)

// healthHandler is liveness, the broker connection and whether the dedup store
// could be read at start (doc/TRD_M1.md 8). Without its dedup store or its log
// the Audit Logger does not consume at all, so that is "down" and a 503; a lost
// broker is "degraded", it reconnects by itself.
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

	dedup, audit, err := openStores(envOr("DEDUP_STORE_PATH", defaultDedupPath), envOr("AUDIT_LOG_PATH", defaultAuditPath))
	if err != nil {
		// Stay up and report it, rather than crash-loop: /health shows the store is unreadable.
		log.Printf("dedup store or audit log unusable, not consuming: %v", err)
		close(done)
	} else {
		defer func() { _ = dedup.close(); _ = audit.close() }()

		c.handle = (&auditor{log: audit, dedup: dedup}).handle

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

	log.Printf("audit-logger listening on :%d", portNum)

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	<-done // unacked deliveries go back to the queue when the connection closes

	return nil
}

// openStores opens the dedup store and the audit log; both are needed to consume.
func openStores(dedupPath, auditPath string) (*dedupStore, *auditLog, error) {
	dedup, err := openDedup(dedupPath)
	if err != nil {
		return nil, nil, err
	}

	audit, err := openAuditLog(auditPath)
	if err != nil {
		_ = dedup.close()

		return nil, nil, err
	}

	return dedup, audit, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
