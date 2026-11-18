// Command situation-summarizer consumes HazardEvents on its own queue and builds a short situation summary on a timer. It flags areas with an unusual event rate (z-score), asks an LLM to word the summary when AI_ENABLED=true and falls back to a rule based text otherwise. The alert and ingest paths never call it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	defaultPort     = "8097"
	defaultExchange = "hazard.events"
	defaultQueue    = "situation-summarizer"
)

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return def
}

// Reads a positive number from the environment, def when unset.
func envPositive(name string, def float64) (float64, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}

	f, err := strconv.ParseFloat(raw, 64)
	if err != nil || f <= 0 {
		return 0, fmt.Errorf("invalid %s value: %q", name, raw)
	}

	return f, nil
}

func loadConfig() (config, int, time.Duration, error) {
	vals := map[string]float64{}

	for name, def := range map[string]float64{
		"SUMMARY_INTERVAL_S": 30, "SUMMARY_WINDOW_S": 300, "SUMMARY_STALE_AFTER_S": 120, "LLM_TIMEOUT_MS": 3000,
		"LLM_CB_FAILURE_THRESHOLD": 3, "LLM_CB_OPEN_MS": 30000, "ANOMALY_BUCKET_S": 60, "ANOMALY_HISTORY_BUCKETS": 30,
		"ANOMALY_Z_THRESHOLD": 3, "ANOMALY_MIN_EVENTS": 5,
	} {
		v, err := envPositive(name, def)
		if err != nil {
			return config{}, 0, 0, err
		}

		vals[name] = v
	}

	sec := func(name string) time.Duration { return time.Duration(vals[name] * float64(time.Second)) }
	cfg := config{
		interval: sec("SUMMARY_INTERVAL_S"), span: sec("SUMMARY_WINDOW_S"), staleAfter: sec("SUMMARY_STALE_AFTER_S"),
		llmTimeout: time.Duration(vals["LLM_TIMEOUT_MS"]) * time.Millisecond, bucket: sec("ANOMALY_BUCKET_S"),
		history: int(vals["ANOMALY_HISTORY_BUCKETS"]), zThreshold: vals["ANOMALY_Z_THRESHOLD"], minEvents: vals["ANOMALY_MIN_EVENTS"],
	}

	if cfg.history < minHistory {
		return config{}, 0, 0, fmt.Errorf("ANOMALY_HISTORY_BUCKETS must be at least %d", minHistory)
	}

	return cfg, int(vals["LLM_CB_FAILURE_THRESHOLD"]), time.Duration(vals["LLM_CB_OPEN_MS"]) * time.Millisecond, nil
}

// The LLM client, or nil when the rule based summary is used: AI_ENABLED is not "true", or the URL or key is missing (an unset or "change-me" key counts as missing).
func newLLM(cfg config) llm {
	if os.Getenv("AI_ENABLED") != "true" {
		return nil
	}

	url, key := os.Getenv("LLM_API_URL"), os.Getenv("LLM_API_KEY")
	if url == "" || key == "" || strings.HasPrefix(key, "change-me") {
		slog.Warn("AI_ENABLED is true but LLM_API_URL or a real LLM_API_KEY is missing, using the rule based summary")

		return nil
	}

	return newHTTPLLM(url, envOr("LLM_MODEL", "default"), key, cfg.llmTimeout)
}

func newMux(s *summarizer, connected func() bool, consumed *atomic.Int64) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /summary", s.serveSummary)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		status, rabbit := "ok", "connected"
		if !connected() {
			status, rabbit = "degraded", "disconnected"
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status, "rabbitmq": rabbit, "ai": strconv.FormatBool(s.llm != nil)})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "# HELP situation_events_consumed_total HazardEvents read from the queue.\n# TYPE situation_events_consumed_total counter\nsituation_events_consumed_total %d\n", consumed.Load())
		s.writeMetrics(w)
	})

	return mux
}

func run() error {
	port := envOr("HEALTH_PORT", defaultPort)
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return fmt.Errorf("invalid HEALTH_PORT value: %q", port)
	}

	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		return errors.New("RABBITMQ_URL must be set")
	}

	cfg, threshold, openFor, err := loadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	win := newWindow(max(cfg.span, cfg.bucket*time.Duration(cfg.history+1)))
	sum := newSummarizer(cfg, win, newLLM(cfg), newBreaker(threshold, openFor))

	var consumed atomic.Int64

	c := &consumer{url: url, exchange: envOr("RABBITMQ_EXCHANGE", defaultExchange), queue: envOr("QUEUE_NAME", defaultQueue), handle: func(b []byte) {
		if win.add(b) {
			consumed.Add(1)
		}
	}}

	go c.run(ctx)
	go sum.run(ctx)

	srv := &http.Server{Addr: ":" + port, Handler: newMux(sum, c.connected, &consumed), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}

	go func() {
		<-ctx.Done()

		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = srv.Shutdown(shutdown)
	}()

	slog.Info("situation-summarizer listening", "port", port, "ai_enabled", sum.llm != nil)

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
