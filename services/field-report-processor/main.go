package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/brii26/distributed-hazard-coordination/services/field-report-processor/aggregatorpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const roundRobin = `{"loadBalancingConfig":[{"round_robin":{}}]}`

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}

// The saver that calls the Aggregator. The correlation ID goes along as gRPC metadata.
func grpcSaver(c aggregatorpb.AggregatorClient) saver {
	return func(ctx context.Context, id string, body []byte, correlationID, parkReason string) (string, error) {
		if correlationID != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-correlation-id", correlationID)
		}

		resp, err := c.SaveFieldReport(ctx, &aggregatorpb.SaveFieldReportRequest{MessageId: id, Body: body, ParkReason: parkReason})
		if err != nil {
			return "", err
		}

		return resp.GetResult(), nil
	}
}

// Health and metrics on one small HTTP port. Health stays 200 while the broker is away and says degraded, so the pod is not restarted for a broker restart.
func newMux(c *consumer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		st := "ok"
		if !c.connected() {
			st = "degraded"
		}

		_ = json.NewEncoder(w).Encode(map[string]string{"status": st, "service": "field-report-processor"})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		p := c.proc
		_, _ = fmt.Fprintf(w, "# TYPE field_report_processed_total counter\n")

		for _, r := range []struct {
			name string
			n    int64
		}{{"stored", p.stored.Load()}, {"duplicate", p.duplicate.Load()}, {"parked", p.parked.Load()}, {"requeued", p.requeued.Load()}} {
			_, _ = fmt.Fprintf(w, "field_report_processed_total{result=%q} %d\n", r.name, r.n)
		}

		_, _ = fmt.Fprintf(w, "# TYPE field_report_process_seconds summary\nfield_report_process_seconds_sum %f\nfield_report_process_seconds_count %d\n",
			time.Duration(p.nanos.Load()).Seconds(), p.calls.Load())
	})

	return mux
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "field-report-processor"))

	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		log.Fatal("RABBITMQ_URL must be set")
	}

	conn, err := grpc.NewClient(env("AGGREGATOR_ADDR", "dns:///aggregator-grpc:9091"),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(roundRobin))
	if err != nil {
		log.Fatal(err)
	}

	defer func() { _ = conn.Close() }()

	c := &consumer{url: url, proc: newProcessor(grpcSaver(aggregatorpb.NewAggregatorClient(conn)))}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr: ":" + env("HEALTH_PORT", "8096"), Handler: newMux(c), ReadHeaderTimeout: 5 * time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Print(err)
			stop()
		}
	}()

	c.run(ctx)

	shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = srv.Shutdown(shut)
}
