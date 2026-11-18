package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"google.golang.org/grpc"
)

const (
	defaultDrainDelay      = 2 * time.Second
	defaultShutdownTimeout = 20 * time.Second
)

// Timing of a graceful stop. The pod's terminationGracePeriodSeconds must cover preStop + drainDelay + timeout + the lease release.
type shutdownConfig struct {
	drainDelay, timeout time.Duration
}

func loadShutdownConfig() (shutdownConfig, error) {
	d, err := envDuration("SHUTDOWN_DRAIN_DELAY", defaultDrainDelay)
	if err != nil {
		return shutdownConfig{}, err
	}

	t, err := envDuration("SHUTDOWN_TIMEOUT", defaultShutdownTimeout)
	if err != nil {
		return shutdownConfig{}, err
	}

	return shutdownConfig{drainDelay: d, timeout: t}, nil
}

// Marks the instance as draining: /health answers 503 and the gRPC health check reports NOT_SERVING, for good (the store watcher cannot flip it back).
func (a *readAPI) startDraining() {
	a.draining.Store(true)

	if a.hs != nil {
		a.hs.Shutdown()
	}
}

// Stops the instance in order: readiness off, a short wait so routing drops it, stop listening and finish in-flight HTTP and gRPC calls within the timeout,
// then releaseWork stops the background work and releases the leases.
func gracefulShutdown(cfg shutdownConfig, api *readAPI, srv *http.Server, grpcSrv *grpc.Server, releaseWork func()) {
	api.startDraining()
	log.Print("aggregator draining, readiness is off")

	time.Sleep(cfg.drainDelay)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	grpcDone := make(chan struct{})

	go func() {
		grpcSrv.GracefulStop()
		close(grpcDone)
	}()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
		_ = srv.Close()
	}

	select {
	case <-grpcDone:
	case <-ctx.Done():
		log.Print("grpc drain timed out, closing connections")
		grpcSrv.Stop()
		<-grpcDone
	}

	releaseWork()
}
