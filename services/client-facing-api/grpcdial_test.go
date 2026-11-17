package main

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/brii26/distributed-hazard-coordination/services/client-facing-api/aggregatorpb"
)

func TestServiceConfigIsRoundRobin(t *testing.T) {
	var cfg struct {
		LB []map[string]any `json:"loadBalancingConfig"`
	}

	if err := json.Unmarshal([]byte(aggregatorServiceConfig), &cfg); err != nil {
		t.Fatal(err)
	}

	if _, ok := cfg.LB[0]["round_robin"]; !ok {
		t.Fatalf("policy = %v, want round_robin", cfg.LB)
	}
}

type countingAggregator struct {
	aggregatorpb.UnimplementedAggregatorServer

	mu sync.Mutex
	n  int
}

func (c *countingAggregator) GetHealth(context.Context, *emptypb.Empty) (*aggregatorpb.HealthStatus, error) {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()

	return &aggregatorpb.HealthStatus{Status: "ok"}, nil
}

func (c *countingAggregator) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.n
}

func TestCallsReachAllReplicas(t *testing.T) {
	var (
		fakes []*countingAggregator
		addrs []resolver.Address
	)

	for range 3 {
		lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		f := &countingAggregator{}
		srv := grpc.NewServer()
		aggregatorpb.RegisterAggregatorServer(srv, f)

		go func() { _ = srv.Serve(lis) }()

		t.Cleanup(srv.Stop)

		fakes = append(fakes, f)
		addrs = append(addrs, resolver.Address{Addr: lis.Addr().String()})
	}

	// A manual resolver stands in for the DNS answer of the headless Service.
	r := manual.NewBuilderWithScheme("dns")
	r.InitialState(resolver.State{Addresses: addrs})

	conn, err := grpc.NewClient("dns:///aggregator-grpc:9091", append(aggregatorDialOptions("test"), grpc.WithResolvers(r))...)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	client := aggregatorpb.NewAggregatorClient(conn)

	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := client.GetHealth(ctx, &emptypb.Empty{}, grpc.WaitForReady(true))

		cancel()

		if err != nil {
			t.Fatal(err)
		}

		if fakes[0].calls() > 0 && fakes[1].calls() > 0 && fakes[2].calls() > 0 {
			return
		}
	}

	t.Fatalf("calls per replica = %d %d %d, want all above 0", fakes[0].calls(), fakes[1].calls(), fakes[2].calls())
}
