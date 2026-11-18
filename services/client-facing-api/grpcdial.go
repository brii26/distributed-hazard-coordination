package main

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health" // registers the client side health check used by healthCheckConfig
)

// round_robin over every resolved address, skipping instances that report NOT_SERVING. Reads are safe to retry on UNAVAILABLE.
const aggregatorServiceConfig = `{
  "loadBalancingConfig": [{"round_robin": {}}],
  "healthCheckConfig": {"serviceName": ""},
  "methodConfig": [{
    "name": [{"service": "aggregator.v1.Aggregator"}],
    "retryPolicy": {"maxAttempts": 3, "initialBackoff": "0.05s", "maxBackoff": "0.5s", "backoffMultiplier": 2, "retryableStatusCodes": ["UNAVAILABLE"]}
  }]
}`

// Dial options for the Aggregator. With a dns:/// target (headless Service) every ready pod gets calls, a plain host:port keeps working.
func aggregatorDialOptions(addr string) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(aggregatorServiceConfig),
		grpc.WithUnaryInterceptor(logOutbound(addr)),
	}
}
