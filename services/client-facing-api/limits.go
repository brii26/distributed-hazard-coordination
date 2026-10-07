package main

import (
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	defaultMaxInflight = 100
	defaultRateRPS     = 50

	// Per client IP on login and register. A person typing never reaches it, a guessing or account-minting loop does.
	defaultAuthRPS = 10

	// ponytail: buckets of clients idle this long are dropped once there are many, so memory stays bounded. A busy client never loses its bucket.
	maxBuckets     = 10_000
	bucketIdleTime = time.Minute
)

// The controlled overload answer: 429 with a hint of when to come back.
func tooMany(w http.ResponseWriter, retryAfter int) {
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	writeJSON(w, http.StatusTooManyRequests, errBody("too many requests"))
}

// Global concurrency cap, a semaphore of max slots. A full cap refuses at once with 429, never queues. /health and the long-lived /hazards/stream (own cap) are exempt.
func capInflight(maxInflight int, next http.Handler) http.Handler {
	sem := make(chan struct{}, maxInflight)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" || r.URL.Path == "/hazards/stream" {
			next.ServeHTTP(w, r)

			return
		}

		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()

			next.ServeHTTP(w, r)
		default:
			logFor(r.Context()).Warn("rate-limit", "kind", "inflight", "path", r.URL.Path, "remaining", 0)
			tooMany(w, 1)
		}
	})
}

// Token bucket per client: rps tokens a second, burst up to one second's worth, so a client hammering the API only empties its own bucket.
type rateLimiter struct {
	rps float64
	now func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(rps int) *rateLimiter {
	return &rateLimiter{rps: float64(rps), now: time.Now, buckets: map[string]*bucket{}}
}

// Spends one token of client's bucket. When none is left returns false and the seconds until one is.
func (l *rateLimiter) take(client string) (ok bool, remaining float64, retryAfter int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()

	b := l.buckets[client]
	if b == nil {
		if len(l.buckets) >= maxBuckets {
			l.evictIdle(now)
		}

		b = &bucket{tokens: l.rps, last: now}
		l.buckets[client] = b
	}

	b.tokens = min(l.rps, b.tokens+now.Sub(b.last).Seconds()*l.rps)
	b.last = now

	if b.tokens < 1 {
		return false, b.tokens, max(1, int(math.Ceil((1-b.tokens)/l.rps)))
	}

	b.tokens--

	return true, b.tokens, 0
}

func (l *rateLimiter) evictIdle(now time.Time) {
	for k, b := range l.buckets {
		if now.Sub(b.last) > bucketIdleTime {
			delete(l.buckets, k)
		}
	}
}

// Limits next per client. Runs behind the JWT middleware, so the client is the token's identity. The JWT has no client_id claim, so the account email is the client_id, never the IP (load tests and NAT share it).
func (l *rateLimiter) wrap(next http.Handler) http.Handler {
	return l.by(func(r *http.Request) string {
		claims, _ := claimsFrom(r.Context())

		return claims.Email
	}, next)
}

// Limits the routes that run before any login (/auth/login, /auth/register), where there is no account to key on.
// ponytail: the key is the TCP peer, so browsers behind the web-frontend's nginx share one bucket. Trusting X-Forwarded-For would let a direct caller dodge it. Upgrade: nginx sets the real IP.
func (l *rateLimiter) byIP(next http.HandlerFunc) http.Handler {
	return l.by(func(r *http.Request) string {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			return r.RemoteAddr
		}

		return host
	}, next)
}

func (l *rateLimiter) by(key func(*http.Request) string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		client := key(r)

		ok, remaining, retryAfter := l.take(client)
		if !ok {
			logFor(r.Context()).Warn("rate-limit", "kind", "client", "client_id", client, "path", r.URL.Path, "remaining", remaining)
			tooMany(w, retryAfter)

			return
		}

		next.ServeHTTP(w, r)
	})
}
