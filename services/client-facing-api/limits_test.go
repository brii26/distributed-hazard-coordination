package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// clock is a settable time source for the token bucket.
type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func TestBucketAllowsABurstThenRefillsAtTheRate(t *testing.T) {
	t.Parallel()

	c := &clock{t: testNow}
	l := newRateLimiter(5)
	l.now = c.now

	for i := range 5 {
		if ok, _, _ := l.take("a"); !ok {
			t.Fatalf("request %d of the burst was refused", i+1)
		}
	}

	ok, _, retryAfter := l.take("a")
	if ok || retryAfter != 1 {
		t.Fatalf("the 6th request in the same instant must be refused with Retry-After 1, got ok=%v retry=%d", ok, retryAfter)
	}

	c.t = c.t.Add(200 * time.Millisecond) // one token at 5 per second

	if ok, _, _ := l.take("a"); !ok {
		t.Fatal("a token must be back after 1/rps seconds")
	}

	if ok, _, _ := l.take("a"); ok {
		t.Fatal("only one token came back")
	}
}

func TestOneClientHammeringDoesNotStarveAnother(t *testing.T) {
	t.Parallel()

	c := &clock{t: testNow}
	l := newRateLimiter(3)
	l.now = c.now

	for range 100 {
		l.take("hammer")
	}

	if ok, _, _ := l.take("hammer"); ok {
		t.Fatal("the hammering client must be limited")
	}

	for i := range 3 {
		if ok, _, _ := l.take("quiet"); !ok {
			t.Fatalf("another client's request %d was refused: buckets must be per client", i+1)
		}
	}
}

func TestIdleBucketsAreEvictedWhenThereAreMany(t *testing.T) {
	t.Parallel()

	c := &clock{t: testNow}
	l := newRateLimiter(1)
	l.now = c.now

	for i := range maxBuckets {
		l.take(fmt.Sprintf("old-%d", i))
	}

	c.t = c.t.Add(2 * bucketIdleTime)

	l.take("new")

	if len(l.buckets) != 1 {
		t.Fatalf("expected the idle buckets evicted, %d left", len(l.buckets))
	}
}

// protected serves an always-200 route behind the JWT middleware with a limiter of rps.
func protected(rps int) http.Handler {
	v := newVerifier()
	v.limiter = newRateLimiter(rps)
	v.limiter.now = func() time.Time { return testNow }

	return v.require(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
}

func callAs(t *testing.T, h http.Handler, token string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/hazards", nil)
	req.AddCookie(sessionCookie(cookieJWT, token))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	return rec
}

func TestProtectedRoutesAreLimitedPerClient(t *testing.T) {
	t.Parallel()

	h := protected(2)

	hammer := sign(t, testSecret, hs256, jwtClaims{Email: "hammer@x.example", Role: roleMedia, Exp: testNow.Add(time.Minute).Unix()})
	other := sign(t, testSecret, hs256, jwtClaims{Email: "other@x.example", Role: roleMedia, Exp: testNow.Add(time.Minute).Unix()})

	for range 2 {
		if rec := callAs(t, h, hammer); rec.Code != http.StatusOK {
			t.Fatalf("within the rate: expected 200, got %d", rec.Code)
		}
	}

	rec := callAs(t, h, hammer)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("over the rate: expected 429 with Retry-After, got %d %v", rec.Code, rec.Header())
	}

	if rec := callAs(t, h, other); rec.Code != http.StatusOK {
		t.Fatalf("another client must be unaffected, got %d", rec.Code)
	}
}

func TestInflightCapRefusesAtOnceAndSparesHealth(t *testing.T) {
	t.Parallel()

	entered, release := make(chan struct{}, 2), make(chan struct{})

	h := capInflight(2, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			entered <- struct{}{}

			<-release
		}

		w.WriteHeader(http.StatusOK)
	}))

	serve := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))

		return rec
	}

	var wg sync.WaitGroup

	for range 2 {
		wg.Go(func() { serve("/hazards") })
	}

	<-entered
	<-entered // both slots are taken

	start := time.Now()

	rec := serve("/hazards")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" || time.Since(start) > 100*time.Millisecond {
		t.Fatalf("over the cap: expected an immediate 429 with Retry-After, got %d after %v", rec.Code, time.Since(start))
	}

	if rec := serve("/health"); rec.Code != http.StatusOK {
		t.Fatalf("/health must answer while the cap is full, got %d", rec.Code)
	}

	close(release)
	wg.Wait()

	if rec := serve("/hazards"); rec.Code != http.StatusOK {
		t.Fatalf("a slot must be free again, got %d", rec.Code)
	}
}

// TestManyConcurrentClientsGetOnlySuccessOr429 is a small in-process load check:
// far more concurrent requests than the cap, from many clients, and every answer
// is either served or a controlled 429, never a 5xx or a hang.
func TestManyConcurrentClientsGetOnlySuccessOr429(t *testing.T) {
	t.Parallel()

	v := newVerifier()
	v.limiter = newRateLimiter(50)
	v.limiter.now = func() time.Time { return testNow }

	slow := v.require(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	h := capInflight(20, slow)

	tokens := make([]string, 50)
	for i := range tokens {
		tokens[i] = sign(t, testSecret, hs256, jwtClaims{Email: fmt.Sprintf("client-%d@x.example", i), Role: roleLapangan, Exp: testNow.Add(time.Minute).Unix()})
	}

	var ok, limited, other atomic.Int64

	var wg sync.WaitGroup

	for i := range 50 {
		wg.Go(func() {
			for range 40 {
				switch callAs(t, h, tokens[i]).Code {
				case http.StatusOK:
					ok.Add(1)
				case http.StatusTooManyRequests:
					limited.Add(1)
				default:
					other.Add(1)
				}
			}
		})
	}

	wg.Wait()

	if other.Load() != 0 || ok.Load() == 0 {
		t.Fatalf("expected only 200s and 429s, got %d ok, %d limited, %d other", ok.Load(), limited.Load(), other.Load())
	}
}
