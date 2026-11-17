package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMediaFloodPreservesHighPriorityCapacity(t *testing.T) {
	t.Parallel()

	v := newVerifier()
	l := newInflightLimiter(4, 2, v)
	entered, release := make(chan struct{}, 4), make(chan struct{})
	h := l.wrap(v.require(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}

		<-release
		w.WriteHeader(http.StatusOK)
	})))
	media := validToken(t, roleMedia)
	field := validToken(t, roleLapangan)
	operator := validToken(t, roleOperator)

	var wg sync.WaitGroup
	// Every blocked request will be released even if an assertion fails.
	t.Cleanup(func() { close(release); wg.Wait() })

	start := func(token string) {
		wg.Go(func() { callAs(t, h, token) })

		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("admitted request did not reach the handler")
		}
	}

	start(media)
	start(media)

	for range 100 {
		rec := callAs(t, h, media)
		if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "1" {
			t.Fatalf("Media flood: got %d %v", rec.Code, rec.Header())
		}
	}

	start(field)
	start(operator)

	for _, token := range []string{media, field, operator} {
		if rec := callAs(t, h, token); rec.Code != http.StatusTooManyRequests {
			t.Fatalf("hard cap must reject every role, got %d", rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	l.metrics(rec, nil)

	for _, sample := range []string{
		`client_api_inflight_requests{role="Media"} 2`,
		`client_api_inflight_requests{role="Lapangan"} 1`,
		`client_api_inflight_requests{role="Operator"} 1`,
		`client_api_inflight_rejected_requests_total{role="Media",reason="media_cap"} 100`,
		`client_api_inflight_rejected_requests_total{role="Lapangan",reason="global_cap"} 1`,
	} {
		if !strings.Contains(rec.Body.String(), sample) {
			t.Errorf("missing metric %s in %s", sample, rec.Body.String())
		}
	}
}

func TestInflightRoleRequiresValidUnexpiredSignature(t *testing.T) {
	t.Parallel()

	l := newInflightLimiter(3, 1, newVerifier())

	cases := []struct{ name, token, want string }{
		{"Media", validToken(t, roleMedia), roleMedia},
		{"Lapangan", validToken(t, roleLapangan), roleLapangan},
		{"Operator", validToken(t, roleOperator), roleOperator},
		{"missing", "", roleAnonymous},
		{"forged", sign(t, "wrong-secret", hs256, claimsExp(roleOperator, testNow.Add(time.Minute))), roleAnonymous},
		{"expired", expiredToken(t), roleAnonymous},
		{"unknown role", validToken(t, "Admin"), roleAnonymous},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/hazards", nil)
			r.Header.Set("X-Client-Role", roleOperator)
			r.AddCookie(sessionCookie(cookieJWT, tc.token))

			if role := l.role(r); role != tc.want {
				t.Fatalf("got role %q, want %q", role, tc.want)
			}
		})
	}
}

func TestAnonymousSharesMediaBudgetAndSlotsRecover(t *testing.T) {
	t.Parallel()

	l := newInflightLimiter(3, 1, nil)
	if !l.acquire(roleAnonymous) || l.acquire(roleMedia) || !l.acquire(roleLapangan) || !l.acquire(roleOperator) {
		t.Fatal("anonymous traffic must share Media's budget and leave two priority slots")
	}

	l.release(roleAnonymous)
	l.release(roleLapangan)
	l.release(roleOperator)

	if !l.acquire(roleMedia) {
		t.Fatal("Media must recover when low priority capacity is freed")
	}

	l.release(roleMedia)

	// Cancellation and panics must also return the slot.
	h := l.wrap(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		panic("handler failure")
	}))

	func() {
		defer func() { _ = recover() }()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(ctx, http.MethodGet, "/hazards", nil))
	}()

	if !l.acquire(roleMedia) || l.total != 1 || l.low != 1 {
		t.Fatal("handler exit leaked its slot")
	}

	l.release(roleMedia)
}

func TestPriorityCapExemptsProbesMetricsAndStreams(t *testing.T) {
	t.Parallel()

	l := newInflightLimiter(2, 1, nil)
	l.acquire(roleMedia)
	l.acquire(roleLapangan)
	h := l.wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))

	for _, path := range []string{"/health", "/metrics", "/hazards/stream"} {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))

		if r.Code != http.StatusOK {
			t.Errorf("%s must remain available under overload: %d", path, r.Code)
		}
	}
}

func TestConfiguredInflightBudgets(t *testing.T) {
	// Environment configuration cannot run in parallel.
	for _, tc := range []struct {
		name, total, media string
		want               int
		bad                bool
	}{
		{"default", "", "", 80, false},
		{"scaled default", "10", "", 8, false},
		{"explicit", "10", "6", 6, false},
		{"no reservation", "10", "10", 0, true},
		{"over cap", "10", "11", 0, true},
		{"zero", "10", "0", 0, true},
		{"invalid", "10", "NaN", 0, true},
		{"invalid global", "bad", "", 0, true},
		{"no room", "1", "", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("MAX_INFLIGHT_REQUESTS", tc.total)
			t.Setenv("MEDIA_MAX_INFLIGHT", tc.media)

			l, err := configuredInflightLimiter(nil)
			if (err != nil) != tc.bad {
				t.Fatalf("unexpected config error: %v", err)
			}

			if err == nil && l.mediaMax != tc.want {
				t.Fatalf("got Media cap %d, want %d", l.mediaMax, tc.want)
			}
		})
	}
}
