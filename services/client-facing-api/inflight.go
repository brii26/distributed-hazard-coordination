package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
)

const roleAnonymous = "anonymous"

// Counts are local to this API instance. Anonymous traffic shares Media's budget
// so login, refresh and invalid tokens cannot consume the reserved capacity.
type inflightLimiter struct {
	mu            sync.Mutex
	max, mediaMax int
	total, low    int
	active        map[string]int
	rejected      map[string]map[string]uint64
	verifier      *jwtVerifier
}

func newInflightLimiter(maxInflight, mediaMax int, verifier *jwtVerifier) *inflightLimiter {
	return &inflightLimiter{
		max: maxInflight, mediaMax: mediaMax, verifier: verifier,
		active: make(map[string]int), rejected: make(map[string]map[string]uint64),
	}
}

func configuredInflightLimiter(verifier *jwtVerifier) (*inflightLimiter, error) {
	maxInflight, err := envPositiveInt("MAX_INFLIGHT_REQUESTS", defaultMaxInflight)
	if err != nil {
		return nil, err
	}

	mediaMax, err := envPositiveInt("MEDIA_MAX_INFLIGHT", max(1, maxInflight*4/5))
	if err != nil {
		return nil, err
	}

	if mediaMax >= maxInflight {
		return nil, fmt.Errorf("MEDIA_MAX_INFLIGHT (%d) must be less than MAX_INFLIGHT_REQUESTS (%d)", mediaMax, maxInflight)
	}

	return newInflightLimiter(maxInflight, mediaMax, verifier), nil
}

// Only an unexpired, locally verified JWT grants priority. Headers and unsigned
// claims never grant it. Expired sessions use the low budget during refresh.
func (l *inflightLimiter) role(r *http.Request) string {
	if l.verifier != nil {
		if cookie, err := r.Cookie(cookieJWT); err == nil {
			if claims, err := l.verifier.verify(cookie.Value); err == nil {
				return claims.Role
			}
		}
	}

	return roleAnonymous
}

func highPriority(role string) bool {
	return role == roleLapangan || role == roleOperator
}

// Both budgets are checked under one lock, so a failed admission takes no slots.
func (l *inflightLimiter) acquire(role string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	reason := ""
	if l.total >= l.max {
		reason = "global_cap"
	} else if !highPriority(role) && l.low >= l.mediaMax {
		reason = "media_cap"
	}

	if reason != "" {
		if l.rejected[role] == nil {
			l.rejected[role] = make(map[string]uint64)
		}

		l.rejected[role][reason]++

		return false
	}

	l.total++

	l.active[role]++
	if !highPriority(role) {
		l.low++
	}

	return true
}

func (l *inflightLimiter) release(role string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.total--

	l.active[role]--
	if !highPriority(role) {
		l.low--
	}
}

func (l *inflightLimiter) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Streams have their own limits. Probes and scrapes must work under load.
		if r.URL.Path == "/health" || r.URL.Path == "/metrics" || r.URL.Path == "/hazards/stream" {
			next.ServeHTTP(w, r)

			return
		}

		role := l.role(r)
		if !l.acquire(role) {
			logFor(r.Context()).Warn("rate-limit", "kind", "inflight", "role", role, "path", r.URL.Path, "remaining", 0)
			tooMany(w, 1)

			return
		}
		defer l.release(role)

		next.ServeHTTP(w, r)
	})
}

// Prometheus text format with bounded labels and no external collector needed.
func (l *inflightLimiter) metrics(w http.ResponseWriter, _ *http.Request) {
	l.mu.Lock()

	var body strings.Builder
	for _, role := range []string{roleMedia, roleLapangan, roleOperator, roleAnonymous} {
		_, _ = fmt.Fprintf(&body, "client_api_inflight_requests{role=%q} %d\n", role, l.active[role])
		for _, reason := range []string{"media_cap", "global_cap"} {
			_, _ = fmt.Fprintf(&body, "client_api_inflight_rejected_requests_total{role=%q,reason=%q} %d\n", role, reason, l.rejected[role][reason])
		}
	}
	l.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprint(w, "# HELP client_api_inflight_requests Active requests admitted by the concurrency limiter.\n# TYPE client_api_inflight_requests gauge\n# HELP client_api_inflight_rejected_requests_total Requests rejected by the concurrency limiter.\n# TYPE client_api_inflight_rejected_requests_total counter\n"+body.String())
}
