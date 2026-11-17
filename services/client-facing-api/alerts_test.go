package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func openAlerts(t *testing.T, upstream, role, lastID string) (code int, body string) {
	t.Helper()

	api := newAlertsAPI(upstream)
	claims := jwtClaims{Email: "field@x.example", Role: role, Exp: time.Now().Add(time.Minute).Unix()}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), claimsKey{}, claims)))
	})

	srv := httptest.NewServer(withCorrelation(h))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/alerts/stream", nil)
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = resp.Body.Close() }()

	b, _ := io.ReadAll(resp.Body)

	return resp.StatusCode, string(b)
}

func TestAlertsStreamRelaysFramesAndLastEventID(t *testing.T) {
	t.Parallel()

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/alerts/stream" {
			http.NotFound(w, r)

			return
		}

		_, _ = fmt.Fprintf(w, "id: o-1\nevent: alert\ndata: {\"from\":%q}\n\n", r.Header.Get("Last-Event-ID"))
	}))
	t.Cleanup(up.Close)

	code, body := openAlerts(t, up.URL, roleLapangan, "o-0")
	if code != http.StatusOK || !strings.Contains(body, "id: o-1") || !strings.Contains(body, `{"from":"o-0"}`) {
		t.Fatalf("got %d %q", code, body)
	}
}

func TestAlertsStreamIsNotForMedia(t *testing.T) {
	t.Parallel()

	if code, _ := openAlerts(t, "http://127.0.0.1:1", roleMedia, ""); code != http.StatusForbidden {
		t.Fatalf("got %d, want 403", code)
	}
}

func TestAlertsStreamBadGatewayWhenTheNotifierIsDown(t *testing.T) {
	t.Parallel()

	if code, _ := openAlerts(t, "http://127.0.0.1:1", roleOperator, ""); code != http.StatusBadGateway {
		t.Fatalf("got %d, want 502", code)
	}
}
