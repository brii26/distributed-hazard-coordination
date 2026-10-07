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

// A Dashboard Updater that sends one seismic and one volcanic event, then ends the stream.
func upstreamStream() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, ": open\n\n")
		_, _ = fmt.Fprint(w, `data: {"hazard_id":"s1","hazard_type":"SEISMIC","severity":"AWAS","source_ref_id":"ref","latitude":1.5,"longitude":2.5,"attributes":{"k":1}}`+"\n\n")
		_, _ = fmt.Fprint(w, `data: {"hazard_id":"v1","hazard_type":"VOLCANIC","severity":"AWAS","source_ref_id":"ref"}`+"\n\n")
	}))
}

// Asks for /hazards/stream as role, with a session that expires in exp.
func openStream(t *testing.T, upstream, role, query string, exp time.Duration) (code int, body string) {
	t.Helper()

	api := newStreamAPI(upstream)
	claims := jwtClaims{Email: "stream@x.example", Role: role, Exp: time.Now().Add(exp).Unix()}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), claimsKey{}, claims)))
	})

	srv := httptest.NewServer(withCorrelation(h))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/hazards/stream?"+query, nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = resp.Body.Close() }()

	b, _ := io.ReadAll(resp.Body)

	return resp.StatusCode, string(b)
}

func TestStreamFiltersByTypeAndRole(t *testing.T) {
	t.Parallel()

	up := upstreamStream()
	t.Cleanup(up.Close)

	code, body := openStream(t, up.URL, roleMedia, "type=SEISMIC", time.Minute)
	if code != http.StatusOK || !strings.Contains(body, `"hazard_id":"s1"`) {
		t.Fatalf("expected the seismic event, got %d %q", code, body)
	}

	if strings.Contains(body, "v1") {
		t.Fatalf("a volcanic event leaked into a seismic stream: %q", body)
	}

	for _, raw := range rawFields {
		if strings.Contains(body, raw) {
			t.Fatalf("Media received raw field %s: %q", raw, body)
		}
	}

	_, body = openStream(t, up.URL, roleOperator, "type=SEISMIC", time.Minute)
	for _, raw := range rawFields {
		if !strings.Contains(body, raw) {
			t.Fatalf("Operator is missing raw field %s: %q", raw, body)
		}
	}
}

func TestStreamRejectsABadType(t *testing.T) {
	t.Parallel()

	if code, _ := openStream(t, "http://unused", roleMedia, "type=NOPE", time.Minute); code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", code)
	}
}

func TestStreamEndsWhenTheSessionExpires(t *testing.T) {
	t.Parallel()

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, ": open\n\n")
		_ = http.NewResponseController(w).Flush()

		<-r.Context().Done()
	}))
	t.Cleanup(up.Close)

	done := make(chan struct{})

	go func() {
		defer close(done)

		openStream(t, up.URL, roleMedia, "type=SEISMIC", 300*time.Millisecond)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the stream outlived its token")
	}
}

func TestStreamIsBadGatewayWhenTheUpstreamIsDown(t *testing.T) {
	t.Parallel()

	if code, _ := openStream(t, "http://127.0.0.1:1", roleMedia, "type=SEISMIC", time.Minute); code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d", code)
	}
}
