package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestSeedAndSchemaFlag(t *testing.T) {
	t.Parallel()

	now := time.Now()
	legacy, extended := &reportStore{}, &reportStore{}
	legacy.seed(25, time.Second, now, false)
	extended.seed(25, time.Second, now, true)

	got := legacy.since(time.Time{})
	if len(got) != 25 {
		t.Fatalf("expected 25 seeded reports, got %d", len(got))
	}

	for _, r := range got {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}

		if strings.Contains(string(b), "confidence_level") {
			t.Fatalf("legacy report must not carry confidence_level: %s", b)
		}

		if r.ReportID == "" || r.VolcanoID == "" || r.AlertLevel == "" || r.ReportedAt.After(now) {
			t.Fatalf("bad seeded report %+v", r)
		}
	}

	for _, r := range extended.since(time.Time{}) {
		if r.ConfidenceLevel == nil || *r.ConfidenceLevel < 0 || *r.ConfidenceLevel > 1 {
			t.Fatalf("extended report needs confidence_level in [0,1]: %+v", r)
		}
	}
}

func TestGeneratorAddsReportsAndHonoursSchemaFlag(t *testing.T) {
	t.Parallel()

	s := &reportStore{}
	s.seed(20, time.Second, time.Now(), false)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		defer close(done)

		s.run(ctx, 5*time.Millisecond, func() bool { return true })
	}()

	deadline := time.Now().Add(2 * time.Second)
	for len(s.since(time.Time{})) < 25 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	<-done

	all := s.since(time.Time{})
	if len(all) < 25 {
		t.Fatalf("generator should keep adding reports, got %d", len(all))
	}

	if last := all[len(all)-1]; last.ConfidenceLevel == nil {
		t.Fatalf("generated report should follow the extended flag: %+v", last)
	}

	if all[0].ConfidenceLevel != nil {
		t.Fatalf("seeded legacy report must stay without confidence_level: %+v", all[0])
	}
}

func TestSinceFilters(t *testing.T) {
	t.Parallel()

	s := &reportStore{}
	base := time.Now()
	s.seed(10, time.Minute, base, false)

	if n := len(s.since(base.Add(-5*time.Minute - time.Second))); n != 5 {
		t.Fatalf("expected the 5 newest reports, got %d", n)
	}
}

func getReports(t *testing.T, tokens *tokenStore, reports *reportStore, minD, maxD time.Duration, auth, query string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/volcanic-reports"+query, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	rec := httptest.NewRecorder()
	requireBearer(tokens, reportsHandler(reports, minD, maxD)).ServeHTTP(rec, req)

	return rec
}

func TestReportsRequireCurrentBearerToken(t *testing.T) {
	t.Parallel()

	tokens := newTestStore()

	first, errCode := tokens.exchange(grantAuthCode, testCode, "")
	if errCode != "" {
		t.Fatal(errCode)
	}

	for _, auth := range []string{"", "Bearer", "Bearer wrong", "Basic " + first.access, first.access} {
		if rec := getReports(t, tokens, &reportStore{}, 0, 0, auth, ""); rec.Code != http.StatusUnauthorized {
			t.Fatalf("auth %q: expected 401, got %d", auth, rec.Code)
		}
	}

	if rec := getReports(t, tokens, &reportStore{}, 0, 0, "Bearer "+first.access, ""); rec.Code != http.StatusOK {
		t.Fatalf("valid token: expected 200, got %d", rec.Code)
	}

	if _, errCode := tokens.exchange(grantRefreshToken, "", first.refresh); errCode != "" {
		t.Fatal(errCode)
	}

	if rec := getReports(t, tokens, &reportStore{}, 0, 0, "Bearer "+first.access, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("superseded token: expected 401, got %d", rec.Code)
	}
}

func TestReportsSinceAndShape(t *testing.T) {
	t.Parallel()

	tokens := newTestStore()
	pair, _ := tokens.exchange(grantAuthCode, testCode, "")
	auth := "Bearer " + pair.access

	reports := &reportStore{}
	base := time.Now().UTC()
	reports.seed(10, time.Minute, base, true)

	since := url.QueryEscape(base.Add(-5*time.Minute - time.Second).Format(time.RFC3339Nano))

	var got []map[string]any
	if err := json.Unmarshal(getReports(t, tokens, reports, 0, 0, auth, "?since="+since).Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}

	if len(got) != 5 {
		t.Fatalf("expected the 5 newest reports, got %d", len(got))
	}

	for _, f := range []string{"report_id", "volcano_id", "alert_level", "eruption_count_24h", "ash_column_height_m", "reported_at", "confidence_level"} {
		if _, ok := got[0][f]; !ok {
			t.Fatalf("missing field %q in %v", f, got[0])
		}
	}

	if rec := getReports(t, tokens, reports, 0, 0, auth, "?since=yesterday"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad since: expected 400, got %d", rec.Code)
	}

	if body := getReports(t, tokens, &reportStore{}, 0, 0, auth, "").Body.String(); body != "[]\n" {
		t.Fatalf("empty result should be [], got %q", body)
	}
}

func TestReportsDelayWithinRange(t *testing.T) {
	t.Parallel()

	tokens := newTestStore()
	pair, _ := tokens.exchange(grantAuthCode, testCode, "")

	const lo, hi = 40 * time.Millisecond, 80 * time.Millisecond

	for range 5 {
		start := time.Now()

		getReports(t, tokens, &reportStore{}, lo, hi, "Bearer "+pair.access, "")

		if d := time.Since(start); d < lo || d > hi+250*time.Millisecond {
			t.Fatalf("latency %v outside [%v, %v]", d, lo, hi)
		}
	}
}

func TestAccessTokenExpires(t *testing.T) {
	t.Parallel()

	clock := time.Now()
	tokens := newTestStore()
	tokens.now = func() time.Time { return clock }

	pair, errCode := tokens.exchange(grantAuthCode, testCode, "")
	if errCode != "" {
		t.Fatal(errCode)
	}

	auth := "Bearer " + pair.access
	status := func() int { return getReports(t, tokens, &reportStore{}, 0, 0, auth, "").Code }

	if got := status(); got != http.StatusOK {
		t.Fatalf("fresh token: expected 200, got %d", got)
	}

	clock = clock.Add(tokenTTLSeconds*time.Second - time.Nanosecond)

	if got := status(); got != http.StatusOK {
		t.Fatalf("just before expiry: expected 200, got %d", got)
	}

	clock = clock.Add(time.Nanosecond)

	if got := status(); got != http.StatusUnauthorized {
		t.Fatalf("at expiry: expected 401, got %d", got)
	}
}

func TestRefreshAfterExpiryGivesFreshLifetime(t *testing.T) {
	t.Parallel()

	clock := time.Now()
	tokens := newTestStore()
	tokens.now = func() time.Time { return clock }

	first, _ := tokens.exchange(grantAuthCode, testCode, "")

	clock = clock.Add(10 * time.Minute)

	second, errCode := tokens.exchange(grantRefreshToken, "", first.refresh)
	if errCode != "" {
		t.Fatalf("refresh after access expiry should work, got %q", errCode)
	}

	if !tokens.validAccess(second.access) {
		t.Fatal("new access token should be valid right after refresh")
	}

	clock = clock.Add(tokenTTLSeconds*time.Second - time.Nanosecond)

	if !tokens.validAccess(second.access) {
		t.Fatal("new access token should live the full advertised lifetime")
	}

	clock = clock.Add(time.Nanosecond)

	if tokens.validAccess(second.access) {
		t.Fatal("new access token should expire after its lifetime")
	}

	if tokens.validAccess(first.access) {
		t.Fatal("superseded access token must stay invalid")
	}

	if _, errCode := tokens.exchange(grantRefreshToken, "", first.refresh); errCode == "" {
		t.Fatal("superseded refresh token must stay invalid")
	}
}
