package main

// Service-to-service suites: this Aggregator against the real BMKG and PVMBG
// mocks, its Canonical Store and RabbitMQ, over the real network. Run them with
// tests/integration/run.sh agg-bmkg or agg-pvmbg; without IT_AGG_GRPC_ADDR every
// test here skips, so `go test ./...` stays offline.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/brii26/distributed-hazard-coordination/services/aggregator/aggregatorpb"
)

type itAgg struct {
	db                      *store
	read                    aggregatorpb.AggregatorClient
	bmkgURL, bmkgKey        string
	pvmbgURL, pvmbgAdminKey string
}

func aggIntegration(t *testing.T) itAgg {
	t.Helper()

	addr := os.Getenv("IT_AGG_GRPC_ADDR")
	if addr == "" {
		t.Skip("IT_AGG_GRPC_ADDR not set (run tests/integration/run.sh agg-bmkg or agg-pvmbg)")
	}

	db, err := openStore(os.Getenv("CANONICAL_DB_TEST_URL"), 2)
	if err != nil {
		t.Fatalf("Canonical Store: %v", err)
	}

	t.Cleanup(func() { _ = db.close() })

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return itAgg{
		db: db, read: aggregatorpb.NewAggregatorClient(conn),
		bmkgURL: os.Getenv("IT_BMKG_URL"), bmkgKey: os.Getenv("IT_BMKG_KEY"),
		pvmbgURL: os.Getenv("IT_PVMBG_URL"), pvmbgAdminKey: os.Getenv("IT_PVMBG_ADMIN_KEY"),
	}
}

// waitFor polls cond until it holds or the timeout passes.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()

	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}

	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

// hazards calls the Aggregator's read API the way the Client-Facing API does.
func (e itAgg) hazards(typ string) (*aggregatorpb.GetHazardsResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return e.read.GetHazards(ctx, &aggregatorpb.GetHazardsRequest{HazardType: typ})
}

// stored lists what the Canonical Store holds for a hazard type.
func (e itAgg) stored(t *testing.T, typ string) []HazardEvent {
	t.Helper()

	events, err := e.db.listByType(context.Background(), typ)
	if err != nil {
		t.Fatal(err)
	}

	return events
}

// send makes one HTTP call with an optional header and returns the status and body.
func send(t *testing.T, method, url, headerKey, headerValue string) (int, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), method, url, nil)
	if err != nil {
		t.Fatal(err)
	}

	if headerKey != "" {
		req.Header.Set(headerKey, headerValue)
	}

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}

	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)

	return resp.StatusCode, body
}

// itCompose runs a docker compose command against the suite's project.
func itCompose(t *testing.T, args ...string) string {
	t.Helper()

	base := strings.Fields(os.Getenv("IT_COMPOSE"))
	if len(base) == 0 {
		t.Fatal("IT_COMPOSE not set")
	}

	var stderr strings.Builder

	cmd := exec.CommandContext(context.Background(), base[0], append(base[1:], args...)...) //nolint:gosec // IT_COMPOSE is set by tests/integration/run.sh
	// compose prints warnings on stderr; only stdout is the answer.
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}

	return strings.TrimSpace(string(out))
}

// ---- BMKG ----

func (e itAgg) bmkgList(t *testing.T, path string) []map[string]any {
	t.Helper()

	code, body := send(t, http.MethodGet, e.bmkgURL+path, "X-BMKG-Key", e.bmkgKey)
	if code != http.StatusOK {
		t.Fatalf("GET %s: %d %s", path, code, body)
	}

	var out []map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}

	return out
}

func TestIntegrationBMKGKeyIsAcceptedAndRejected(t *testing.T) {
	e := aggIntegration(t)

	tests := []struct {
		name, key string
		want      int
	}{
		{"missing key", "", http.StatusUnauthorized},
		{"another institution's key", e.pvmbgAdminKey, http.StatusForbidden},
		{"wrong key", "not-the-key", http.StatusForbidden},
		{"the Aggregator's key", e.bmkgKey, http.StatusOK},
	}

	for _, tt := range tests {
		if code, _ := send(t, http.MethodGet, e.bmkgURL+"/seismic-events", "X-BMKG-Key", tt.key); code != tt.want {
			t.Fatalf("%s: expected %d, got %d", tt.name, tt.want, code)
		}
	}

	// The proof the Aggregator really sends the right key: BMKG data reached the store.
	waitFor(t, "BMKG events in the Canonical Store", 30*time.Second, func() bool { return len(e.stored(t, typeSeismic)) > 0 })
}

func TestIntegrationBMKGSinceCursorIsHonouredAndPollingIsIncremental(t *testing.T) {
	e := aggIntegration(t)

	if code, body := send(t, http.MethodGet, e.bmkgURL+"/seismic-events?since="+time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "X-BMKG-Key", e.bmkgKey); code != http.StatusOK || strings.TrimSpace(string(body)) != "[]" {
		t.Fatalf("since in the future must give an empty list, got %d %s", code, body)
	}

	all := e.bmkgList(t, "/seismic-events")
	if len(all) < 20 {
		t.Fatalf("the mock must start with at least 20 events, got %d", len(all))
	}

	waitFor(t, "the seed events in the store", 30*time.Second, func() bool { return len(e.stored(t, typeSeismic)) >= len(all) })

	// The generator keeps adding events, and they must keep arriving without
	// re-reading everything (the Aggregator polls with its cursor).
	before := len(e.stored(t, typeSeismic))
	waitFor(t, "a newly generated event to arrive", 30*time.Second, func() bool { return len(e.stored(t, typeSeismic)) > before })

	// Every stored event exists at the source, once.
	known := map[string]bool{}
	for _, ev := range e.bmkgList(t, "/seismic-events") {
		known[ev["event_id"].(string)] = true //nolint:forcetypeassert // the mock's contract
	}

	seen := map[string]bool{}

	for _, h := range e.stored(t, typeSeismic) {
		if !known[h.SourceRefID] || seen[h.SourceRefID] {
			t.Fatalf("stored event %s is unknown to BMKG or stored twice", h.SourceRefID)
		}

		seen[h.SourceRefID] = true
	}
}

// wantSeverity is TRD 3.2's rule, written out independently of the mapper.
func wantSeverity(magnitude float64, warnings []map[string]any) string {
	rank := map[string]int{"Waspada": 1, "Siaga": 2, "Awas": 3}
	best := ""

	for _, w := range warnings {
		if level, _ := w["threat_level"].(string); rank[level] > rank[best] {
			best = level
		}
	}

	switch {
	case best != "":
		return strings.ToUpper(best)
	case magnitude < 5.0:
		return severityNormal
	case magnitude < 6.5:
		return severityWaspada
	default:
		return severitySiaga
	}
}

func TestIntegrationBMKGEventsAndWarningsAreMappedAndStored(t *testing.T) {
	e := aggIntegration(t)

	events := e.bmkgList(t, "/seismic-events")
	warnings := e.bmkgList(t, "/tsunami-warnings")

	if len(warnings) == 0 {
		t.Fatal("the BMKG mock must have tsunami warnings in its seed data")
	}

	byID := func() map[string]HazardEvent {
		m := map[string]HazardEvent{}
		for _, h := range e.stored(t, typeSeismic) {
			m[h.SourceRefID] = h
		}

		return m
	}

	waitFor(t, "every event in the snapshot to be stored", 45*time.Second, func() bool {
		stored := byID()
		for _, ev := range events {
			if _, ok := stored[ev["event_id"].(string)]; !ok { //nolint:forcetypeassert // the mock's contract
				return false
			}
		}

		return true
	})

	stored := byID()
	withWarning := 0

	for _, ev := range events {
		id, _ := ev["event_id"].(string)
		mag, _ := ev["magnitude"].(float64)

		var related []map[string]any

		for _, w := range warnings {
			if w["related_event_id"] == id {
				related = append(related, w)
			}
		}

		h := stored[id]

		if h.Source != sourceBMKG || h.HazardType != typeSeismic {
			t.Fatalf("%s stored as %s/%s", id, h.Source, h.HazardType)
		}

		if h.AreaName == nil || *h.AreaName != ev["region_name"] || h.Latitude == nil || *h.Latitude != ev["epicenter_lat"] ||
			h.Longitude == nil || *h.Longitude != ev["epicenter_lon"] {
			t.Fatalf("%s: area and coordinates must come from region_name, epicenter_lat and epicenter_lon, got %+v", id, h)
		}

		if want := wantSeverity(mag, related); h.Severity == nil || *h.Severity != want {
			t.Fatalf("%s: severity should be %s (magnitude %v, %d warnings), got %v", id, want, mag, len(related), h.Severity)
		}

		if h.Attributes["magnitude"] != mag {
			t.Fatalf("%s: magnitude must be kept in attributes, got %v", id, h.Attributes["magnitude"])
		}

		if len(related) > 0 {
			withWarning++

			if _, ok := h.Attributes["tsunami_warning"].(map[string]any); !ok {
				t.Fatalf("%s has a tsunami warning at the source but none in attributes: %v", id, h.Attributes)
			}
		}
	}

	if withWarning == 0 {
		t.Fatal("no stored event was correlated with a tsunami warning")
	}
}

// ---- PVMBG ----

// toggle flips one of the mock's admin switches and returns its JSON answer.
func (e itAgg) toggle(t *testing.T, path string) map[string]any {
	t.Helper()

	code, body := send(t, http.MethodPost, e.pvmbgURL+path, "X-Admin-Key", e.pvmbgAdminKey)
	if code != http.StatusOK {
		t.Fatalf("POST %s: %d %s", path, code, body)
	}

	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}

	return out
}

// pvmbgStatus is the state of PVMBG as the read API reports it, or "" when the call fails.
func (e itAgg) pvmbgStatus() string {
	resp, err := e.hazards(typeVolcanic)
	if err != nil {
		return ""
	}

	return resp.GetMeta().GetSourceStatus()
}

func TestIntegrationPVMBGOAuthBootstrapFeedsTheStore(t *testing.T) {
	e := aggIntegration(t)

	// The mock only hands out tokens for its one-time code and Bearer-protects
	// /volcanic-reports, so reports in the store prove bootstrap and the Bearer call.
	if code, _ := send(t, http.MethodGet, e.pvmbgURL+"/volcanic-reports", "", ""); code != http.StatusUnauthorized {
		t.Fatalf("/volcanic-reports without a token must be 401, got %d", code)
	}

	waitFor(t, "volcanic reports in the Canonical Store", 60*time.Second, func() bool { return len(e.stored(t, typeVolcanic)) > 0 })
	waitFor(t, "PVMBG reported UP", 30*time.Second, func() bool { return e.pvmbgStatus() == statusUp })
}

func TestIntegrationPVMBGOutageGoesStaleThenRecoversAndSeismicIsUnaffected(t *testing.T) {
	e := aggIntegration(t)

	waitFor(t, "stored volcanic data and PVMBG UP", 60*time.Second, func() bool {
		return len(e.stored(t, typeVolcanic)) > 0 && e.pvmbgStatus() == statusUp
	})
	waitFor(t, "stored seismic data", 30*time.Second, func() bool { return len(e.stored(t, typeSeismic)) > 0 })

	outage := e.toggle(t, "/admin/outage")["outage_active"] == true

	t.Cleanup(func() {
		if outage {
			e.toggle(t, "/admin/outage")
		}
	})

	if !outage {
		t.Fatal("expected the toggle to switch the outage on")
	}

	waitFor(t, "the PVMBG breaker to open", 30*time.Second, func() bool { return e.pvmbgStatus() == statusDown })

	// Stored data is still served, marked stale.
	vol, err := e.hazards(typeVolcanic)
	if err != nil || len(vol.GetData()) == 0 || vol.GetMeta().GetStaleSince() == nil || vol.GetMeta().GetLastSuccessAt() == nil {
		t.Fatalf("a down PVMBG with stored data must serve it with stale_since and last_success_at, got %v %v", vol, err)
	}

	// BMKG is untouched.
	seis, err := e.hazards(typeSeismic)
	if err != nil || len(seis.GetData()) == 0 || seis.GetMeta().GetSourceStatus() == statusDown || seis.GetMeta().GetStaleSince() != nil {
		t.Fatalf("seismic must be unaffected by a PVMBG outage, got %v %v", seis, err)
	}

	// Switching it off recovers without a restart.
	if e.toggle(t, "/admin/outage")["outage_active"] != false {
		t.Fatal("expected the toggle to switch the outage off")
	}

	outage = false

	waitFor(t, "PVMBG to recover", 30*time.Second, func() bool {
		resp, err := e.hazards(typeVolcanic)

		return err == nil && resp.GetMeta().GetSourceStatus() == statusUp && resp.GetMeta().GetStaleSince() == nil
	})
}

func TestIntegrationPVMBGSchemaChangeAddsConfidenceLevelWithoutARestart(t *testing.T) {
	e := aggIntegration(t)

	waitFor(t, "stored volcanic data", 60*time.Second, func() bool { return len(e.stored(t, typeVolcanic)) > 0 })

	withConfidence := func() int {
		n := 0

		for _, h := range e.stored(t, typeVolcanic) {
			if _, ok := h.Attributes["confidence_level"]; ok {
				n++
			}
		}

		return n
	}

	if n := withConfidence(); n != 0 {
		t.Fatalf("the legacy schema has no confidence_level, but %d stored reports carry one", n)
	}

	container := itCompose(t, "ps", "-q", "aggregator")
	startedAt := func() string {
		out, err := exec.CommandContext(context.Background(), "docker", "inspect", "-f", "{{.State.StartedAt}}", container).CombinedOutput()
		if err != nil {
			t.Fatalf("docker inspect: %v %s", err, out)
		}

		return strings.TrimSpace(string(out))
	}

	started, before := startedAt(), len(e.stored(t, typeVolcanic))

	if v := e.toggle(t, "/admin/schema-version")["schema_version"]; v != "extended" {
		t.Fatalf("expected the toggle to switch to the extended schema, got %v", v)
	}

	t.Cleanup(func() { e.toggle(t, "/admin/schema-version") })

	waitFor(t, "reports with confidence_level to be stored", 30*time.Second, func() bool { return withConfidence() > 0 })

	if got := startedAt(); got != started {
		t.Fatalf("the Aggregator was restarted by the schema change: started %s, now %s", started, got)
	}

	if after := len(e.stored(t, typeVolcanic)); after < before {
		t.Fatalf("stored reports went from %d to %d: the schema change lost data", before, after)
	}

	// The read path keeps working with the new field.
	if _, err := e.hazards(typeVolcanic); err != nil {
		t.Fatalf("the read API failed after the schema change: %v", err)
	}
}

// tokenFile reads the PVMBG token the Aggregator keeps on its volume.
func tokenFileOf(t *testing.T) tokenFile {
	t.Helper()

	var f tokenFile
	if err := json.Unmarshal([]byte(itCompose(t, "exec", "-T", "aggregator", "cat", "/var/lib/aggregator/pvmbg-token.json")), &f); err != nil {
		t.Fatalf("the token file: %v", err)
	}

	return f
}

func TestIntegrationPVMBGTokenRotatesAndASpentCodeIsNotNeededAfterARestart(t *testing.T) {
	e := aggIntegration(t)

	waitFor(t, "PVMBG UP", 60*time.Second, func() bool { return e.pvmbgStatus() == statusUp })

	first := tokenFileOf(t)
	if first.AccessToken == "" || first.RefreshToken == "" {
		t.Fatalf("expected a persisted token pair, got %+v", first)
	}

	// Fresh tokens last 60 s and rotate at a third of that.
	waitFor(t, "the refresh token to rotate", 60*time.Second, func() bool { return tokenFileOf(t).RefreshToken != first.RefreshToken })

	if e.pvmbgStatus() == statusDown {
		t.Fatal("PVMBG went down while the token rotated")
	}

	// The one-time code is spent (the mock takes it once), so coming back after a
	// restart can only work from the token on the volume.
	itCompose(t, "restart", "aggregator")

	waitFor(t, "PVMBG UP again after the restart", 90*time.Second, func() bool {
		_, err := e.read.GetHealth(context.Background(), &emptypb.Empty{})

		return err == nil && e.pvmbgStatus() == statusUp
	})

	if got := fmt.Sprint(len(e.stored(t, typeVolcanic)) > 0); got != "true" {
		t.Fatal("expected volcanic reports in the store")
	}
}
