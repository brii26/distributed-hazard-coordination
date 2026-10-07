package main

// Abuse suite: the whole stack in an isolated compose project, pushed through restarts, outages, schema change, consumer down, sustained load, forged credentials and malformed input. Run it with tests/integration/run.sh abuse, without IT_ABUSE_URL every test skips.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	svcPVMBG     = "pvmbg-mock"
	tokenTTL     = 60 * time.Second // the stack runs with the default ACCESS_TOKEN_TTL_S
	typSeismic   = "SEISMIC"
	typVolcanic  = "VOLCANIC"
	fieldAttribs = "attributes"
)

// Keeps connections alive, so a load test doesn't run out of ports and blame the system for it.
var abClient = &http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 200}}

type abEnv struct {
	itEnv

	bmkgURL, bmkgKey, pvmbgURL, pvmbgAdminKey string
	dbUser, dbName                            string
}

func abuseEnv(t *testing.T) abEnv {
	t.Helper()

	url := os.Getenv("IT_ABUSE_URL")
	if url == "" {
		t.Skip("IT_ABUSE_URL not set (run tests/integration/run.sh abuse)")
	}

	return abEnv{
		itEnv: itEnv{
			url:              url,
			operatorEmail:    os.Getenv("IT_OPERATOR_EMAIL"),
			operatorPassword: os.Getenv("IT_OPERATOR_PASSWORD"),
		},
		bmkgURL: os.Getenv("IT_BMKG_URL"), bmkgKey: os.Getenv("IT_BMKG_KEY"),
		pvmbgURL: os.Getenv("IT_PVMBG_URL"), pvmbgAdminKey: os.Getenv("IT_PVMBG_ADMIN_KEY"),
		dbUser: os.Getenv("IT_CANONICAL_USER"), dbName: os.Getenv("IT_CANONICAL_DB"),
	}
}

// Runs a docker compose command against the suite's project and returns stdout.
func composeOut(t *testing.T, args ...string) string {
	t.Helper()

	base := strings.Fields(os.Getenv("IT_COMPOSE"))
	if len(base) == 0 {
		t.Fatal("IT_COMPOSE not set")
	}

	var stderr strings.Builder

	cmd := exec.CommandContext(context.Background(), base[0], append(base[1:], args...)...) //nolint:gosec // IT_COMPOSE is set by tests/integration/run.sh
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("compose %s: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}

	return strings.TrimSpace(string(out))
}

// Every container of the stack except the web frontend.
var allServices = []string{
	"bmkg-mock", svcPVMBG, "canonical-store", "user-db", "rabbitmq",
	"aggregator", "auth-service", "client-facing-api", "dashboard-updater", "notifier", "audit-logger",
}

func (e abEnv) upAll(t *testing.T) {
	t.Helper()
	composeOut(t, append([]string{"up", "-d", "--wait", "--no-deps"}, allServices...)...)
}

func eventually(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()

	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", within, what)
		}

		time.Sleep(500 * time.Millisecond)
	}
}

// e.do without the t.Errorf on a transport error. Callers expect failures while a service is down and want the status (-1 for no answer).
func (e abEnv) quiet(method, path, body string, jar cookies, hdr map[string]string) reply {
	req, _ := http.NewRequestWithContext(context.Background(), method, e.url+path, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	for n, v := range jar {
		req.AddCookie(&http.Cookie{Name: n, Value: v}) //nolint:gosec // test client
	}

	resp, err := abClient.Do(req)
	if err != nil {
		return reply{code: -1, body: err.Error(), cookies: map[string]*http.Cookie{}}
	}

	defer func() { _ = resp.Body.Close() }()

	raw, _ := io.ReadAll(resp.Body)
	r := reply{code: resp.StatusCode, body: string(raw), cookies: map[string]*http.Cookie{}}

	for _, c := range resp.Cookies() {
		r.cookies[c.Name] = c
	}

	return r
}

type hazardPage struct {
	Data []map[string]any `json:"data"`
	Meta map[string]any   `json:"meta"`
	Page map[string]any   `json:"page"`
	Err  string           `json:"error"`
}

func (e abEnv) hazards(t *testing.T, jar cookies, query string) (reply, hazardPage) {
	t.Helper()

	r := e.quiet(http.MethodGet, "/hazards?"+query, "", jar, nil)

	var p hazardPage

	_ = json.Unmarshal([]byte(r.body), &p)

	return r, p
}

// The Client-Facing API's own /health status.
func (e abEnv) healthStatus() string {
	r := e.quiet(http.MethodGet, "/health", "", nil, nil)

	var h map[string]string

	_ = json.Unmarshal([]byte(r.body), &h)

	if r.code == -1 {
		return "unreachable"
	}

	return h["status"]
}

// True when an Operator can log in and read seismic and volcanic data.
func (e abEnv) servesData(t *testing.T) bool {
	t.Helper()

	r, jar := e.login(t, e.operatorEmail, e.operatorPassword)
	if r.code != http.StatusOK {
		return false
	}

	for _, typ := range []string{typSeismic, typVolcanic} {
		if r, p := e.hazards(t, jar, "type="+typ+"&limit=1"); r.code != http.StatusOK || len(p.Data) == 0 {
			return false
		}
	}

	return true
}

// Gives the next test a known-good stack: every container recreated and every volume wiped.
func (e abEnv) reset(t *testing.T) {
	t.Helper()
	composeOut(t, "down", "-v", "--remove-orphans")
	e.upAll(t)
	e.waitHealthy(t, 120*time.Second)
	eventually(t, 40*time.Second, "both sources ingesting", func() bool { return e.count(t, "source='BMKG'") > 0 && e.count(t, "source='PVMBG'") > 0 })
}

// Waits for the whole stack to serve again after a lifecycle change.
func (e abEnv) waitHealthy(t *testing.T, within time.Duration) {
	t.Helper()
	eventually(t, within, "Client-Facing API /health ok and data served", func() bool {
		return e.healthStatus() == "ok" && e.servesData(t)
	})
}

// Runs a count query on the Canonical Store through its own container, so the suite needs no database driver.
func (e abEnv) count(t *testing.T, where string) int {
	t.Helper()

	q := "SELECT count(*) FROM hazard_events WHERE " + where
	out := composeOut(t, "exec", "-T", "canonical-store", "psql", "-U", e.dbUser, "-d", e.dbName, "-tAc", q)

	n, err := strconv.Atoi(out)
	if err != nil {
		t.Fatalf("count %q: %v (%q)", where, err, out)
	}

	return n
}

// Counts a service's log lines (since the project's start) that contain every given substring.
func logCount(t *testing.T, service string, parts ...string) int {
	t.Helper()

	n := 0

	for l := range strings.SplitSeq(composeOut(t, "logs", "--no-log-prefix", service), "\n") {
		hit := true

		for _, p := range parts {
			hit = hit && strings.Contains(l, p)
		}

		if hit {
			n++
		}
	}

	return n
}

func postAdmin(t *testing.T, base, adminKey, path string) reply {
	t.Helper()

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, base+path, nil)
	req.Header.Set("X-Admin-Key", adminKey)

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}

	defer func() { _ = resp.Body.Close() }()

	raw, _ := io.ReadAll(resp.Body)

	return reply{code: resp.StatusCode, body: string(raw)}
}

// `compose down`, wait past the access token TTL, then `up` with volumes kept. The stack comes back healthy and both sources keep ingesting, even though the PVMBG mock forgot its tokens.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseDownUpKeepsVolumes(t *testing.T) {
	e := abuseEnv(t)
	e.reset(t)

	bmkg, pvmbg := e.count(t, "source='BMKG'"), e.count(t, "source='PVMBG'")

	composeOut(t, "down", "--remove-orphans")
	time.Sleep(tokenTTL + 5*time.Second)
	e.upAll(t)
	e.waitHealthy(t, 90*time.Second)

	eventually(t, 40*time.Second, "BMKG ingest to continue after down/up", func() bool { return e.count(t, "source='BMKG'") > bmkg })
	eventually(t, 40*time.Second, "PVMBG ingest to continue after down/up", func() bool { return e.count(t, "source='PVMBG'") > pvmbg })
}

// `compose down -v` then `up`: a clean bootstrap ends with the stack healthy, both sources ingesting and the seeded Operator able to log in.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseDownVolumesUpIsAFreshStart(t *testing.T) {
	e := abuseEnv(t)

	composeOut(t, "down", "-v", "--remove-orphans")
	e.upAll(t)
	e.waitHealthy(t, 120*time.Second)

	eventually(t, 40*time.Second, "BMKG records after a fresh start", func() bool { return e.count(t, "source='BMKG'") > 0 })
	eventually(t, 40*time.Second, "PVMBG records after a fresh start", func() bool { return e.count(t, "source='PVMBG'") > 0 })
}

// Restarts every service on its own. The stack comes back healthy without restarting a neighbour, and independent services keep answering meanwhile.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseRestartEachServiceAlone(t *testing.T) {
	e := abuseEnv(t)
	e.reset(t)

	jar := e.operator(t)

	order := append(slices.DeleteFunc(slices.Clone(allServices), func(s string) bool { return s == svcPVMBG }), svcPVMBG)

	for _, svc := range order {
		t.Run(svc, func(t *testing.T) {
			pvmbg := e.count(t, "source='PVMBG'")

			composeOut(t, "stop", svc)

			switch svc {
			case svcPVMBG, "rabbitmq", "notifier", "audit-logger", "dashboard-updater", "bmkg-mock", "auth-service", "user-db":
				if r, p := e.hazards(t, jar, "type="+typSeismic+"&limit=1"); r.code != http.StatusOK || len(p.Data) == 0 {
					t.Errorf("seismic read with %s stopped: %d %.120s", svc, r.code, r.body)
				}
			default:
				began := time.Now()
				r, _ := e.hazards(t, jar, "type="+typSeismic+"&limit=1")

				if time.Since(began) > 5*time.Second {
					t.Errorf("read with %s stopped hung for %s (code %d)", svc, time.Since(began), r.code)
				}
			}

			composeOut(t, "start", svc)
			composeOut(t, "up", "-d", "--wait", "--no-deps", svc)
			e.waitHealthy(t, 90*time.Second)

			if svc == svcPVMBG || svc == "aggregator" || svc == "canonical-store" || svc == "rabbitmq" {
				eventually(t, 45*time.Second, "PVMBG ingest to resume after restarting "+svc, func() bool { return e.count(t, "source='PVMBG'") > pvmbg })
			}
		})
	}
}

// Stops rabbitmq, the Canonical Store and the User DB one at a time. Reads and sessions keep working or fail cleanly (never a hang), and everything recovers without a restart.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseBrokerStoreAndUserDBDown(t *testing.T) {
	e := abuseEnv(t)
	e.reset(t)

	jar := e.operator(t)

	t.Run("rabbitmq", func(t *testing.T) {
		seen := logCount(t, "notifier", "message processed")

		composeOut(t, "stop", "rabbitmq")
		time.Sleep(4 * time.Second)

		if r, p := e.hazards(t, jar, "type="+typSeismic+"&limit=1"); r.code != http.StatusOK || len(p.Data) == 0 {
			t.Errorf("reads must survive a broker outage: %d %.120s", r.code, r.body)
		}

		composeOut(t, "start", "rabbitmq")
		eventually(t, 60*time.Second, "notifier to reconnect and process messages again", func() bool {
			return logCount(t, "notifier", "message processed") > seen
		})
		e.waitHealthy(t, 60*time.Second)
	})

	t.Run("canonical-store", func(t *testing.T) {
		composeOut(t, "stop", "canonical-store")
		time.Sleep(3 * time.Second)

		began := time.Now()
		r, _ := e.hazards(t, jar, "type="+typSeismic+"&limit=1")

		if r.code < 500 || r.code > 599 || time.Since(began) > 4*time.Second {
			t.Errorf("with the store down want a quick 5xx, got %d after %s: %.120s", r.code, time.Since(began), r.body)
		}

		if st := e.healthStatus(); st != "degraded" {
			t.Errorf("/health with the store down = %q, want degraded", st)
		}

		composeOut(t, "start", "canonical-store")
		e.waitHealthy(t, 90*time.Second)
	})

	t.Run("user-db", func(t *testing.T) {
		composeOut(t, "stop", "user-db")
		time.Sleep(3 * time.Second)

		began := time.Now()
		r := e.quiet(http.MethodPost, "/auth/login", fmt.Sprintf(`{"email":%q,"password":%q}`, e.operatorEmail, e.operatorPassword), nil, nil)

		if r.code < 500 || r.code > 599 || time.Since(began) > 4*time.Second {
			t.Errorf("login with the user db down: want a quick 5xx, got %d after %s", r.code, time.Since(began))
		}

		if r, p := e.hazards(t, jar, "type="+typSeismic+"&limit=1"); r.code != http.StatusOK || len(p.Data) == 0 {
			t.Errorf("an existing session must keep reading while user-db is down: %d", r.code)
		}

		composeOut(t, "start", "user-db")
		e.waitHealthy(t, 90*time.Second)
	})
}

// Stops one consumer. The others carry on, and the stopped one gets the events published meanwhile once it returns (durable queue).
//
//nolint:paralleltest // shares the docker stack
func TestAbuseConsumerDownCatchesUp(t *testing.T) {
	e := abuseEnv(t)
	e.reset(t)

	composeOut(t, "stop", "notifier")

	audit := logCount(t, "audit-logger", "message processed")

	eventually(t, 30*time.Second, "audit-logger to keep processing while notifier is down", func() bool {
		return logCount(t, "audit-logger", "message processed") > audit
	})

	alerts := logCount(t, "notifier", "alert sent")

	composeOut(t, "start", "notifier")
	eventually(t, 45*time.Second, "notifier to receive the events it missed", func() bool {
		return logCount(t, "notifier", "alert sent") > alerts
	})
}

// Switches PVMBG off with POST /admin/outage. Seismic reads stay normal, volcanic reads are never a 500 or a hang and say it's stale, and data is fresh again after switching back.
//
//nolint:paralleltest // shares the docker stack
func TestAbusePVMBGOutageToggle(t *testing.T) {
	e := abuseEnv(t)
	e.reset(t)

	jar := e.operator(t)

	on := postAdmin(t, e.pvmbgURL, e.pvmbgAdminKey, "/admin/outage")
	if !strings.Contains(on.body, `"outage_active":true`) {
		t.Fatalf("outage did not switch on: %d %s", on.code, on.body)
	}

	defer func() {
		if r := postAdmin(t, e.pvmbgURL, e.pvmbgAdminKey, "/admin/outage"); strings.Contains(r.body, `"outage_active":true`) {
			postAdmin(t, e.pvmbgURL, e.pvmbgAdminKey, "/admin/outage")
		}
	}()

	eventually(t, 45*time.Second, "volcanic reads to report the outage explicitly", func() bool { return e.outageVisible(t, jar) })

	began := time.Now()

	if r, p := e.hazards(t, jar, "type="+typSeismic+"&limit=5"); r.code != http.StatusOK || len(p.Data) == 0 || time.Since(began) > 2*time.Second {
		t.Errorf("seismic read during the PVMBG outage: %d in %s", r.code, time.Since(began))
	}

	off := postAdmin(t, e.pvmbgURL, e.pvmbgAdminKey, "/admin/outage")
	if !strings.Contains(off.body, `"outage_active":false`) {
		t.Fatalf("outage did not switch off: %s", off.body)
	}

	eventually(t, 60*time.Second, "volcanic data to be fresh again (no stale_since) without a restart", func() bool {
		r, p := e.hazards(t, jar, "type=VOLCANIC&limit=5")

		return r.code == http.StatusOK && len(p.Data) > 0 && p.Meta["stale_since"] == nil
	})
}

// True once a volcanic read says PVMBG is down (stale data with a marker, or an explicit 503). Any other 5xx or a hang fails the test.
func (e abEnv) outageVisible(t *testing.T, jar cookies) bool {
	t.Helper()

	r, p := e.hazards(t, jar, "type="+typVolcanic+"&limit=5")
	if r.code >= 500 && r.code != http.StatusServiceUnavailable {
		t.Fatalf("volcanic read during outage: %d %.120s (want data with stale marker, or an explicit 503)", r.code, r.body)
	}

	return r.code == http.StatusServiceUnavailable || p.Meta["stale_since"] != nil || (p.Meta["source_status"] != nil && p.Meta["source_status"] != "UP")
}

// Flips PVMBG to the extended schema with POST /admin/schema-version while running. No restart or crash, old and new records both readable, new records carry confidence_level in attributes.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseSchemaVersionToggle(t *testing.T) {
	e := abuseEnv(t)
	e.reset(t)

	jar := e.operator(t)
	before := e.count(t, "source='PVMBG'")

	r := postAdmin(t, e.pvmbgURL, e.pvmbgAdminKey, "/admin/schema-version")
	if !strings.Contains(r.body, "extended") {
		r = postAdmin(t, e.pvmbgURL, e.pvmbgAdminKey, "/admin/schema-version")
		before = e.count(t, "source='PVMBG'")
		r = postAdmin(t, e.pvmbgURL, e.pvmbgAdminKey, "/admin/schema-version")
	}

	if !strings.Contains(r.body, "extended") {
		t.Fatalf("schema did not switch to extended: %s", r.body)
	}

	defer postAdmin(t, e.pvmbgURL, e.pvmbgAdminKey, "/admin/schema-version")

	eventually(t, 45*time.Second, "new PVMBG records after the schema change", func() bool { return e.count(t, "source='PVMBG'") > before })

	eventually(t, 30*time.Second, "a volcanic record carrying confidence_level", func() bool {
		_, p := e.hazards(t, jar, "type=VOLCANIC&limit=100")
		for _, d := range p.Data {
			if a, ok := d[fieldAttribs].(map[string]any); ok && a["confidence_level"] != nil {
				return true
			}
		}

		return false
	})

	_, p := e.hazards(t, jar, "type=VOLCANIC&limit=100")

	old := 0

	for _, d := range p.Data {
		if a, ok := d[fieldAttribs].(map[string]any); ok && a["confidence_level"] == nil {
			old++
		}
	}

	if old == 0 {
		t.Error("records from before the schema change are gone or unreadable")
	}

	if e.healthStatus() != "ok" {
		t.Errorf("/health after the schema change = %q, want ok", e.healthStatus())
	}
}

// SIGKILLs the Aggregator mid-poll three times. It resumes on its own, every BMKG record is stored exactly once, and the notifier never alerts twice for one hazard and severity.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseKillAggregatorMidPoll(t *testing.T) {
	e := abuseEnv(t)
	e.reset(t)

	for range 3 {
		composeOut(t, "kill", "aggregator")
		time.Sleep(time.Second)
		composeOut(t, "up", "-d", "--wait", "--no-deps", "aggregator")
	}

	e.waitHealthy(t, 90*time.Second)

	for _, id := range e.sourceIDs(t) {
		if e.count(t, "source='BMKG' AND source_ref_id='"+id+"'") != 1 {
			t.Errorf("BMKG record %s missing or duplicated after the crashes", id)
		}
	}

	seen := map[string]int{}

	for l := range strings.SplitSeq(composeOut(t, "logs", "--no-log-prefix", "notifier"), "\n") {
		if !strings.Contains(l, `"message":"alert sent"`) {
			continue
		}

		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil {
			seen[fmt.Sprint(m["hazard_id"], "/", m["severity"])]++
		}
	}

	for k, n := range seen {
		if n > 1 {
			t.Errorf("notifier alerted %d times for %s", n, k)
		}
	}
}

// The event ids BMKG's mock serves right now.
func (e abEnv) sourceIDs(t *testing.T) []string {
	t.Helper()

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, e.bmkgURL+"/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", e.bmkgKey)

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("bmkg: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	var events []struct {
		EventID string `json:"event_id"`
	}

	raw, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(raw, &events); err != nil || len(events) == 0 {
		t.Fatalf("bmkg seismic-events: %v %.200s", err, raw)
	}

	ids := make([]string, 0, len(events))
	for _, d := range events {
		ids = append(ids, d.EventID)
	}

	return ids
}

// Uses each source's credential on the other source. Every try is 401 or 403.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseCredentialsDoNotCrossDomains(t *testing.T) {
	e := abuseEnv(t)

	cases := []struct {
		name, url string
		hdr       map[string]string
	}{
		{"bmkg key on pvmbg", e.pvmbgURL + "/volcanic-reports", map[string]string{"X-BMKG-Key": e.bmkgKey}},
		{"bmkg key as bearer on pvmbg", e.pvmbgURL + "/volcanic-reports", map[string]string{"Authorization": "Bearer " + e.bmkgKey}},
		{"pvmbg admin key on pvmbg data", e.pvmbgURL + "/volcanic-reports", map[string]string{"X-Admin-Key": e.pvmbgAdminKey}},
		{"bearer on bmkg", e.bmkgURL + "/seismic-events", map[string]string{"Authorization": "Bearer " + e.pvmbgAdminKey}},
		{"pvmbg admin key as bmkg key", e.bmkgURL + "/seismic-events", map[string]string{"X-BMKG-Key": e.pvmbgAdminKey}},
		{"no credential on bmkg", e.bmkgURL + "/seismic-events", nil},
		{"no credential on pvmbg", e.pvmbgURL + "/volcanic-reports", nil},
		{"bmkg key on pvmbg admin", e.pvmbgURL + "/admin/outage", map[string]string{"X-Admin-Key": e.bmkgKey}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			method := http.MethodGet
			if strings.HasSuffix(c.url, "/admin/outage") {
				method = http.MethodPost
			}

			req, _ := http.NewRequestWithContext(context.Background(), method, c.url, nil)
			for k, v := range c.hdr {
				req.Header.Set(k, v)
			}

			resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
			if err != nil {
				t.Fatal(err)
			}

			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
				t.Errorf("got %d, want 401 or 403", resp.StatusCode)
			}
		})
	}
}

// A Media client asks for raw fields every way it can. Explicit raw fields are refused (401/403), the default is summary only, and forged or tampered tokens are 401.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseMediaCannotReachRawFields(t *testing.T) {
	e := abuseEnv(t)
	e.requireAccountRPCs(t)

	email, pw := freshEmail("media"), "pw-media-1234"
	if r := e.do(t, http.MethodPost, "/auth/register", fmt.Sprintf(`{"email":%q,"password":%q}`, email, pw), nil); r.code != http.StatusCreated {
		t.Fatalf("register: %d %s", r.code, r.body)
	}

	_, jar := e.login(t, email, pw)

	for _, f := range append(slices.Clone(rawFields), "hazard_id,"+rawFields[1]) {
		if r, _ := e.hazards(t, jar, "type="+typSeismic+"&fields="+f); r.code != http.StatusForbidden && r.code != http.StatusUnauthorized {
			t.Errorf("Media asking for fields=%s got %d, want 401/403", f, r.code)
		}
	}

	_, p := e.hazards(t, jar, "type="+typSeismic+"&limit=3")
	for _, d := range p.Data {
		for _, raw := range rawFields {
			if _, leaked := d[raw]; leaked {
				t.Errorf("Media got raw field %q in the default response", raw)
			}
		}
	}

	parts := strings.Split(jar[cookieJWT], ".")
	if len(parts) != 3 {
		t.Fatalf("jwt has %d parts", len(parts))
	}

	enc := func(v any) string {
		b, _ := json.Marshal(v)

		return base64.RawURLEncoding.EncodeToString(b)
	}

	var claims map[string]any

	raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	_ = json.Unmarshal(raw, &claims)
	claims["role"] = "Operator"

	forged := map[string]string{
		"role changed, old signature": parts[0] + "." + enc(claims) + "." + parts[2],
		"alg none":                    enc(map[string]string{"alg": "none", "typ": "JWT"}) + "." + enc(claims) + ".",
		"garbage":                     "abc.def.ghi",
		"empty":                       "",
		"70KB cookie":                 strings.Repeat("a", 70000),
	}

	for name, tok := range forged {
		if r := e.quiet(http.MethodGet, "/me", "", cookies{cookieJWT: tok}, nil); r.code != http.StatusUnauthorized {
			t.Errorf("%s: got %d, want 401", name, r.code)
		}
	}

	if r := e.quiet(http.MethodGet, "/me", "", nil, map[string]string{"Authorization": "Bearer " + jar[cookieJWT]}); r.code != http.StatusUnauthorized {
		t.Errorf("a Bearer header must not replace the cookie: got %d", r.code)
	}
}

// Sends malformed login and register bodies. Every one is a 4xx, a client mistake never becomes a 5xx.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseAuthInputIsRejectedCleanly(t *testing.T) {
	e := abuseEnv(t)
	e.requireAccountRPCs(t)

	in4xx := func(t *testing.T, r reply) {
		t.Helper()

		if r.code < 400 || r.code > 499 {
			t.Errorf("got %d %.100s, want a 4xx", r.code, r.body)
		}
	}

	bodies := map[string]string{
		"not json":          "hello",
		"empty object":      "{}",
		"email is an array": `{"email":["a"],"password":1}`,
		"1MB body":          `{"email":"` + strings.Repeat("a", 1<<20) + `"}`,
		"5000 char email":   `{"email":"` + strings.Repeat("a", 5000) + `@x.test","password":"pw-123456"}`,
		"sql in email":      `{"email":"' OR '1'='1","password":"' OR '1'='1"}`,
	}

	for name, b := range bodies {
		for _, path := range []string{"/auth/login", "/auth/register"} {
			t.Run(name+" "+path, func(t *testing.T) { in4xx(t, e.do(t, http.MethodPost, path, b, nil)) })
		}
	}

	t.Run("NUL byte in login email", func(t *testing.T) {
		in4xx(t, e.do(t, http.MethodPost, "/auth/login", `{"email":"a\u0000b","password":"x"}`, nil))
	})
	t.Run("NUL byte in register email", func(t *testing.T) {
		in4xx(t, e.do(t, http.MethodPost, "/auth/register", `{"email":"a\u0000b@x.test","password":"pw-123456"}`, nil))
	})
	t.Run("email that is not an email", func(t *testing.T) {
		in4xx(t, e.do(t, http.MethodPost, "/auth/register", `{"email":"notanemail","password":"pw-123456"}`, nil))
	})
	t.Run("one character password", func(t *testing.T) {
		in4xx(t, e.do(t, http.MethodPost, "/auth/register", fmt.Sprintf(`{"email":%q,"password":"1"}`, freshEmail("weak")), nil))
	})
	t.Run("same email in another case", func(t *testing.T) {
		email := freshEmail("case")
		e.do(t, http.MethodPost, "/auth/register", fmt.Sprintf(`{"email":%q,"password":"pw-123456"}`, email), nil)

		if r := e.do(t, http.MethodPost, "/auth/register", fmt.Sprintf(`{"email":%q,"password":"pw-123456"}`, strings.ToUpper(email)), nil); r.code != http.StatusConflict {
			t.Errorf("registering %s again in upper case: got %d, want 409", strings.ToUpper(email), r.code)
		}
	})
	t.Run("register cannot pick a role", func(t *testing.T) {
		email := freshEmail("role")
		e.do(t, http.MethodPost, "/auth/register", fmt.Sprintf(`{"email":%q,"password":"pw-123456","role":"Operator"}`, email), nil)
		_, jar := e.login(t, email, "pw-123456")

		if r := e.do(t, http.MethodGet, "/me", "", jar); !strings.Contains(r.body, `"Media"`) {
			t.Errorf("self-registration with role=Operator gave %s, want Media", r.body)
		}
	})
}

// Sends bad query parameters to /hazards. Each is a 400 and never a 5xx, including SQL metacharacters, NUL, emoji and a 100KB search.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseQueryParametersAreValidatedAlike(t *testing.T) {
	e := abuseEnv(t)
	jar := e.operator(t)

	for _, q := range []string{"offset=-1", "offset=abc", "offset=99999999999999999999", "limit=0", "limit=-5", "limit=1000000", "limit=abc", "severity=EVIL"} {
		t.Run("400 for "+q, func(t *testing.T) {
			if r, _ := e.hazards(t, jar, "type="+typSeismic+"&"+q); r.code != http.StatusBadRequest {
				t.Errorf("got %d, want 400", r.code)
			}
		})
	}

	t.Run("400 for a lowercase type", func(t *testing.T) {
		if r, _ := e.hazards(t, jar, "type=seismic"); r.code != http.StatusBadRequest {
			t.Errorf("got %d, want 400", r.code)
		}
	})

	for _, s := range []string{"%27%20OR%201%3D1--", "%00", "%25", "_", "%F0%9F%92%A5", strings.Repeat("A", 100000), ";drop"} {
		t.Run("no 5xx for search "+s[:min(len(s), 12)], func(t *testing.T) {
			if r, _ := e.hazards(t, jar, "type="+typSeismic+"&q="+s); r.code >= 500 {
				t.Errorf("search got %d", r.code)
			}
		})
	}
}

// Sends hostile X-Correlation-ID values. A reasonable one is echoed, an oversized or malformed one is replaced by a generated id.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseCorrelationIDIsSane(t *testing.T) {
	e := abuseEnv(t)

	get := func(id string) (int, string) {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, e.url+"/health", nil)
		req.Header.Set("X-Correlation-ID", id)

		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			t.Fatal(err)
		}

		_ = resp.Body.Close()

		return resp.StatusCode, resp.Header.Get("X-Correlation-ID")
	}

	if code, got := get("abuse-corr-1"); code != http.StatusOK || got != "abuse-corr-1" {
		t.Errorf("a normal id: %d %q, want it echoed", code, got)
	}

	for name, id := range map[string]string{"100KB": strings.Repeat("x", 100000), "spaces and quotes": `a b"}{`} {
		if code, got := get(id); code != http.StatusOK || got == id || got == "" || len(got) > 64 {
			t.Errorf("%s: %d %.40q, want a generated id", name, code, got)
		}
	}
}

// Hammers the API with one account until limited. That account gets 429 with Retry-After, a second account is still served, and the first recovers after the window.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseRateLimitIsPerAccount(t *testing.T) {
	e := abuseEnv(t)
	e.requireAccountRPCs(t)

	mk := func() cookies {
		email, pw := freshEmail("rl"), "pw-rate-12345"
		e.do(t, http.MethodPost, "/auth/register", fmt.Sprintf(`{"email":%q,"password":%q}`, email, pw), nil)
		_, jar := e.login(t, email, pw)

		return jar
	}

	loud, quiet := mk(), mk()

	limited := false

	for range 400 {
		r := e.quiet(http.MethodGet, "/hazards?type="+typSeismic+"&limit=1", "", loud, nil)
		if r.code == http.StatusTooManyRequests {
			limited = true

			break
		}
	}

	if !limited {
		t.Fatal("400 back-to-back requests from one account were never limited")
	}

	if r, _ := e.hazards(t, quiet, "type="+typSeismic+"&limit=1"); r.code != http.StatusOK {
		t.Errorf("a second account was affected by the first one's flood: %d", r.code)
	}

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, e.url+"/hazards?type="+typSeismic+"&limit=1", nil)
	req.AddCookie(&http.Cookie{Name: cookieJWT, Value: loud[cookieJWT]}) //nolint:gosec // test client

	for range 400 {
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			t.Fatal(err)
		}

		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			if resp.Header.Get("Retry-After") == "" {
				t.Error("429 without Retry-After")
			}

			break
		}
	}

	time.Sleep(2 * time.Second)

	if r, _ := e.hazards(t, loud, "type="+typSeismic+"&limit=1"); r.code != http.StatusOK {
		t.Errorf("the limited account is not served again after the window: %d", r.code)
	}
}

// Drives the API with 50 parallel connections for IT_LOAD_SECONDS, half on volcanic data (PVMBG is slow here) and half on seismic only. Seismic p95 stays under 300 ms, non-429 errors stay under 1%, and the stack is healthy afterwards.
//
//nolint:paralleltest // shares the docker stack
func TestAbuseSustainedLoadAndHeadOfLine(t *testing.T) {
	e := abuseEnv(t)
	e.requireAccountRPCs(t)
	e.reset(t)

	secs, _ := strconv.Atoi(os.Getenv("IT_LOAD_SECONDS"))
	if secs <= 0 {
		secs = 15
	}

	const accounts, perAccount = 10, 5

	jars := make([]cookies, accounts)

	for i := range jars {
		email, pw := freshEmail("load"), "pw-load-12345"
		e.do(t, http.MethodPost, "/auth/register", fmt.Sprintf(`{"email":%q,"password":%q}`, email, pw), nil)
		_, jars[i] = e.login(t, email, pw)
	}

	var (
		mu                         sync.Mutex
		seismic, volcanic          []float64
		ok, limited, failed, total atomic.Int64
		codes                      = map[int]int{}
	)

	stop := time.Now().Add(time.Duration(secs) * time.Second)

	var wg sync.WaitGroup

	for i := range accounts * perAccount {
		wg.Go(func() {
			typ := typSeismic
			if i%2 == 1 {
				typ = typVolcanic
			}

			for time.Now().Before(stop) {
				began := time.Now()
				r := e.quiet(http.MethodGet, "/hazards?type="+typ+"&limit=20", "", jars[i%accounts], nil)
				took := time.Since(began).Seconds() * 1000

				total.Add(1)

				switch r.code {
				case http.StatusOK:
					ok.Add(1)
					mu.Lock()
					if typ == "SEISMIC" {
						seismic = append(seismic, took)
					} else {
						volcanic = append(volcanic, took)
					}
					mu.Unlock()
				case http.StatusTooManyRequests:
					limited.Add(1)
				default:
					failed.Add(1)
					mu.Lock()
					codes[r.code]++
					mu.Unlock()
				}

				time.Sleep(120 * time.Millisecond)
			}
		})
	}

	wg.Wait()

	pct := func(v []float64, p float64) float64 {
		if len(v) == 0 {
			return 0
		}

		sort.Float64s(v)

		return v[min(len(v)-1, int(float64(len(v))*p))]
	}

	t.Logf("load %ds, %d conns: %d requests, %.0f req/s, 200=%d 429=%d other=%d", secs, accounts*perAccount, total.Load(), float64(total.Load())/float64(secs), ok.Load(), limited.Load(), failed.Load())
	t.Logf("non-200, non-429 responses by status (-1 = no answer): %v", codes)
	t.Logf("seismic  p50=%.0fms p95=%.0fms p99=%.0fms", pct(seismic, .5), pct(seismic, .95), pct(seismic, .99))
	t.Logf("volcanic p50=%.0fms p95=%.0fms p99=%.0fms", pct(volcanic, .5), pct(volcanic, .95), pct(volcanic, .99))

	if p95 := pct(seismic, .95); p95 >= 300 {
		t.Errorf("seismic p95 = %.0fms with slow PVMBG traffic alongside, want < 300ms", p95)
	}

	if rate := float64(failed.Load()) / float64(max(1, total.Load()-limited.Load())); rate >= 0.01 {
		t.Errorf("error rate excluding 429 = %.2f%%, want < 1%%", rate*100)
	}

	e.waitHealthy(t, 30*time.Second)
}
