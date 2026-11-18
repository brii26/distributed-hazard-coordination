//nolint:paralleltest,noctx,gosec,errcheck,forcetypeassert // needs the broker and runs one test at a time; helper calls are plain test plumbing
package main

// Field report inbox against a real RabbitMQ. Needs RABBITMQ_URL (amqp url reachable from here) and IT_COMPOSE (the docker compose command prefix that controls the broker, e.g. "docker compose -p x"), otherwise every test skips. Each test starts the real client-facing-api binary.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const itSecret = "it-secret"

type itAPI struct {
	url string
	cmd *exec.Cmd
}

func startAPI(t *testing.T) *itAPI {
	t.Helper()

	url := os.Getenv("RABBITMQ_URL")
	if url == "" || os.Getenv("IT_COMPOSE") == "" {
		t.Skip("RABBITMQ_URL or IT_COMPOSE not set")
	}

	bin := filepath.Join(t.TempDir(), "cfa")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()

	cmd := exec.Command(bin)

	cmd.Env = append(os.Environ(), "PORT="+port, "AGGREGATOR_GRPC_ADDR=127.0.0.1:1", "AUTH_SERVICE_GRPC_ADDR=127.0.0.1:1", "JWT_SIGNING_SECRET="+itSecret,
		"RATE_LIMIT_RPS=100000", "MAX_INFLIGHT_REQUESTS=1000", "COOKIE_SECURE=false")

	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	api := &itAPI{url: "http://127.0.0.1:" + port, cmd: cmd}

	for range 100 {
		if c, err := net.Dial("tcp", "127.0.0.1:"+port); err == nil {
			_ = c.Close()

			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	return api
}

func brokerCmd(t *testing.T, args ...string) {
	t.Helper()

	f := strings.Fields(os.Getenv("IT_COMPOSE"))
	if out, err := exec.Command(f[0], append(f[1:], args...)...).CombinedOutput(); err != nil { //nolint:gosec // test-only, command comes from the developer's env
		t.Fatalf("compose %v: %v\n%s", args, err, out)
	}
}

func (a *itAPI) post(role, key, body string) (int, string, http.Header, error) {
	req, _ := http.NewRequest(http.MethodPost, a.url+"/field-reports", strings.NewReader(body))

	if role != "" {
		c := jwtClaims{Name: "N", Email: "it@x.example", Role: role, Iat: time.Now().Unix(), Exp: time.Now().Add(time.Hour).Unix()}
		req.AddCookie(&http.Cookie{Name: cookieJWT, Value: signIT(c)})
	}

	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	var b bytes.Buffer

	_, _ = b.ReadFrom(resp.Body)

	return resp.StatusCode, b.String(), resp.Header, nil
}

func signIT(c jwtClaims) string {
	payload, _ := json.Marshal(c)
	signing := b64.EncodeToString([]byte(hs256)) + "." + b64.EncodeToString(payload)

	return signing + "." + b64.EncodeToString(hmacSHA256([]byte(itSecret), signing))
}

// Fresh channel on its own connection, queue emptied.
func inspector(t *testing.T) *amqp.Channel {
	t.Helper()

	conn, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	ch, _ := conn.Channel()
	if _, err := ch.QueueDeclare(reportQueue, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}

	return ch
}

func depth(t *testing.T, ch *amqp.Channel) int {
	t.Helper()

	q, err := ch.QueueDeclarePassive(reportQueue, true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	return q.Messages
}

func waitUp(t *testing.T, a *itAPI) {
	t.Helper()

	for range 100 {
		if code, _, _, err := a.post(roleLapangan, "probe-"+strconv.FormatInt(time.Now().UnixNano(), 10), `{"description":"probe"}`); err == nil && code == 202 {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatal("API never reached the broker")
}

func TestITFiveReportsWaitInTheQueue(t *testing.T) {
	a := startAPI(t)
	ch := inspector(t)
	_, _ = ch.QueuePurge(reportQueue, false)

	waitUp(t, a)

	_, _ = ch.QueuePurge(reportQueue, false)

	for i := range 5 {
		if code, _, _, _ := a.post(roleOperator, "", fmt.Sprintf(`{"description":"r%d","kind":"flood"}`, i)); code != 202 {
			t.Fatalf("report %d: %d", i, code)
		}
	}

	if n := depth(t, ch); n != 5 {
		t.Fatalf("queue has %d, want 5", n)
	}

	d, _, _ := ch.Get(reportQueue, true)

	var e reportEnvelope
	if err := json.Unmarshal(d.Body, &e); err != nil || e.ReportID != d.MessageId || e.ReporterRole != roleOperator || d.DeliveryMode != amqp.Persistent || d.CorrelationId == "" {
		t.Fatalf("bad message %+v %+v %v", e, d, err)
	}
}

func TestITLatencyP95(t *testing.T) {
	a := startAPI(t)
	ch := inspector(t)
	waitUp(t, a)

	_, _ = ch.QueuePurge(reportQueue, false)

	var (
		mu   sync.Mutex
		lats []time.Duration
		wg   sync.WaitGroup
	)

	sem := make(chan struct{}, 20)

	for range 300 {
		sem <- struct{}{}

		wg.Go(func() {
			defer func() { <-sem }()

			start := time.Now()
			code, _, _, err := a.post(roleLapangan, "", `{"description":"load"}`)

			mu.Lock()
			defer mu.Unlock()

			if err != nil || code != 202 {
				t.Errorf("code %d err %v", code, err)
			}

			lats = append(lats, time.Since(start))
		})
	}

	wg.Wait()
	slices.Sort(lats)

	p95 := lats[len(lats)*95/100]
	t.Logf("300 requests, 20 parallel: p50 %v p95 %v max %v, queue depth %d", lats[150], p95, lats[299], depth(t, ch))

	if p95 >= 200*time.Millisecond {
		t.Fatalf("p95 %v", p95)
	}
}

func TestITDurableAcrossBrokerRestart(t *testing.T) {
	a := startAPI(t)
	ch := inspector(t)
	waitUp(t, a)

	_, _ = ch.QueuePurge(reportQueue, false)

	if code, _, _, _ := a.post(roleLapangan, "", `{"description":"survive"}`); code != 202 {
		t.Fatal(code)
	}

	brokerCmd(t, "restart", "rabbitmq")

	var n int

	for range 60 {
		time.Sleep(time.Second)

		if conn, err := amqp.Dial(os.Getenv("RABBITMQ_URL")); err == nil {
			c, _ := conn.Channel()
			n = depth(t, c)
			_ = conn.Close()

			break
		}
	}

	if n != 1 {
		t.Fatalf("queue has %d after restart, want 1", n)
	}
}

func TestITBrokerDownIs503ThenRecovers(t *testing.T) {
	a := startAPI(t)
	waitUp(t, a)
	brokerCmd(t, "stop", "rabbitmq")

	defer brokerCmd(t, "start", "rabbitmq")

	var code int

	var h http.Header

	for range 20 { // the API notices the closed connection within a moment
		start := time.Now()
		code, _, h, _ = a.post(roleLapangan, "", `{"description":"down"}`)

		if code == 503 {
			t.Logf("503 after %v, Retry-After %q", time.Since(start), h.Get("Retry-After"))

			if time.Since(start) > time.Second || h.Get("Retry-After") != "1" {
				t.Fatal("503 too slow or no Retry-After")
			}

			break
		}

		if code == 202 {
			t.Fatal("202 while the broker is stopped")
		}

		time.Sleep(100 * time.Millisecond)
	}

	if code != 503 {
		t.Fatalf("never 503, last %d", code)
	}

	brokerCmd(t, "start", "rabbitmq")
	waitUp(t, a)
}

func TestITSameKeyTwiceQueuesOnce(t *testing.T) {
	a := startAPI(t)
	ch := inspector(t)
	waitUp(t, a)

	_, _ = ch.QueuePurge(reportQueue, false)

	c1, b1, _, _ := a.post(roleLapangan, "same-1", `{"description":"x"}`)
	c2, b2, _, _ := a.post(roleLapangan, "same-1", `{"description":"x"}`)
	c3, _, _, _ := a.post(roleLapangan, "same-1", `{"description":"y"}`)

	if c1 != 202 || c2 != 202 || b1 != b2 || c3 != 409 || depth(t, ch) != 1 {
		t.Fatalf("codes %d %d %d bodies %s %s depth %d", c1, c2, c3, b1, b2, depth(t, ch))
	}
}

func TestITAuthAndBadRequests(t *testing.T) {
	a := startAPI(t)
	waitUp(t, a)

	for _, c := range []struct {
		role, body string
		want       int
	}{{"", `{"description":"x"}`, 401}, {roleMedia, `{"description":"x"}`, 403}, {roleLapangan, `{"description":""}`, 400}, {roleLapangan, `{"description":"x","latitude":95,"longitude":0}`, 400}} {
		if code, _, _, _ := a.post(c.role, "", c.body); code != c.want {
			t.Errorf("role %q body %s: %d, want %d", c.role, c.body, code, c.want)
		}
	}
}

func TestITKillAfter202KeepsTheMessage(t *testing.T) {
	a := startAPI(t)
	ch := inspector(t)
	waitUp(t, a)

	_, _ = ch.QueuePurge(reportQueue, false)

	if code, _, _, _ := a.post(roleLapangan, "kill-1", `{"description":"before kill"}`); code != 202 {
		t.Fatal(code)
	}

	if err := a.cmd.Process.Kill(); err != nil { // SIGKILL
		t.Fatal(err)
	}

	_ = a.cmd.Wait()

	d, ok, _ := ch.Get(reportQueue, false)
	if !ok || d.MessageId != "kill-1" {
		t.Fatalf("message gone after kill -9: ok=%v %+v", ok, d)
	}
}
