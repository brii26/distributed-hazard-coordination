package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// Records what a notifier sent.
type alerts struct {
	mu   sync.Mutex
	sent []string
}

func (a *alerts) send(e HazardEvent, _ string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.sent = append(a.sent, e.HazardID+"/"+orDash(e.Severity))
}

func (a *alerts) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	return len(a.sent)
}

func newTestNotifier(t *testing.T, path string) (*notifier, *alerts) {
	t.Helper()

	d, err := openDedup(path)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = d.close() })

	a := &alerts{}

	return &notifier{dedup: d, send: a.send}, a
}

func msgFor(t *testing.T, id, severity string) message {
	t.Helper()

	var sev *string
	if severity != "" {
		sev = &severity
	}

	b, err := json.Marshal(HazardEvent{HazardID: id, HazardType: "VOLCANIC", Severity: sev})
	if err != nil {
		t.Fatal(err)
	}

	return message{body: b, correlationID: "c-" + id}
}

func TestRedeliveryDoesNotAlertTwice(t *testing.T) {
	t.Parallel()

	n, a := newTestNotifier(t, filepath.Join(t.TempDir(), "dedup.log"))
	m := msgFor(t, "h1", "SIAGA")

	for range 3 {
		if out := n.handle(m); out != ack {
			t.Fatalf("expected ack, got %v", out)
		}
	}

	if a.count() != 1 {
		t.Fatalf("expected one alert for three deliveries, got %v", a.sent)
	}
}

func TestAnEscalationIsANewAlert(t *testing.T) {
	t.Parallel()

	n, a := newTestNotifier(t, filepath.Join(t.TempDir(), "dedup.log"))

	n.handle(msgFor(t, "h1", "SIAGA"))
	n.handle(msgFor(t, "h1", "AWAS"))
	n.handle(msgFor(t, "h1", "AWAS"))

	if a.count() != 2 || a.sent[1] != "h1/AWAS" {
		t.Fatalf("expected the SIAGA alert and one AWAS alert, got %v", a.sent)
	}
}

func TestDedupStateSurvivesARestart(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sub", "dedup.log")

	first, _ := newTestNotifier(t, path)
	first.handle(msgFor(t, "h1", "AWAS"))
	first.handle(msgFor(t, "h2", ""))

	restarted, a := newTestNotifier(t, path)

	restarted.handle(msgFor(t, "h1", "AWAS"))
	restarted.handle(msgFor(t, "h2", ""))

	if a.count() != 0 {
		t.Fatalf("already-processed messages redelivered after a restart must be skipped, got %v", a.sent)
	}

	restarted.handle(msgFor(t, "h1", "NORMAL"))

	if a.count() != 1 {
		t.Fatalf("a new severity after the restart must still alert, got %v", a.sent)
	}
}

func TestATruncatedLastLineIsSkippedOnLoad(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "dedup.log")
	if err := os.WriteFile(path, []byte("h1\tAWAS\nh2"), 0o600); err != nil {
		t.Fatal(err)
	}

	n, a := newTestNotifier(t, path)

	n.handle(msgFor(t, "h1", "AWAS"))
	n.handle(msgFor(t, "h2", "SIAGA"))

	if a.count() != 1 || a.sent[0] != "h2/SIAGA" {
		t.Fatalf("expected h1 skipped and h2 alerted, got %v", a.sent)
	}
}

func TestADedupWriteFailureRequeuesAndForgetsNothing(t *testing.T) {
	t.Parallel()

	n, _ := newTestNotifier(t, filepath.Join(t.TempDir(), "dedup.log"))
	_ = n.dedup.file.Close()

	if out := n.handle(msgFor(t, "h1", "AWAS")); out != requeue {
		t.Fatalf("a message whose dedup record could not be written must be requeued, got %v", out)
	}

	if n.dedup.has(dedupKey{"h1", "AWAS"}) {
		t.Fatal("a key must only count as seen once it is on disk")
	}
}

func TestMalformedMessagesAreDropped(t *testing.T) {
	t.Parallel()

	n, a := newTestNotifier(t, filepath.Join(t.TempDir(), "dedup.log"))

	for _, body := range []string{"not json", `{"severity":"AWAS"}`} {
		if out := n.handle(message{body: []byte(body)}); out != drop {
			t.Fatalf("%q: expected drop, got %v", body, out)
		}
	}

	if a.count() != 0 {
		t.Fatal("a malformed message must not alert")
	}
}

func TestAnUnusableDedupPathFailsToOpen(t *testing.T) {
	t.Parallel()

	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := openDedup(filepath.Join(blocker, "dedup.log")); err == nil {
		t.Fatal("a dedup path under a regular file must fail")
	}
}
