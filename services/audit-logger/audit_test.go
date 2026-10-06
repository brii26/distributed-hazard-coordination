package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func newTestAuditor(t *testing.T, dir string) *auditor {
	t.Helper()

	dedup, audit, err := openStores(filepath.Join(dir, "dedup.log"), filepath.Join(dir, "audit.log"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = dedup.close(); _ = audit.close() })

	return &auditor{log: audit, dedup: dedup}
}

func msgFor(id, severity, extra string) message {
	body := fmt.Sprintf(`{"hazard_id":%q,"severity":%q%s}`, id, severity, extra)

	return message{body: []byte(body), correlationID: "c-" + id}
}

// lines reads the audit log back.
func lines(t *testing.T, dir string) []entry {
	t.Helper()

	f, err := os.Open(filepath.Join(dir, "audit.log")) //nolint:gosec // dir is the test's own temp dir
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = f.Close() }()

	var out []entry

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var e entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("the audit log must be one JSON object per line: %v: %s", err, sc.Text())
		}

		out = append(out, e)
	}

	return out
}

func TestEveryNewEventIsAppendedOnce(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	a := newTestAuditor(t, dir)

	for _, m := range []message{msgFor("h1", "SIAGA", ""), msgFor("h1", "SIAGA", ""), msgFor("h1", "AWAS", ""), msgFor("h2", "NORMAL", "")} {
		if out := a.handle(m); out != ack {
			t.Fatalf("expected ack, got %v", out)
		}
	}

	got := lines(t, dir)
	if len(got) != 3 {
		t.Fatalf("expected h1/SIAGA, h1/AWAS and h2 once each, got %d entries", len(got))
	}

	if got[0].CorrelationID != "c-h1" || got[0].ReceivedAt.IsZero() {
		t.Fatalf("an entry must carry the correlation ID and when it arrived, got %+v", got[0])
	}
}

func TestTheEventIsKeptAsReceived(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	a := newTestAuditor(t, dir)

	a.handle(msgFor("h1", "SIAGA", `,"confidence_level":0.8,"area_name":"Gunung Merapi"`))

	var event map[string]any
	if err := json.Unmarshal(lines(t, dir)[0].Event, &event); err != nil {
		t.Fatal(err)
	}

	if event["confidence_level"] != 0.8 || event["area_name"] != "Gunung Merapi" {
		t.Fatalf("fields the logger does not know must still be logged, got %v", event)
	}
}

func TestDedupSurvivesARestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	newTestAuditor(t, dir).handle(msgFor("h1", "AWAS", ""))
	newTestAuditor(t, dir).handle(msgFor("h1", "AWAS", "")) // redelivered after a restart

	if got := len(lines(t, dir)); got != 1 {
		t.Fatalf("a redelivery after a restart must not be logged again, got %d entries", got)
	}
}

func TestAFailedAppendRequeuesAndRecordsNothing(t *testing.T) {
	t.Parallel()

	a := newTestAuditor(t, t.TempDir())
	_ = a.log.file.Close()

	if out := a.handle(msgFor("h1", "AWAS", "")); out != requeue {
		t.Fatalf("expected requeue, got %v", out)
	}

	if a.dedup.has(dedupKey{"h1", "AWAS"}) {
		t.Fatal("a message that was not logged must not count as seen")
	}
}

func TestMalformedMessagesAreDropped(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	a := newTestAuditor(t, dir)

	for _, body := range []string{"not json", `{"severity":"AWAS"}`} {
		if out := a.handle(message{body: []byte(body)}); out != drop {
			t.Fatalf("%q: expected drop, got %v", body, out)
		}
	}

	if got := len(lines(t, dir)); got != 0 {
		t.Fatalf("nothing must be logged, got %d entries", got)
	}
}
