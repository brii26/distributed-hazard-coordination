package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/brii26/distributed-hazard-coordination/services/aggregator/aggregatorpb"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testEmail   = "secret.reporter@example.org"
	keyReportID = "report_id"
)

func reportBody(id string, edit func(m map[string]any)) []byte {
	m := map[string]any{
		keyReportID: id, "description": "banjir di jalan", "latitude": -6.2, "longitude": 106.8,
		"reporter_id": testEmail, "reporter_role": "warga", "received_at": "2026-11-18T10:00:00Z",
	}
	if edit != nil {
		edit(m)
	}

	return []byte(toJSON(m))
}

func toJSON(m map[string]any) string {
	var b strings.Builder

	b.WriteString("{")

	first := true

	for k, v := range m {
		if !first {
			b.WriteString(",")
		}

		first = false

		fmt.Fprintf(&b, "%q:%s", k, jsonValue(v))
	}

	b.WriteString("}")

	return b.String()
}

func jsonValue(v any) string {
	switch t := v.(type) {
	case string:
		return fmt.Sprintf("%q", t)
	default:
		return fmt.Sprint(t)
	}
}

// Turns the marker NULMARK into the JSON escape of a NUL: valid JSON and valid UTF-8, but Postgres refuses it.
func withNUL(body []byte) []byte {
	return bytes.ReplaceAll(body, []byte("NULMARK"), []byte(`\u0000`))
}

func TestStoreRejectionIsNotRetryable(t *testing.T) {
	t.Parallel()

	for code, want := range map[string]codes.Code{
		"22021": codes.InvalidArgument, // invalid byte sequence (NUL)
		"22P05": codes.InvalidArgument, // untranslatable character
		"23514": codes.InvalidArgument, // check violation
		"08006": codes.Unavailable,     // connection failure
		"57014": codes.Unavailable,     // query cancelled
	} {
		f := &fakeFields{rows: map[string]bool{}, saveEr: &pgconn.PgError{Code: code}}
		api := &readAPI{fields: f.wire()}

		_, err := api.SaveFieldReport(context.Background(), &aggregatorpb.SaveFieldReportRequest{MessageId: "r1", Body: reportBody("r1", nil)})
		if status.Code(err) != want {
			t.Errorf("SQLSTATE %s: %v, want %v", code, status.Code(err), want)
		}
	}

	// Both owners fail in cluster mode: the join still carries the Postgres error.
	joined := errors.Join(context.DeadlineExceeded, &pgconn.PgError{Code: "22021"})
	if !rejectedByStore(joined) || rejectedByStore(context.DeadlineExceeded) || rejectedByStore(nil) {
		t.Fatal("rejectedByStore misreads the error")
	}
}

func TestParseFieldReport(t *testing.T) {
	t.Parallel()

	good := reportBody("r1", nil)
	if _, err := parseFieldReport("r1", good); err != nil {
		t.Fatalf("good report rejected: %v", err)
	}

	cases := map[string][]byte{
		"bad json":       []byte(`{"report_id":`),
		"trailing":       append(append([]byte{}, good...), []byte(`{}`)...),
		"wrong type":     reportBody("r1", func(m map[string]any) { m["description"] = 5 }),
		"empty desc":     reportBody("r1", func(m map[string]any) { m["description"] = "  " }),
		"long desc":      reportBody("r1", func(m map[string]any) { m["description"] = strings.Repeat("x", 4097) }),
		"id mismatch":    reportBody("other", nil),
		"lat only":       reportBody("r1", func(m map[string]any) { delete(m, "longitude") }),
		"lat range":      reportBody("r1", func(m map[string]any) { m["latitude"] = 91 }),
		"no reporter":    reportBody("r1", func(m map[string]any) { m["reporter_id"] = "" }),
		"bad received":   reportBody("r1", func(m map[string]any) { m["received_at"] = "yesterday" }),
		"invalid utf8":   append([]byte(`{"report_id":"r1","description":"`), 0xff, 0xfe, '"', '}'),
		"NUL in desc":    withNUL(reportBody("r1", func(m map[string]any) { m["description"] = "banjir NULMARK x" })),
		"NUL in kind":    withNUL(reportBody("r1", func(m map[string]any) { m["kind"] = "a NULMARK b" })),
		"NUL in role":    withNUL(reportBody("r1", func(m map[string]any) { m["reporter_role"] = "NULMARK" })),
		"not an object":  []byte(`[1,2]`),
		"empty body":     nil,
		"description 4k": reportBody("r1", func(m map[string]any) { m["description"] = strings.Repeat("é", 2049) }),
	}

	for name, body := range cases {
		_, err := parseFieldReport("r1", body)
		if err == nil {
			t.Errorf("%s: accepted", name)

			continue
		}

		if strings.Contains(err.Error(), testEmail) {
			t.Errorf("%s: error leaks the e-mail: %v", name, err)
		}
	}

	// boundary: exactly 4096 bytes is fine
	if _, err := parseFieldReport("r1", reportBody("r1", func(m map[string]any) { m["description"] = strings.Repeat("x", 4096) })); err != nil {
		t.Errorf("4096 bytes rejected: %v", err)
	}
}

type fakeFields struct {
	mu     sync.Mutex
	rows   map[string]bool
	parked []failedMessage
	saveEr error
}

func (f *fakeFields) wire() *fieldReports {
	return &fieldReports{
		save: func(_ context.Context, r fieldReport, _ string) (bool, error) {
			f.mu.Lock()
			defer f.mu.Unlock()

			if f.saveEr != nil {
				return false, f.saveEr
			}

			if f.rows[r.ReportID] {
				return false, nil
			}

			f.rows[r.ReportID] = true

			return true, nil
		},
		park: func(_ context.Context, m failedMessage) error {
			f.mu.Lock()
			defer f.mu.Unlock()

			f.parked = append(f.parked, m)

			return nil
		},
	}
}

func TestSaveFieldReportHandler(t *testing.T) { //nolint:paralleltest,gocyclo // swaps the global logger; one flat scenario
	var logs bytes.Buffer

	setupLogging(&logs)
	t.Cleanup(func() { setupLogging(&bytes.Buffer{}) })

	f := &fakeFields{rows: map[string]bool{}}
	api := &readAPI{fields: f.wire()}
	ctx := context.Background()

	call := func(id string, body []byte, park string) (string, error) {
		resp, err := api.SaveFieldReport(ctx, &aggregatorpb.SaveFieldReportRequest{MessageId: id, Body: body, ParkReason: park})
		if err != nil {
			return "", err
		}

		return resp.GetResult(), nil
	}

	if res, err := call("r1", reportBody("r1", nil), ""); err != nil || res != resultStored {
		t.Fatalf("first: %q %v", res, err)
	}

	if res, _ := call("r1", reportBody("r1", nil), ""); res != resultDuplicate {
		t.Fatalf("second: %q", res)
	}

	if res, _ := call("r2", []byte(`{"report_id":`), ""); res != resultParked {
		t.Fatalf("malformed: %q", res)
	}

	if res, _ := call("r3", reportBody("r3", nil), "five failures"); res != resultParked {
		t.Fatalf("park_reason: %q", res)
	}

	if len(f.rows) != 1 || len(f.parked) != 2 {
		t.Fatalf("rows=%d parked=%d", len(f.rows), len(f.parked))
	}

	for _, m := range f.parked {
		if m.Source != sourceFieldReport || m.Retryable || m.Status != failedParked || strings.Contains(m.Reason, testEmail) {
			t.Fatalf("bad parked row: %+v", m)
		}
	}

	f.saveEr = errors.New("db down")

	if _, err := call("r4", reportBody("r4", nil), ""); status.Code(err) != codes.Unavailable {
		t.Fatalf("store down: %v", err)
	}

	if strings.Contains(logs.String(), testEmail) {
		t.Fatalf("log output contains the e-mail:\n%s", logs.String())
	}

	if api.fields.stored.Load() != 1 || api.fields.duplicate.Load() != 1 || api.fields.parked.Load() != 2 {
		t.Fatal("counters wrong")
	}
}

func TestFanoutFieldReport(t *testing.T) {
	t.Parallel()

	f := newFake()
	c := f.cluster("a", "b", "c")
	shard := shardOf("rep-1")
	own := owners(shard, c.live())
	wrote := map[string]int{}

	var mu sync.Mutex

	write := func(_ context.Context, n *node) (bool, error) {
		mu.Lock()
		defer mu.Unlock()

		if n.name == own[1].node.name {
			return false, errDown
		}

		wrote[n.name]++

		return true, nil
	}

	changed, err := c.fanout(context.Background(), shard, write)
	if err != nil || !changed {
		t.Fatalf("one owner down must still succeed: %v %v", changed, err)
	}

	if len(wrote) != 1 || !c.dirty[shard] {
		t.Fatalf("wrote=%v dirty=%v", wrote, c.dirty)
	}

	if _, err := c.fanout(context.Background(), shard, func(context.Context, *node) (bool, error) { return false, errDown }); err == nil {
		t.Fatal("both owners down must fail")
	}
}
