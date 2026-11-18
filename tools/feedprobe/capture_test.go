package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestCaptureResumesAndKeepsTokenOut checks the forced reconnect asks for ?from=<last id>
// and that the token never reaches the capture or connection files.
func TestCaptureResumesAndKeepsTokenOut(t *testing.T) {
	t.Parallel()

	const token = "probe-test-secret-7f3a" //nolint:gosec // fake value the test searches for in the output

	var (
		mu       sync.Mutex
		requests []string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, `{"error":{"code":"unauthorized"}}`, http.StatusUnauthorized)

			return
		}

		if r.URL.Path == "/v1/session" {
			_, _ = fmt.Fprint(w, `{"profile":"test","speed":1,"latest_offset":3}`)

			return
		}

		mu.Lock()

		requests = append(requests, r.URL.RawQuery)

		mu.Unlock()

		start := 1
		if r.URL.Query().Get("from") == "3" {
			start = 4
		}

		w.Header().Set("Content-Type", "text/event-stream")

		for id := start; id < start+3; id++ {
			_, _ = fmt.Fprintf(w, "id: %d\nevent: detection\ndata: {\"n\":%d}\n\n", id, id)
		}

		_ = http.NewResponseController(w).Flush()

		<-r.Context().Done()
	}))
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "cap.ndjson")
	base, _ := url.Parse(srv.URL)
	cr := newTestCapture(t, captureOpts{out: out, from: "1", disconnectAt: 300 * time.Millisecond},
		&feedClient{base: base, token: token, http: srv.Client()})

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	if err := cr.run(ctx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(requests) < 2 || requests[0] != "from=1" || requests[1] != "from=3" {
		t.Fatalf("stream requests %v; want from=1 then from=3", requests)
	}

	for _, path := range []string{out, connPath(out)} {
		raw, err := os.ReadFile(path) //nolint:gosec // test temp file
		if err != nil {
			t.Fatal(err)
		}

		if strings.Contains(string(raw), token) {
			t.Fatalf("%s contains the token", filepath.Base(path))
		}
	}

	ids := capturedIDs(t, out)
	if strings.Join(ids, ",") != "1,2,3,4,5,6" {
		t.Fatalf("captured ids %v", ids)
	}
}

func newTestCapture(t *testing.T, opts captureOpts, client *feedClient) *captureRun {
	t.Helper()

	events, err := os.Create(opts.out)
	if err != nil {
		t.Fatal(err)
	}

	conns, err := os.Create(connPath(opts.out))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = events.Close()
		_ = conns.Close()
	})

	return &captureRun{opts: opts, client: client, start: time.Now(), events: json.NewEncoder(events), conns: json.NewEncoder(conns)}
}

func capturedIDs(t *testing.T, path string) []string {
	t.Helper()

	var ids []string

	err := readNDJSON(path, func(line []byte) error {
		var r eventRecord
		if err := json.Unmarshal(line, &r); err != nil {
			return err
		}

		if r.SSEID != nil {
			ids = append(ids, *r.SSEID)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	return ids
}
