package main

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStreamDeliversAnUpsertedEvent(t *testing.T) {
	t.Parallel()

	s := newState()
	srv := httptest.NewServer(newMux(s, func() bool { return true }))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/stream", nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = resp.Body.Close() })

	r := bufio.NewReader(resp.Body)
	if line, _ := r.ReadString('\n'); !strings.HasPrefix(line, ": open") {
		t.Fatalf("expected the open comment, got %q", line)
	}

	s.handle(msg(t, event("h1", "SEISMIC", "WASPADA")))

	got := make(chan string, 1)

	go func() {
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}

			if strings.HasPrefix(line, "data: ") {
				got <- line

				return
			}
		}
	}()

	select {
	case line := <-got:
		if !strings.Contains(line, `"hazard_id":"h1"`) {
			t.Fatalf("unexpected frame %q", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no frame within 2s")
	}
}

func TestPublishDropsASlowSubscriberWithoutBlocking(t *testing.T) {
	t.Parallel()

	h := newHub()
	slow, fast := h.subscribe(), h.subscribe()

	for range subBuffer + 1 {
		h.publish([]byte("x"))

		<-fast
	}

	if _, ok := <-drain(slow); ok {
		t.Fatal("expected the slow subscriber to be closed after its buffer overflowed")
	}

	h.publish([]byte("y"))

	if string(<-fast) != "y" {
		t.Fatal("the fast subscriber must keep receiving")
	}
}

// Empties a closed channel's backlog and returns it closed.
func drain(ch chan []byte) chan []byte {
	for len(ch) > 0 {
		<-ch
	}

	return ch
}
