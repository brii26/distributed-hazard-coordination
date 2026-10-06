package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

const (
	// subBuffer is how many events one subscriber may fall behind before it is cut off.
	subBuffer = 64
	keepAlive = 15 * time.Second
)

// hub fans each upserted event out to the connected streams (doc/TRD_M1.md 8.1).
// publish never blocks: a subscriber whose buffer is full is dropped, so one slow
// or dead client cannot slow the consumer or the other clients. It reconnects and
// refetches, so a dropped event is not lost to it.
type hub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

func newHub() *hub { return &hub{subs: map[chan []byte]struct{}{}} }

func (h *hub) subscribe() chan []byte {
	ch := make(chan []byte, subBuffer)

	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()

	return ch
}

func (h *hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.subs[ch]; ok {
		delete(h.subs, ch)
		close(ch)
	}
}

func (h *hub) publish(body []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for ch := range h.subs {
		select {
		case ch <- body:
		default:
			delete(h.subs, ch)
			close(ch)
		}
	}
}

// stream is GET /stream: one Server-Sent Events frame per event, the full
// HazardEvent as JSON. It is internal; the Client-Facing API filters it by role.
func (h *hub) stream(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	// The server's WriteTimeout would cut a stream that is meant to stay open.
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)

		return
	}

	ch := h.subscribe()
	defer h.unsubscribe(ch)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, ": open\n\n")
	_ = rc.Flush()

	tick := time.NewTicker(keepAlive)
	defer tick.Stop()

	for {
		var err error

		select {
		case <-r.Context().Done():
			return
		case body, ok := <-ch:
			if !ok {
				return // dropped as too slow
			}

			_, err = fmt.Fprintf(w, "data: %s\n\n", body)
		case <-tick.C:
			_, err = fmt.Fprint(w, ": keep-alive\n\n")
		}

		if err == nil {
			err = rc.Flush()
		}

		if err != nil {
			return
		}
	}
}
