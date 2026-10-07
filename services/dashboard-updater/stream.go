package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

const (
	// Max events one subscriber may fall behind before it is cut off.
	subBuffer = 64
	keepAlive = 15 * time.Second
)

// Fans each upserted event out to the connected streams. publish never blocks: a subscriber with a full buffer is dropped, so one slow client can't hold up the consumer or the others. It reconnects and refetches.
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

// GET /stream: one Server-Sent Events frame per event, the full HazardEvent as JSON. Internal, the Client-Facing API filters it by role.
func (h *hub) stream(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
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
				return
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
