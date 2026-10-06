package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// maxStreams caps open streams. They are exempt from capInflight (a long-lived
// stream would hold a slot for its whole life), so they get their own cap.
const maxStreams = 500

// streamAPI is GET /hazards/stream?type=: the Dashboard Updater's event stream
// (doc/TRD_M1.md 9), behind the same JWT middleware and role filtering as
// GET /hazards. It is the only public way to that stream.
type streamAPI struct {
	upstream string // DASHBOARD_UPDATER_URL, e.g. http://dashboard-updater:8093
	client   *http.Client
	slots    chan struct{}
	now      func() time.Time
}

func newStreamAPI(upstream string) streamAPI {
	// No client Timeout: it would cut the stream. The request context ends it.
	return streamAPI{upstream: strings.TrimRight(upstream, "/"), client: &http.Client{}, slots: make(chan struct{}, maxStreams), now: time.Now}
}

func (s streamAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	claims, _ := claimsFrom(r.Context())

	typ := r.URL.Query().Get("type")
	if typ != "SEISMIC" && typ != "VOLCANIC" {
		writeJSON(w, http.StatusBadRequest, errBody("type must be SEISMIC or VOLCANIC"))

		return
	}

	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		w.Header().Set("Retry-After", "5")
		writeJSON(w, http.StatusServiceUnavailable, errBody("too many open streams"))

		return
	}

	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil { // the server's WriteTimeout would cut the stream
		writeJSON(w, http.StatusInternalServerError, errBody("streaming unsupported"))

		return
	}

	// The stream ends when the session's token does: the browser reconnects, and
	// the middleware then refreshes it or answers 401. An expired session cannot keep a stream.
	ctx, cancel := context.WithTimeout(r.Context(), time.Unix(claims.Exp, 0).Sub(s.now()))
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.upstream+"/stream", nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("stream unavailable"))

		return
	}

	resp, err := s.client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			_ = resp.Body.Close()
		}

		logFor(r.Context()).Warn("hazards stream upstream", "error", err)
		writeJSON(w, http.StatusBadGateway, errBody("stream unavailable"))

		return
	}

	defer func() { _ = resp.Body.Close() }()

	fields := fieldsFor(claims.Role) // role filtering, same as GET /hazards

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no") // nginx must not hold the frames back
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, ": open\n\n")
	_ = rc.Flush()

	logFor(r.Context()).Info("hazards stream", "role", claims.Role, "type", typ, "result", "opened")

	relay(w, rc, resp.Body, typ, fields)

	logFor(r.Context()).Info("hazards stream", "role", claims.Role, "type", typ, "result", "closed")
}

// relay copies the upstream frames to the client, keeping the events of one type
// and only the fields the role may see, until either side ends.
func relay(w http.ResponseWriter, rc *http.ResponseController, body io.Reader, typ string, fields []string) {
	var err error

	sc := bufio.NewScanner(body)
	sc.Buffer(nil, 1<<20)

	for sc.Scan() {
		line := sc.Text()

		switch {
		case strings.HasPrefix(line, "data: "):
			var e map[string]any
			if json.Unmarshal([]byte(line[len("data: "):]), &e) != nil || e["hazard_type"] != typ {
				continue
			}

			out, _ := json.Marshal(project(e, fields))
			_, err = fmt.Fprintf(w, "data: %s\n\n", out)
		case line == ": keep-alive": // passed on so the browser and proxies see traffic
			_, err = fmt.Fprint(w, line+"\n\n")
		default:
			continue
		}

		if err == nil {
			err = rc.Flush()
		}

		if err != nil {
			return
		}
	}
}
