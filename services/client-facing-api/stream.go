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

// Caps open streams. They skip capInflight (a long-lived stream would hold a slot for its whole life), so they get their own cap.
const maxStreams = 500

// GET /hazards/stream?type=: the Dashboard Updater's event stream, behind the same JWT middleware and role filtering as GET /hazards. The only public way to that stream.
type streamAPI struct {
	upstream string // DASHBOARD_UPDATER_URL, e.g. http://dashboard-updater:8093
	client   *http.Client
	slots    chan struct{}
	now      func() time.Time
}

func newStreamAPI(upstream string) streamAPI {
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
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("streaming unsupported"))

		return
	}

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

	fields := fieldsFor(claims.Role)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, ": open\n\n")
	_ = rc.Flush()

	logFor(r.Context()).Info("hazards stream", "role", claims.Role, "type", typ, "result", "opened")

	relay(w, rc, resp.Body, typ, fields)

	logFor(r.Context()).Info("hazards stream", "role", claims.Role, "type", typ, "result", "closed")
}

// Copies upstream frames to the client, keeping one type's events and only the fields the role may see, until either side ends.
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
		case line == ": keep-alive":
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
