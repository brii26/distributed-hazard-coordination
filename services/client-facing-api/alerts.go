package main

import (
	"context"
	"net/http"
	"time"
)

// GET /alerts/stream: the Notifier's critical alert stream for Tim Lapangan (and the Operator). Same JWT middleware and stream caps as /hazards/stream, Last-Event-ID is passed on so a reconnecting client misses nothing.
type alertsAPI struct {
	streamAPI
}

func newAlertsAPI(notifierURL string) alertsAPI {
	return alertsAPI{newStreamAPI(notifierURL)}
}

func (s alertsAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	claims, _ := claimsFrom(r.Context())

	if claims.Role != roleLapangan && claims.Role != roleOperator {
		writeJSON(w, http.StatusForbidden, errBody("alerts are for Lapangan and Operator"))

		return
	}

	if !s.acquire(claims.Email) {
		w.Header().Set("Retry-After", "5")
		writeJSON(w, http.StatusServiceUnavailable, errBody("too many open streams"))

		return
	}

	defer s.release(claims.Email)

	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("streaming unsupported"))

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Unix(claims.Exp, 0).Sub(s.now()))
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.upstream+"/alerts/stream", nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("stream unavailable"))

		return
	}

	if id := r.Header.Get("Last-Event-ID"); id != "" {
		req.Header.Set("Last-Event-ID", id)
	}

	resp, err := s.client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			_ = resp.Body.Close()
		}

		logFor(r.Context()).Warn("alerts stream upstream", "error", err)
		writeJSON(w, http.StatusBadGateway, errBody("stream unavailable"))

		return
	}

	defer func() { _ = resp.Body.Close() }()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_ = rc.Flush()

	logFor(r.Context()).Info("alerts stream", "role", claims.Role, "result", "opened")

	// Frames are copied as they are: an alert is not filtered by role.
	buf := make([]byte, 4096)

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil || rc.Flush() != nil {
				break
			}
		}

		if err != nil {
			break // the Notifier or the client went away, the client reconnects
		}
	}

	logFor(r.Context()).Info("alerts stream", "role", claims.Role, "result", "closed")
}
