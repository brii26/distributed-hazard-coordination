package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	auditDefaultLimit = 100
	auditMaxLimit     = 1000
)

type auditErrorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	_ = json.NewEncoder(w).Encode(v)
}

func writeAuditError(w http.ResponseWriter, status int, code, msg string) {
	var b auditErrorBody

	b.Error.Code, b.Error.Message = code, msg
	writeJSON(w, status, b)
}

// Checks the bearer token in constant time and that the cluster is there. Writes the error and returns false when either fails.
func (a *readAPI) auditGate(w http.ResponseWriter, r *http.Request) bool {
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || a.auditTok == "" || subtle.ConstantTimeCompare([]byte(got), []byte(a.auditTok)) != 1 {
		writeAuditError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")

		return false
	}

	if a.cl == nil {
		writeAuditError(w, http.StatusServiceUnavailable, "unavailable", "audit needs STORE_SERVICE")

		return false
	}

	return true
}

// A source we store. Unknown names are a bad request, a known source with no rows is just empty.
func knownSource(s string) bool {
	return s == sourceBMKG || s == sourcePVMBG
}

type auditEventsResponse struct {
	Items            []HazardEvent `json:"items"`
	NextCursor       *string       `json:"next_cursor"`
	Partial          bool          `json:"partial"`
	UnavailableUnits []string      `json:"unavailable_units"`
}

// Reads and checks the events query. A non-empty msg is the 400 message.
func parseAuditQuery(q url.Values) (f listFilter, limit int, after *listPos, msg string) {
	f = listFilter{Asc: true, Source: q.Get("source")}
	limit = auditDefaultLimit

	if f.Source != "" && !knownSource(f.Source) {
		return f, 0, nil, "source tidak dikenal"
	}

	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > auditMaxLimit {
			return f, 0, nil, "limit harus 1 sampai 1000"
		}

		limit = n
	}

	if raw := q.Get("since"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return f, 0, nil, "since bukan waktu ISO 8601"
		}

		f.Since = &t
	}

	after, err := decodeCursor(q.Get("cursor"))
	if err != nil {
		return f, 0, nil, "cursor tidak valid"
	}

	return f, limit, after, ""
}

// GET /internal/audit/events: oldest first by (ingested_at, hazard_id), filtered by source and since, paged by an opaque cursor.
func (a *readAPI) auditEvents(w http.ResponseWriter, r *http.Request) {
	if !a.auditGate(w, r) {
		return
	}

	f, limit, after, msg := parseAuditQuery(r.URL.Query())
	if msg != "" {
		writeAuditError(w, http.StatusBadRequest, "bad_request", msg)

		return
	}

	events, more, unavailable := a.cl.list(r.Context(), limit, after, f)

	resp := auditEventsResponse{Items: []HazardEvent{}, Partial: len(unavailable) > 0, UnavailableUnits: nonNil(unavailable)}

	for _, e := range events {
		e.OccurredAt, e.IngestedAt = e.OccurredAt.UTC(), e.IngestedAt.UTC()
		resp.Items = append(resp.Items, e)
	}

	if more && len(events) > 0 {
		last := events[len(events)-1]
		c := listPos{T: last.IngestedAt, ID: last.HazardID}.encode()
		resp.NextCursor = &c
	}

	writeJSON(w, http.StatusOK, resp)
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}

	return s
}

type auditChecksumResponse struct {
	Source           string   `json:"source"`
	Count            int      `json:"count"`
	Checksum         string   `json:"checksum"`
	Algorithm        string   `json:"algorithm"`
	AsOf             string   `json:"as_of"`
	Partial          bool     `json:"partial"`
	UnavailableUnits []string `json:"unavailable_units"`
}

// GET /internal/audit/checksum: SHA-256 over the byte-sorted source_ref_id values of a source, joined by newline, no trailing newline. Not a snapshot: shards are read a few ms apart.
func (a *readAPI) auditChecksum(w http.ResponseWriter, r *http.Request) {
	if !a.auditGate(w, r) {
		return
	}

	source := r.URL.Query().Get("source")
	if !knownSource(source) {
		writeAuditError(w, http.StatusBadRequest, "bad_request", "source tidak dikenal")

		return
	}

	asOf := time.Now().UTC()
	ids, unavailable := a.cl.sourceIDs(r.Context(), source)

	// ponytail: the ids are held in memory for one source; fine for thousands, stream per shard if it grows to millions.
	sum := sha256.Sum256([]byte(strings.Join(ids, "\n")))

	writeJSON(w, http.StatusOK, auditChecksumResponse{
		Source:           source,
		Count:            len(ids),
		Checksum:         hex.EncodeToString(sum[:]),
		Algorithm:        "sha256",
		AsOf:             asOf.Format(time.RFC3339),
		Partial:          len(unavailable) > 0,
		UnavailableUnits: nonNil(unavailable),
	})
}
