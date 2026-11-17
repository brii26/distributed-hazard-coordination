package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	sensorPageLimit = 1000 // the most the sensor allows per page
	sensorMaxPages  = 20   // pages read in one poll, the rest comes in the next one
)

// Reads Sensor Pantai: the station list once, then observations from the saved cursor. Fetch runs on the poller goroutine; the
// mapper and the cursor saver run on the ingest goroutine.
type sensorClient struct {
	http      *sourceClient
	validator *sensorValidator
	rejects   *rejectCounter
	mapper    *mapper

	loadCursor func(context.Context) (string, error)
	saveCursor func(context.Context, string) error
	quarantine func(context.Context, failedMessage) error

	mu       sync.RWMutex
	stations map[string]station

	// Poller goroutine only. The cursor is read from the store on the first poll and again whenever reload is set.
	cursor       string
	cursorLoaded bool

	// reload asks the next poll to read the cursor from the store again. gen counts those resets: a batch fetched under an
	// older gen never saves its cursor, so a reset cannot be undone by a batch that was already queued.
	reload atomic.Bool
	gen    atomic.Uint64
}

// Wires the client to the store: the cursor and the quarantine live in the Canonical Store.
func newSensorClient(c pollConfig, db *store) *sensorClient {
	return &sensorClient{
		http: &sourceClient{
			baseURL: strings.TrimRight(c.pantaiURL, "/"), timeout: c.timeout, client: &http.Client{},
			auth: func(r *http.Request) error { r.Header.Set("Authorization", "Bearer "+c.pantaiToken); return nil },
		},
		validator:  newSensorValidator(nil),
		rejects:    newRejectCounter(),
		mapper:     newMapper(),
		loadCursor: func(ctx context.Context) (string, error) { return db.getCursor(ctx, sourceSensorPantai) },
		saveCursor: func(ctx context.Context, cur string) error { return db.setCursor(ctx, sourceSensorPantai, cur) },
		quarantine: db.insertFailed,
	}
}

type stationsBody struct {
	Items []struct {
		StationID string  `json:"station_id"`
		Name      string  `json:"name"`
		Province  string  `json:"province"`
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
	} `json:"items"`
}

// Loads /v1/stations once. An empty list is a failed poll, since every observation would be set aside as unknown.
func (s *sensorClient) ensureStations(ctx context.Context) error {
	s.mu.RLock()
	loaded := s.stations != nil
	s.mu.RUnlock()

	if loaded {
		return nil
	}

	var body stationsBody

	if err := s.http.get(ctx, "/v1/stations", "", func(r io.Reader) error { return json.NewDecoder(r).Decode(&body) }); err != nil {
		return err
	}

	set := make(map[string]station, len(body.Items))
	ids := make([]string, 0, len(body.Items))

	for _, it := range body.Items {
		if it.StationID == "" {
			continue
		}

		set[it.StationID] = station{Name: it.Name, Province: it.Province, Lat: it.Latitude, Lon: it.Longitude}
		ids = append(ids, it.StationID)
	}

	if len(set) == 0 {
		return errors.New("/v1/stations returned no stations")
	}

	s.validator.setStations(ids)

	s.mu.Lock()
	s.stations = set
	s.mu.Unlock()

	return nil
}

type observationsBody struct {
	GeneratedAt string            `json:"generated_at"`
	Items       []json.RawMessage `json:"items"`
	NextCursor  *string           `json:"next_cursor"`
}

// One poll: pages from the saved cursor until the sensor has nothing new (next_cursor null, which keeps the old cursor).
// The cursor in memory moves only when the whole poll worked; the saved one moves after the batch is stored (afterBatch).
func (s *sensorClient) fetch(ctx context.Context, _ time.Time) (pollResult, error) {
	if err := s.ensureStations(ctx); err != nil {
		return pollResult{}, err
	}

	g := s.gen.Load()

	if !s.cursorLoaded || s.reload.Swap(false) {
		cur, err := s.loadCursor(ctx)
		if err != nil {
			return pollResult{}, fmt.Errorf("load sensor cursor: %w", err)
		}

		s.cursor, s.cursorLoaded = cur, true
	}

	var res pollResult

	cur := s.cursor

	for range sensorMaxPages {
		q := fmt.Sprintf("limit=%d", sensorPageLimit)
		if cur != "" {
			q += "&cursor=" + url.QueryEscape(cur)
		}

		var body observationsBody

		if err := s.http.get(ctx, "/v1/observations", q, func(r io.Reader) error { return json.NewDecoder(r).Decode(&body) }); err != nil {
			return pollResult{}, err
		}

		if t, err := time.Parse(time.RFC3339Nano, body.GeneratedAt); err == nil && t.After(res.generatedAt) {
			res.generatedAt = t
		}

		for _, raw := range body.Items {
			res.records = append(res.records, decodeItem(raw))
			res.raws = append(res.raws, raw)
		}

		if body.NextCursor == nil || *body.NextCursor == "" || *body.NextCursor == cur {
			break
		}

		cur = *body.NextCursor
	}

	if s.gen.Load() != g {
		// A reset came in while this poll ran. Drop it and start again from the stored cursor.
		s.reload.Store(true)

		return pollResult{}, nil
	}

	res.gen = g

	if cur != s.cursor {
		res.nextCursor, s.cursor = cur, cur
	}

	return res, nil
}

// A new lease term: the stored cursor is the truth again, and batches still queued from the last term do not save theirs.
func (s *sensorClient) newTerm() {
	s.reload.Store(true)
	s.gen.Add(1)
}

// The item as a map with numbers kept exact, or nil when it is not a JSON object.
func decodeItem(raw []byte) map[string]any {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil
	}

	return m
}

// Validates and maps a batch. A violation is counted, written to the failed message store as non-retryable and left out of the
// result. A repeated observation_id inside the batch is mapped once. The error is set only when a quarantine write failed.
func (s *sensorClient) mapBatch(res pollResult) ([]HazardEvent, error) {
	var (
		out  []HazardEvent
		errs []error
	)

	seen := map[string]bool{}
	l := withID(res.correlationID)

	for i, rec := range res.records {
		v := s.check(rec, res.generatedAt)

		var h HazardEvent

		if v == nil {
			id, _ := rec["observation_id"].(string)
			if seen[id] {
				continue
			}

			seen[id] = true

			s.mu.RLock()
			st, ok := s.stations[rec["station_id"].(string)] //nolint:forcetypeassert // the validator checked the type
			s.mu.RUnlock()

			if !ok {
				v = &violation{Reason: reasonUnknownStation, Field: "station_id", Detail: "station_id is not in the station list"}
			} else if hh, err := s.mapper.coastal(rec, st); err != nil {
				v = &violation{Reason: reasonOutOfRange, Field: "observation_id", Detail: err.Error()}
			} else {
				h = hh
			}
		}

		if v == nil {
			out = append(out, h)

			continue
		}

		s.rejects.add(v.Reason)
		l.Warn("sensor observation rejected", "source", sourceSensorPantai, "reason", v.Reason, "field", v.Field, "detail", v.Detail)

		if err := s.quarantine(context.Background(), quarantineObservation(res.raws[i], rec, v, time.Now())); err != nil {
			errs = append(errs, fmt.Errorf("quarantine: %w", err))
		}
	}

	return out, errors.Join(errs...)
}

// A nil record is an item that was not a JSON object.
func (s *sensorClient) check(rec map[string]any, generatedAt time.Time) *violation {
	if rec == nil {
		return &violation{Reason: reasonBadType, Field: "item", Detail: "item is not a JSON object"}
	}

	return s.validator.validate(rec, generatedAt)
}

// Saves the cursor once the batch is stored. A batch that was not fully stored resets the cursor to the stored one, so the
// next poll reads it again.
func (s *sensorClient) afterBatch(ctx context.Context, res pollResult, clean bool) {
	if res.gen != s.gen.Load() {
		return
	}

	if !clean {
		s.reload.Store(true)
		s.gen.Add(1)

		return
	}

	if res.nextCursor == "" {
		return
	}

	if err := s.saveCursor(context.WithoutCancel(ctx), res.nextCursor); err != nil {
		withID(res.correlationID).Warn("sensor cursor not saved", "source", sourceSensorPantai, "error", err.Error())
	}
}
