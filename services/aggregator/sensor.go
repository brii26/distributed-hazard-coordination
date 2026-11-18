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

	// Bulkhead: calls in flight are capped by bulk, a poll reads at most maxPages pages, and a call slower than slowAfter marks
	// the poll slow. All optional, so a client built without them has no cap.
	bulk      chan struct{}
	maxPages  int
	slowAfter time.Duration
	slow      atomic.Bool
	stats     *sensorStats

	// Stale detection, poller goroutine only. mark is the newest sensor time seen (generated_at or observed_at), movedAt is
	// the local time it last grew. The sensor's clock is its own, so only its progress is compared, never its value.
	staleAfter time.Duration
	mark       time.Time
	movedAt    time.Time
	now        func() time.Time
	onStale    func(stale bool, since time.Time)
}

// Wires the client to the store: the cursor and the quarantine live in the Canonical Store.
func newSensorClient(c pollConfig, db *store) *sensorClient {
	sc := c.sensor.orDefaults()

	// The sensor has its own connection pool, capped like its calls, shared with no other source.
	tr := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert // the default is a *http.Transport
	tr.MaxConnsPerHost = sc.maxConcurrent
	tr.MaxIdleConnsPerHost = sc.maxConcurrent

	s := &sensorClient{
		http: &sourceClient{
			baseURL: strings.TrimRight(c.pantaiURL, "/"), timeout: sc.timeout, client: &http.Client{Transport: tr},
			auth: func(r *http.Request) error { r.Header.Set("Authorization", "Bearer "+c.pantaiToken); return nil },
		},
		bulk:       make(chan struct{}, sc.maxConcurrent),
		maxPages:   sc.maxPages,
		slowAfter:  sc.slowAfter,
		staleAfter: sc.staleAfter,
		stats:      &sensorStats{},
		validator:  newSensorValidator(nil),
		rejects:    newRejectCounter(),
		mapper:     newMapper(),
		loadCursor: func(ctx context.Context) (string, error) { return db.getCursor(ctx, sourceSensorPantai) },
		saveCursor: func(ctx context.Context, cur string) error { return db.setCursor(ctx, sourceSensorPantai, cur) },
		quarantine: db.insertFailed,
	}
	s.stats.inFlight = func() int { return len(s.bulk) }

	return s
}

// One call to the sensor through the bulkhead: it never waits for a slot, a full bulkhead is an error.
func (s *sensorClient) get(ctx context.Context, path, query string, decode func(io.Reader) error) error {
	if s.bulk != nil {
		select {
		case s.bulk <- struct{}{}:
			defer func() { <-s.bulk }()
		default:
			s.stats.bulkheadRejected()

			return errBulkheadFull
		}
	}

	s.stats.request()

	start := time.Now()

	err := s.http.get(ctx, path, query, decode)
	if err == nil && s.slowAfter > 0 && time.Since(start) > s.slowAfter {
		s.slow.Store(true)
		s.stats.slowAnswer()
	}

	return err
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

	if err := s.get(ctx, "/v1/stations", "", func(r io.Reader) error { return json.NewDecoder(r).Decode(&body) }); err != nil {
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

// Reads one observations page. A bad item does not fail the page: it comes back in bad and the other items stay. A break in the
// syntax ends the items, and next_cursor is then left unread so the page is asked for again.
func decodeObservations(r io.Reader, limit int) (body observationsBody, bad []rejectedRaw, err error) { //nolint:gocyclo // one walk over the top-level keys
	raw, cut, err := readBody(r, limit)
	if err != nil {
		return body, nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	base := int64(0) // body offset = base + dec.InputOffset()

	if tok, terr := dec.Token(); terr != nil || tok != json.Delim('{') {
		if cut {
			terr = errOversize
		} else if terr = checkShape(raw); terr == nil {
			terr = errNotArray
		}

		return body, []rejectedRaw{{raw, terr}}, terr
	}

	for dec.More() {
		key, kerr := dec.Token()
		if kerr != nil {
			return body, nil, kerr
		}

		switch key {
		case "items":
			tok, terr := dec.Token()
			if terr == nil && tok == nil {
				continue
			}

			if terr != nil || tok != json.Delim('[') {
				return body, nil, errNotArray
			}

			var (
				broken bool
				next   int
			)

			bad, next, broken = streamArray(raw, int(base+dec.InputOffset()), cut, func(e json.RawMessage) error {
				body.Items = append(body.Items, e)

				return nil
			})
			if broken {
				return body, bad, nil
			}

			// The decoder did not read the items, so start a new one just after them: the "{" stands in for the object it is inside.
			next = skipSpace(raw, next)
			if next < len(raw) && raw[next] == ',' {
				next++
			}

			base = int64(next) - 1
			dec = json.NewDecoder(io.MultiReader(strings.NewReader("{"), bytes.NewReader(raw[next:])))

			if _, err = dec.Token(); err != nil {
				return body, bad, err
			}
		case "generated_at":
			err = dec.Decode(&body.GeneratedAt)
		case "next_cursor":
			err = dec.Decode(&body.NextCursor)
		default:
			var skip json.RawMessage

			err = dec.Decode(&skip)
		}

		if err != nil {
			return body, bad, err
		}
	}

	return body, bad, nil
}

// One poll: pages from the saved cursor until the sensor has nothing new (next_cursor null, which keeps the old cursor).
// The cursor in memory moves only when the whole poll worked; the saved one moves after the batch is stored (afterBatch).
func (s *sensorClient) fetch(ctx context.Context, _ time.Time) (pollResult, error) {
	if err := s.ensureStations(ctx); err != nil {
		return pollResult{}, err
	}

	g := s.gen.Load()

	s.slow.Store(false)

	if !s.cursorLoaded || s.reload.Swap(false) {
		cur, err := s.loadCursor(ctx)
		if err != nil {
			return pollResult{}, fmt.Errorf("load sensor cursor: %w", err)
		}

		s.cursor, s.cursorLoaded = cur, true
	}

	var res pollResult

	cur := s.cursor

	pages := s.maxPages
	if pages <= 0 {
		pages = sensorMaxPages
	}

	for range pages {
		q := fmt.Sprintf("limit=%d", sensorPageLimit)
		if cur != "" {
			q += "&cursor=" + url.QueryEscape(cur)
		}

		var (
			body observationsBody
			bad  []rejectedRaw
		)

		err := s.get(ctx, "/v1/observations", q, func(r io.Reader) (derr error) {
			body, bad, derr = decodeObservations(r, s.http.bodyCap())

			return derr
		})

		res.rejected = append(res.rejected, bad...)

		if err != nil {
			return pollResult{rejected: res.rejected}, err
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
	res.slow = s.slow.Load()

	s.checkFresh(res)

	if cur != s.cursor {
		res.nextCursor, s.cursor = cur, cur
	}

	return res, nil
}

// A new lease term: the stored cursor is the truth again, and batches still queued from the last term do not save theirs.
func (s *sensorClient) newTerm() {
	s.reload.Store(true)
	s.gen.Add(1)
	s.resetFresh()
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

// Compares the newest sensor time of this poll with the highest one seen. A sensor that answers 200 but whose clock and
// observations stand still for staleAfter is marked stale.
func (s *sensorClient) checkFresh(res pollResult) {
	if s.staleAfter <= 0 || s.onStale == nil {
		return
	}

	now := time.Now
	if s.now != nil {
		now = s.now
	}

	newest := res.generatedAt

	for _, rec := range res.records {
		if str, ok := rec["observed_at"].(string); ok {
			if t, err := time.Parse(time.RFC3339Nano, str); err == nil && t.After(newest) {
				newest = t
			}
		}
	}

	t := now()

	if s.movedAt.IsZero() || newest.After(s.mark) {
		s.mark, s.movedAt = newest, t
	}

	s.onStale(t.Sub(s.movedAt) > s.staleAfter, s.movedAt)
}

// A new lease term starts the freshness clock again.
func (s *sensorClient) resetFresh() {
	s.mark, s.movedAt = time.Time{}, time.Time{}
}
