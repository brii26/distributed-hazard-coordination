package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSatStore holds what hazard_events, failed_messages and source_cursors would, with the same rules: a fire is written only when its version is not lower and something differs, and a parked record is kept once.
type fakeSatStore struct {
	mu          sync.Mutex
	rows        map[string]HazardEvent // by source_ref_id; ingested_at is cleared so runs compare
	parked      map[string]string      // message id → reason
	cursor      string
	upserts     int // upserts that reached the store
	parkCalls   int
	failUpserts int  // fail this many upserts first, like an outage
	fenced      bool // every upsert is fenced, like a newer leader took over
}

func newFakeSatStore() *fakeSatStore {
	return &fakeSatStore{rows: map[string]HazardEvent{}, parked: map[string]string{}}
}

func (s *fakeSatStore) upsert(ctx context.Context, h HazardEvent) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fenced {
		epoch, _ := leaseEpoch(ctx)

		return false, &fencedError{source: h.Source, token: epoch}
	}

	if s.failUpserts > 0 {
		s.failUpserts--

		return false, errors.New("store down")
	}

	s.upserts++
	h.IngestedAt = time.Time{}

	if old, ok := s.rows[h.SourceRefID]; ok && (h.Version < old.Version || reflect.DeepEqual(old, h)) {
		return false, nil
	}

	s.rows[h.SourceRefID] = h

	return true, nil
}

func (s *fakeSatStore) insertFailed(_ context.Context, m failedMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.parkCalls++

	if _, ok := s.parked[m.MessageID]; !ok {
		s.parked[m.MessageID] = m.Reason
	}

	return nil
}

func (s *fakeSatStore) getCursor(context.Context, string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.cursor, nil
}

func (s *fakeSatStore) setCursor(_ context.Context, _, c string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cursor = c

	return nil
}

func (s *fakeSatStore) state() (map[string]HazardEvent, map[string]string, string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return maps.Clone(s.rows), maps.Clone(s.parked), s.cursor, s.upserts
}

func (s *fakeSatStore) parkCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.parkCalls
}

// fakeConfirmPub records what was published. hold never confirms, like a broker that went away.
type fakeConfirmPub struct {
	mu   sync.Mutex
	sent []string // hazard ids
	hold bool
}

func (p *fakeConfirmPub) publishWait(_ context.Context, h HazardEvent, _ string) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.sent = append(p.sent, h.HazardID)

	return uint64(len(p.sent)), nil
}

func (p *fakeConfirmPub) waitConfirmed(ctx context.Context, _ uint64) error {
	if p.hold {
		<-ctx.Done()

		return ctx.Err()
	}

	return nil
}

func (p *fakeConfirmPub) published() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.sent...)
}

// feedRecords renders engine test events as the raw records the feed sends, with keys built the way the parser builds them.
func feedRecords(events []feedEvent) []feedRecord {
	keys := map[string]string{} // engine test key → feed key
	out := make([]feedRecord, len(events))

	for i, ev := range events {
		var data string

		switch ev.Kind {
		case feedDetection:
			d := ev.Det
			at := time.Unix(d.Acq, 0).UTC()
			date, hhmm := at.Format(time.DateOnly), at.Hour()*100+at.Minute()
			lat, lon := strconv.FormatFloat(d.Lat, 'f', 5, 64), strconv.FormatFloat(d.Lon, 'f', 5, 64)
			keys[ev.Key] = fmt.Sprintf("%s|%s|%d|%s|%s", d.Satellite, date, hhmm, lat, lon)
			data = fmt.Sprintf(`{"offset":%d,"detection":{"acq_date":%q,"acq_time":%d,"confidence":%q,"frp":%g,"latitude":%s,"longitude":%s,"satellite":%q}}`,
				ev.Offset, date, hhmm, d.Confidence, d.FRP, lat, lon, d.Satellite)
		case feedUpdate:
			data = fmt.Sprintf(`{"offset":%d,"key":%q,"changes":{"confidence":%q,"frp":%g}}`, ev.Offset, keys[ev.Key], *ev.Changes.Confidence, *ev.Changes.FRP)
		case feedRetract:
			data = fmt.Sprintf(`{"offset":%d,"reason":"false_positive","key":%q}`, ev.Offset, keys[ev.Key])
		case feedHeartbeat:
			data = fmt.Sprintf(`{"offset":%d,"virtual_now":%q}`, ev.Offset, time.Unix(ev.Now, 0).UTC().Format(time.RFC3339))
		}

		out[i] = feedRecord{Offset: ev.Offset, Kind: ev.Kind, Data: []byte(data)}
	}

	return out
}

// testFeed is the random engine feed as raw records, with one record that does not parse and one whose data offset differs from its event id.
func testFeed(n int) []feedRecord {
	recs := feedRecords(randomFeed(n))
	recs[5].Data = []byte(`{"offset":5,"detection":{}}`)
	recs[6] = feedRecord{Offset: 6, Kind: feedHeartbeat, Data: []byte(`{"offset":60,"virtual_now":"2026-11-28T10:00:00Z"}`)}

	return recs
}

// satelitRun is one pipeline process over recs. crashAfter ≥ 0 kills it right after the reader hands over that offset, whatever is half done. oneBatch saves everything in a single batch at the last record.
type satelitRun struct {
	st         *fakeSatStore
	real       *store // set: the run writes to this Postgres store instead of st
	pub        confirmPublisher
	recs       []feedRecord
	crashAfter int64
	oneBatch   bool
	fenced     []int64
	stats      *satelitStats
	session    func(context.Context) (feedSession, error)
}

func (r *satelitRun) pipeline(cancel context.CancelFunc) *satelitPipeline {
	read := func(ctx context.Context, start int64, out chan<- feedRecord) error {
		for _, rec := range r.recs[start:] {
			rec.Read = time.Now()

			select {
			case out <- rec:
			case <-ctx.Done():
				return ctx.Err()
			}

			if rec.Offset == r.crashAfter {
				cancel()
			}
		}

		<-ctx.Done()

		return ctx.Err()
	}

	p := &satelitPipeline{
		read: read, upsert: r.st.upsert, park: r.st.insertFailed, marks: r.st, pub: r.pub,
		onFenced: func(epoch int64) { r.fenced = append(r.fenced, epoch) },
		now:      func() time.Time { return time.Unix(1, 0) },
		lateness: 12 * time.Hour, batchSize: 50, batchWait: 5 * time.Millisecond,
		backoff: newBackoff(time.Millisecond, 2*time.Millisecond),
		stats:   r.stats, session: r.session,
	}

	if r.oneBatch {
		p.batchSize, p.batchWait = len(r.recs), time.Hour
	}

	if r.real != nil {
		p.upsert, p.park, p.marks = r.real.upsert, r.real.insertFailed, r.real
	}

	return p
}

// start runs the pipeline and returns a func that waits for it to end, ending it first if it is still running.
func (r *satelitRun) start(t *testing.T) (stop func() error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	p := r.pipeline(cancel)
	done := make(chan error, 1)

	go func() { done <- p.run(ctx) }()

	return func() error {
		cancel()

		return <-done
	}
}

// toEnd runs until the bookmark reaches the last offset.
func toEnd(t *testing.T, st *fakeSatStore, pub confirmPublisher, recs []feedRecord) {
	t.Helper()

	stop := (&satelitRun{st: st, pub: pub, recs: recs, crashAfter: -1}).start(t)
	last := strconv.FormatInt(recs[len(recs)-1].Offset, 10)

	eventually(t, "the bookmark at "+last, func() bool { _, _, c, _ := st.state(); return c == last })

	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
}

// crash runs until the reader has handed over offset at, then kills the pipeline.
func crash(t *testing.T, st *fakeSatStore, recs []feedRecord, at int64) {
	t.Helper()

	stop := (&satelitRun{st: st, pub: &fakeConfirmPub{}, recs: recs, crashAfter: at}).start(t)

	if err := stop(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("crashed run: %v", err)
	}
}

func parkCodes(parked map[string]string) map[string]int {
	out := map[string]int{}

	for _, r := range parked {
		code, _, _ := strings.Cut(r, ":")
		out[code]++
	}

	return out
}

func TestSatelitPipelineSavesParksAndMovesTheBookmark(t *testing.T) {
	t.Parallel()

	st, pub := newFakeSatStore(), &fakeConfirmPub{}
	toEnd(t, st, pub, testFeed(3000))

	rows, parked, _, _ := st.state()
	codes := parkCodes(parked)

	if codes[codeMalformed] != 2 || codes[parkTooLate] == 0 {
		t.Fatalf("parked %v, want 2 malformed and some too late", codes)
	}

	withdrawn := 0

	for _, h := range rows {
		if h.WithdrawnAt != nil {
			withdrawn++
		}
	}

	if len(rows) == 0 || withdrawn == 0 {
		t.Fatalf("%d fires, %d withdrawn; the feed should give both", len(rows), withdrawn)
	}

	sent := map[string]bool{}
	for _, id := range pub.published() {
		sent[id] = true
	}

	for _, h := range rows {
		if !sent[h.HazardID] {
			t.Fatalf("fire %s saved but never published", h.SourceRefID)
		}
	}
}

// Criterion 5 (#313): kill the pipeline anywhere, once or twice; after recovery the store holds exactly what a run without a crash holds: the same fires, names, versions, dots and withdrawals (anchor retracts, merges and splits included), and the same parked records (a record too late before the crash is too late after it).
func TestSatelitPipelineCrashGivesTheSameStore(t *testing.T) {
	t.Parallel()

	recs := testFeed(3000)
	ref := newFakeSatStore()
	toEnd(t, ref, &fakeConfirmPub{}, recs)

	wantRows, wantParked, _, _ := ref.state()

	for _, crashes := range [][]int64{{0}, {1}, {137}, {1499}, {2998}, {800, 2100}, {2100, 400}} {
		t.Run(fmt.Sprint(crashes), func(t *testing.T) {
			t.Parallel()

			st := newFakeSatStore()
			for _, at := range crashes {
				crash(t, st, recs, at)
			}

			toEnd(t, st, &fakeConfirmPub{}, recs)

			rows, parked, _, _ := st.state()

			if !reflect.DeepEqual(rows, wantRows) {
				for ref, want := range wantRows {
					if got := rows[ref]; !reflect.DeepEqual(got, want) {
						t.Fatalf("%d fires, want %d; %s:\n got %+v\nwant %+v", len(rows), len(wantRows), ref, got, want)
					}
				}

				t.Fatalf("%d fires, want %d", len(rows), len(wantRows))
			}

			if !reflect.DeepEqual(parked, wantParked) {
				t.Fatalf("parked %v, want %v", parkCodes(parked), parkCodes(wantParked))
			}
		})
	}
}

// touchedAfter is the set of fires changed by the offsets after mark, as hazard ids.
func touchedAfter(recs []feedRecord, mark int64) map[string]bool {
	e := newFireEngine(12 * time.Hour)
	out := map[string]bool{}

	for _, r := range recs {
		if ev, err := parseFeedEvent(r.Kind, r.Data); err == nil && ev.Offset == r.Offset {
			e.apply(ev)
		}

		for _, s := range e.drain() {
			if r.Offset > mark {
				out[hazardID(sourceSatelit, s.Name)] = true
			}
		}
	}

	return out
}

// Up to the bookmark a restart only rebuilds the engine. Past it, exactly the fires the new offsets changed are saved and published.
func TestSatelitPipelineReplaysSilentlyUpToTheBookmark(t *testing.T) {
	t.Parallel()

	recs := testFeed(1000)
	st := newFakeSatStore()
	toEnd(t, st, &fakeConfirmPub{}, recs[:600])

	_, parked, _, _ := st.state()
	calls := st.parkCount()

	again := &fakeConfirmPub{}
	toEnd(t, st, again, recs)

	got := map[string]bool{}
	for _, id := range again.published() {
		got[id] = true
	}

	if want := touchedAfter(recs, 599); !reflect.DeepEqual(got, want) {
		t.Fatalf("restart published %d fires, want the %d changed after the bookmark", len(got), len(want))
	}

	_, parked2, _, _ := st.state()

	if again, fresh := st.parkCount()-calls, len(parked2)-len(parked); again != fresh {
		t.Fatalf("restart parked %d records, %d of them new; records before the bookmark were parked again", again, fresh)
	}
}

// A batch saved but never confirmed by the broker keeps the bookmark where it was, so after a crash it is published again (at-least-once).
func TestSatelitPipelineRepublishesWhatTheBrokerNeverConfirmed(t *testing.T) {
	t.Parallel()

	recs := testFeed(400)
	st, held := newFakeSatStore(), &fakeConfirmPub{hold: true}

	// One batch holds the whole feed, so after the crash every fire is saved already and no upsert changes anything.
	stop := (&satelitRun{st: st, pub: held, recs: recs, crashAfter: -1, oneBatch: true}).start(t)

	eventually(t, "a publish", func() bool { return len(held.published()) > 0 })

	_ = stop()

	if _, _, c, _ := st.state(); c != "" {
		t.Fatalf("bookmark moved to %s without a broker confirm", c)
	}

	after := &fakeConfirmPub{}
	toEnd(t, st, after, recs)

	sent := map[string]bool{}
	for _, id := range after.published() {
		sent[id] = true
	}

	for _, id := range held.published() {
		if !sent[id] {
			t.Fatalf("fire %s was never confirmed and not published again", id)
		}
	}
}

// A store outage holds the batch and retries it; nothing is lost and the bookmark waits.
func TestSatelitPipelineRetriesAStoreOutage(t *testing.T) {
	t.Parallel()

	recs := testFeed(1000)
	ref := newFakeSatStore()
	toEnd(t, ref, &fakeConfirmPub{}, recs)

	st := newFakeSatStore()
	st.failUpserts = 5
	toEnd(t, st, &fakeConfirmPub{}, recs)

	want, _, _, _ := ref.state()
	if got, _, _, _ := st.state(); !reflect.DeepEqual(got, want) {
		t.Fatalf("%d fires after the outage, want %d", len(got), len(want))
	}
}

// A fenced write (a newer leader took over) stops the pipeline, tells the elector the epoch, and leaves the bookmark alone. One batch holds the whole feed, so its first write is a fire.
func TestSatelitPipelineStopsWhenFenced(t *testing.T) {
	t.Parallel()

	st := newFakeSatStore()
	st.fenced = true

	r := &satelitRun{st: st, pub: &fakeConfirmPub{}, recs: testFeed(200), crashAfter: -1, oneBatch: true}

	ctx, cancel := context.WithTimeout(withLeaseEpoch(context.Background(), 7), 3*time.Second)
	defer cancel()

	if err := r.pipeline(cancel).run(ctx); !errors.Is(err, errFenced) {
		t.Fatalf("run: %v, want errFenced", err)
	}

	if fmt.Sprint(r.fenced) != "[7]" {
		t.Fatalf("elector told %v, want [7]", r.fenced)
	}

	if _, _, c, _ := st.state(); c != "" {
		t.Fatalf("bookmark moved to %s on a fenced write", c)
	}
}

func TestLoadSatelitConfig(t *testing.T) {
	t.Setenv("SATELIT_ENABLED", "")

	if c, err := loadSatelitConfig(); err != nil || c.enabled {
		t.Fatalf("unset: %+v %v, want off", c, err)
	}

	t.Setenv("SATELIT_ENABLED", "true")
	t.Setenv("SATELLITE_BASE_URL", "https://satellite.example")
	t.Setenv("SATELLITE_GROUP_TOKEN", "")

	if _, err := loadSatelitConfig(); err == nil {
		t.Fatal("enabled without a token accepted")
	}

	t.Setenv("SATELLITE_GROUP_TOKEN", "t")

	if c, err := loadSatelitConfig(); err != nil || c.lateness != 12*time.Hour {
		t.Fatalf("defaults: %+v %v, want L = 12h", c, err)
	}

	t.Setenv("SATELIT_ENABLED", "yes please")

	if _, err := loadSatelitConfig(); err == nil {
		t.Fatal("a bad SATELIT_ENABLED accepted")
	}
}
