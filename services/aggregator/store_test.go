package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testStore connects to the database named by CANONICAL_DB_TEST_URL and skips
// the test when it is unset, so plain `go test ./...` still works offline. The
// schema is applied from the project's own migration (it is idempotent).
func testStore(t *testing.T, poolSize int) *store {
	t.Helper()

	url := os.Getenv("CANONICAL_DB_TEST_URL")
	if url == "" {
		t.Skip("CANONICAL_DB_TEST_URL not set")
	}

	s, err := openStore(url, poolSize)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = s.close() })

	ddl, err := os.ReadFile("migrations/001_hazard_events.sql")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.db.ExecContext(context.Background(), string(ddl)); err != nil {
		t.Fatal(err)
	}

	return s
}

// ref returns a source_ref_id unique to this test and removes its rows afterwards,
// so tests never touch each other's data or anything else in the database.
func ref(t *testing.T, s *store) string {
	t.Helper()

	id := fmt.Sprintf("%s-%d-%d", strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano(), hazardN.Add(1))

	t.Cleanup(func() {
		_, _ = s.db.ExecContext(context.Background(), `DELETE FROM hazard_events WHERE source_ref_id = $1`, id)
	})

	return id
}

var (
	pastTime = time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC)
	hazardN  atomic.Int64
)

func hazard(source, refID string) HazardEvent {
	sev, area, lat, lon := severityWaspada, "Gunung Merapi", -7.5407, 110.4457

	return HazardEvent{
		HazardID:    newHazardID(),
		Source:      source,
		SourceRefID: refID,
		HazardType:  typeVolcanic,
		Severity:    &sev,
		AreaName:    &area,
		Latitude:    &lat,
		Longitude:   &lon,
		OccurredAt:  pastTime,
		IngestedAt:  time.Now().UTC().Truncate(time.Microsecond),
		Attributes:  map[string]any{"eruption_count_24h": 4.0},
	}
}

type row struct {
	id       string
	severity *string
	ingested time.Time
	attrs    map[string]any
}

func readRow(t *testing.T, s *store, source, refID string) row {
	t.Helper()

	var (
		r   row
		raw []byte
	)

	err := s.db.QueryRowContext(context.Background(), `SELECT hazard_id, severity, ingested_at, attributes FROM hazard_events WHERE source = $1 AND source_ref_id = $2`,
		source, refID).Scan(&r.id, &r.severity, &r.ingested, &raw)
	if err != nil {
		t.Fatal(err)
	}

	if err := json.Unmarshal(raw, &r.attrs); err != nil {
		t.Fatal(err)
	}

	return r
}

func count(t *testing.T, s *store, refID string) int {
	t.Helper()

	var n int
	if err := s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM hazard_events WHERE source_ref_id = $1`, refID).Scan(&n); err != nil {
		t.Fatal(err)
	}

	return n
}

func TestUpsertTwiceKeepsOneRowAndReportsTheRepeat(t *testing.T) {
	t.Parallel()

	s := testStore(t, 5)
	id := ref(t, s)

	first, err := s.upsert(context.Background(), hazard(sourcePVMBG, id))
	if err != nil || !first {
		t.Fatalf("first upsert: changed=%v err=%v, want a new record", first, err)
	}

	// The same record polled again, as the mapper would hand it over: a new
	// hazard_id and ingested_at, identical content.
	again, err := s.upsert(context.Background(), hazard(sourcePVMBG, id))
	if err != nil || again {
		t.Fatalf("repeat upsert: changed=%v err=%v, want an identical repeat", again, err)
	}

	if n := count(t, s, id); n != 1 {
		t.Fatalf("%d rows for one record, want 1", n)
	}
}

func TestChangedRecordUpdatesTheRowAndKeepsItsIdentity(t *testing.T) {
	t.Parallel()

	s := testStore(t, 5)
	id := ref(t, s)

	orig := hazard(sourcePVMBG, id)
	if _, err := s.upsert(context.Background(), orig); err != nil {
		t.Fatal(err)
	}

	esc := hazard(sourcePVMBG, id)
	siaga := severitySiaga
	esc.Severity = &siaga
	esc.IngestedAt = esc.IngestedAt.Add(time.Hour)

	changed, err := s.upsert(context.Background(), esc)
	if err != nil || !changed {
		t.Fatalf("escalation: changed=%v err=%v, want a change", changed, err)
	}

	got := readRow(t, s, sourcePVMBG, id)
	if got.severity == nil || *got.severity != severitySiaga {
		t.Fatalf("severity not updated: %v", got.severity)
	}

	if got.id != orig.HazardID || !got.ingested.Equal(orig.IngestedAt) {
		t.Fatalf("hazard_id and ingested_at must stay those of the first arrival: %+v", got)
	}
}

// P4 criterion 2: records from before and after the schema change live side by
// side in the same table and read back unchanged, with no migration in between.
func TestAttributesRoundTripBeforeAndAfterSchemaChange(t *testing.T) {
	t.Parallel()

	s := testStore(t, 5)
	legacyID, extendedID := ref(t, s), ref(t, s)

	legacy := hazard(sourcePVMBG, legacyID)

	extended := hazard(sourcePVMBG, extendedID)
	extended.Attributes = map[string]any{
		"eruption_count_24h": 4.0,
		"confidence_level":   0.75,
		"tsunami_warning":    map[string]any{"threat_level": "Siaga", "affected_zones": []any{"Bali"}},
	}

	for _, h := range []HazardEvent{legacy, extended} {
		if _, err := s.upsert(context.Background(), h); err != nil {
			t.Fatal(err)
		}
	}

	for _, h := range []HazardEvent{legacy, extended} {
		if got := readRow(t, s, sourcePVMBG, h.SourceRefID); !reflect.DeepEqual(got.attrs, h.Attributes) {
			t.Fatalf("attributes changed on the way: stored %v, sent %v", got.attrs, h.Attributes)
		}
	}
}

func TestMissingFieldsAreStoredAsNull(t *testing.T) {
	t.Parallel()

	s := testStore(t, 5)
	id := ref(t, s)

	h := hazard(sourcePVMBG, id)
	h.Severity, h.AreaName, h.Latitude, h.Longitude = nil, nil, nil, nil

	if _, err := s.upsert(context.Background(), h); err != nil {
		t.Fatal(err)
	}

	var nulls int
	if err := s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM hazard_events WHERE source_ref_id = $1 AND severity IS NULL AND area_name IS NULL AND latitude IS NULL AND longitude IS NULL`, id).Scan(&nulls); err != nil || nulls != 1 {
		t.Fatalf("expected one row with all four NULL, got %d (err %v)", nulls, err)
	}
}

func TestSameRefFromTwoSourcesDoesNotCollide(t *testing.T) {
	t.Parallel()

	s := testStore(t, 5)
	id := ref(t, s)

	for _, src := range []string{sourceBMKG, sourcePVMBG} {
		h := hazard(src, id)
		if src == sourceBMKG {
			h.HazardType = typeSeismic
		}

		if changed, err := s.upsert(context.Background(), h); err != nil || !changed {
			t.Fatalf("%s: changed=%v err=%v", src, changed, err)
		}
	}

	if n := count(t, s, id); n != 2 {
		t.Fatalf("%d rows, want one per source", n)
	}
}

func TestConcurrentUpsertOfTheSameRecordWritesOnce(t *testing.T) {
	t.Parallel()

	s := testStore(t, 10)
	id := ref(t, s)

	var (
		wg      sync.WaitGroup
		changed atomic.Int32
		failed  atomic.Int32
	)

	for range 30 {
		wg.Go(func() {
			c, err := s.upsert(context.Background(), hazard(sourcePVMBG, id))
			if err != nil {
				failed.Add(1)

				return
			}

			if c {
				changed.Add(1)
			}
		})
	}

	wg.Wait()

	if failed.Load() != 0 || changed.Load() != 1 || count(t, s, id) != 1 {
		t.Fatalf("30 racing upserts: %d failed, %d reported new, %d rows; want 0, 1, 1", failed.Load(), changed.Load(), count(t, s, id))
	}
}

func TestPoolIsCappedAndStillServesEveryCaller(t *testing.T) {
	t.Parallel()

	s := testStore(t, 3)

	if got := s.db.Stats().MaxOpenConnections; got != 3 {
		t.Fatalf("pool cap = %d, want 3", got)
	}

	var (
		wg     sync.WaitGroup
		failed atomic.Int32
	)

	for range 25 {
		id := ref(t, s) // each caller writes its own record, cleaned up afterwards

		wg.Go(func() {
			if _, err := s.upsert(context.Background(), hazard(sourcePVMBG, id)); err != nil {
				failed.Add(1)
			}
		})
	}

	wg.Wait()

	if failed.Load() != 0 || s.db.Stats().OpenConnections > 3 {
		t.Fatalf("%d failed, %d connections open, want 0 and at most 3", failed.Load(), s.db.Stats().OpenConnections)
	}
}

func TestUpsertStopsWhenItsContextIsDone(t *testing.T) {
	t.Parallel()

	s := testStore(t, 5)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.upsert(ctx, hazard(sourcePVMBG, ref(t, s))); err == nil {
		t.Fatal("a cancelled context must stop the upsert")
	}
}

func TestOpenStoreFailsFastOnAnUnreachableDatabase(t *testing.T) {
	t.Parallel()

	start := time.Now()

	if _, err := openStore("postgres://x:x@127.0.0.1:1/x?connect_timeout=1", 5); err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("want a quick error for an unreachable database, got %v after %v", err, time.Since(start))
	}
}

func TestListByTypeReturnsOnlyThatTypeWithNullsKept(t *testing.T) {
	t.Parallel()

	s := testStore(t, 5)
	volcanic, seismic := ref(t, s), ref(t, s)

	full := hazard(sourcePVMBG, volcanic)

	bare := hazard(sourceBMKG, seismic)
	bare.HazardType, bare.Severity, bare.AreaName, bare.Latitude, bare.Longitude, bare.Attributes = typeSeismic, nil, nil, nil, nil, nil

	for _, h := range []HazardEvent{full, bare} {
		if _, err := s.upsert(context.Background(), h); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.listByType(context.Background(), typeSeismic)
	if err != nil {
		t.Fatal(err)
	}

	var found *HazardEvent

	for i, h := range got {
		if h.HazardType != typeSeismic {
			t.Fatalf("a SEISMIC list returned a %s event", h.HazardType)
		}

		if h.SourceRefID == seismic {
			found = &got[i]
		}

		if h.SourceRefID == volcanic {
			t.Fatal("the volcanic event leaked into the SEISMIC list")
		}
	}

	if found == nil || found.Severity != nil || found.AreaName != nil || found.Latitude != nil || found.Longitude != nil || len(found.Attributes) != 0 {
		t.Fatalf("expected the bare event with its nulls kept, got %+v", found)
	}

	vol, err := s.listByType(context.Background(), typeVolcanic)
	if err != nil {
		t.Fatal(err)
	}

	for _, h := range vol {
		if h.SourceRefID == volcanic && (h.Severity == nil || *h.Severity != severityWaspada || h.Attributes["eruption_count_24h"] != 4.0) {
			t.Fatalf("full event came back wrong: %+v", h)
		}
	}
}
