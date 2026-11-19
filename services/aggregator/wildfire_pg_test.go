package main

import (
	"context"
	"os"
	"testing"
	"time"
)

func fireHazard(ref string, version int64, count int) HazardEvent {
	t0 := time.Date(2026, 11, 28, 10, 5, 0, 0, time.UTC)

	return HazardEvent{
		HazardID: hazardID(sourceSatelit, ref), Source: sourceSatelit, SourceRefID: ref, HazardType: typeWildfire,
		OccurredAt: t0, IngestedAt: t0, Version: version, Attributes: map[string]any{"detection_count": count},
	}
}

func TestMigration015RunsTwice(t *testing.T) { //nolint:paralleltest // shares one database
	s := testFailedStore(t)

	ddl, err := os.ReadFile("migrations/015_wildfire.sql")
	if err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if _, err := s.db.ExecContext(context.Background(), string(ddl)); err != nil {
			t.Fatal(err)
		}
	}
}

// A fire write with a lower version never rolls the stored fire back; the same version is a no-op; a higher one is written and read back with its withdrawal time.
func TestFireVersionOnlyMovesForwardAgainstPostgres(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, `TRUNCATE hazard_events`); err != nil {
		t.Fatal(err)
	}

	if changed, err := s.upsert(ctx, fireHazard("fire-a", 100, 3)); err != nil || !changed {
		t.Fatalf("first write: changed=%v err=%v", changed, err)
	}

	if changed, err := s.upsert(ctx, fireHazard("fire-a", 90, 2)); err != nil || changed {
		t.Fatalf("lower version must change nothing: changed=%v err=%v", changed, err)
	}

	if changed, err := s.upsert(ctx, fireHazard("fire-a", 100, 3)); err != nil || changed {
		t.Fatalf("same version again must be a no-op: changed=%v err=%v", changed, err)
	}

	withdrawn := time.Date(2026, 11, 28, 18, 0, 0, 0, time.UTC)
	gone := fireHazard("fire-a", 120, 1)
	gone.WithdrawnAt = &withdrawn

	if changed, err := s.upsert(ctx, gone); err != nil || !changed {
		t.Fatalf("higher version: changed=%v err=%v", changed, err)
	}

	checkStoredFire(t, s, "fire-a", 120, withdrawn)
}

func checkStoredFire(t *testing.T, s *store, ref string, version int64, withdrawn time.Time) {
	t.Helper()

	got, err := getOn(context.Background(), s.db, hazardID(sourceSatelit, ref))
	if err != nil || got == nil {
		t.Fatalf("read back: %v %v", got, err)
	}

	if got.Version != version || got.WithdrawnAt == nil || !got.WithdrawnAt.Equal(withdrawn) {
		t.Fatalf("stored version %d withdrawn %v; want %d %s", got.Version, got.WithdrawnAt, version, withdrawn)
	}
}

// Sources without versions keep updating as before: both writes carry version 0.
func TestUnversionedSourcesStillUpdateAgainstPostgres(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, `TRUNCATE hazard_events`); err != nil {
		t.Fatal(err)
	}

	t0 := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	h := replayHazard("V1", t0)

	if _, err := s.upsert(ctx, h); err != nil {
		t.Fatal(err)
	}

	area := "Sulawesi Utara"
	h.AreaName = &area

	if changed, err := s.upsert(ctx, h); err != nil || !changed {
		t.Fatalf("version 0 update: changed=%v err=%v", changed, err)
	}
}
