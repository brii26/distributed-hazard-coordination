package main

import (
	"context"
	"errors"
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

// A withdrawn fire stays in hazard_events (its version guards against an older write) but is hidden everywhere it is read: the audit list and checksum, analytics and the type list. Rebalancing still copies it, version and withdrawal included.
func TestWithdrawnFiresAreHiddenAgainstPostgres(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, `TRUNCATE hazard_events`); err != nil {
		t.Fatal(err)
	}

	withdrawn := time.Date(2026, 11, 28, 18, 0, 0, 0, time.UTC)
	gone := fireHazard("fire-gone", 12, 1)
	gone.WithdrawnAt = &withdrawn

	for _, h := range []HazardEvent{fireHazard("fire-live", 10, 3), gone} {
		if _, err := s.upsert(ctx, h); err != nil {
			t.Fatal(err)
		}
	}

	live, all, ids, counted := readFires(t, s, analyticsSpec{Since: withdrawn.Add(-24 * time.Hour), AsOf: withdrawn, BucketSec: 3600})

	listed, _, err := s.listByType(ctx, listQuery{HazardType: typeWildfire, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}

	if len(live) != 1 || len(all) != 2 || len(ids) != 1 || ids[0] != "fire-live" || counted != 1 || len(listed) != 1 {
		t.Fatalf("audit list %d (all %d), checksum ids %v, analytics %d, type list %d; want the live fire only (2 for rebalance)", len(live), len(all), ids, counted, len(listed))
	}

	if _, err := s.db.ExecContext(ctx, `TRUNCATE hazard_events`); err != nil {
		t.Fatal(err)
	}

	if err := copyOn(ctx, s.db, all); err != nil {
		t.Fatal(err)
	}

	checkStoredFire(t, s, "fire-gone", 12, withdrawn)
}

// readFires reads the satellite rows of every shard the way the audit list (live only), rebalance (all), the checksum (ids) and analytics (count) do.
func readFires(t *testing.T, s *store, spec analyticsSpec) (live, all []HazardEvent, ids []string, counted int64) {
	t.Helper()

	ctx := context.Background()

	for shard := range numShards {
		l, err := listShardOn(ctx, s.db, shard, nil, 10, listFilter{Asc: true, Live: true, Source: sourceSatelit})
		a, err2 := listShardOn(ctx, s.db, shard, nil, 10, listFilter{Asc: true, Source: sourceSatelit})
		i, err3 := idsShardOn(ctx, s.db, shard, sourceSatelit)
		n, err4 := analyticsShardOn(ctx, s.db, shard, spec)

		if err := errors.Join(err, err2, err3, err4); err != nil {
			t.Fatal(err)
		}

		live, all, ids, counted = append(live, l...), append(all, a...), append(ids, i...), counted+n.Source[sourceSatelit]
	}

	return live, all, ids, counted
}
