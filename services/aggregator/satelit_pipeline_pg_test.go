package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// Criterion 5 (#313) once against Postgres: kill the pipeline twice, recover, and the stored fires and parked records equal a run without a crash. This checks the real versioned upsert, failed_messages and source_cursors.
func TestSatelitPipelineCrashAgainstPostgres(t *testing.T) { //nolint:paralleltest // shares one truncated table
	s := testFailedStore(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, `TRUNCATE hazard_events; DELETE FROM source_cursors WHERE source = 'SATELIT'`); err != nil {
		t.Fatal(err)
	}

	recs := testFeed(3000)
	ref := newFakeSatStore()
	toEnd(t, ref, &fakeConfirmPub{}, recs)

	for _, at := range []int64{800, 2100} {
		stop := (&satelitRun{real: s, pub: &fakeConfirmPub{}, recs: recs, crashAfter: at}).start(t)
		if err := stop(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}

	stop := (&satelitRun{real: s, pub: &fakeConfirmPub{}, recs: recs, crashAfter: -1}).start(t)
	last := strconv.FormatInt(recs[len(recs)-1].Offset, 10)

	for deadline := time.Now().Add(time.Minute); ; time.Sleep(20 * time.Millisecond) { // a real store is slower than eventually's 3 s
		if c, err := s.getCursor(ctx, sourceSatelit); err == nil && c == last {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the bookmark at " + last)
		}
	}

	_ = stop()

	wantRows, wantParked, _, _ := ref.state()

	if got, want := storedFires(t, s), firesJSON(t, wantRows); !reflect.DeepEqual(got, want) {
		t.Fatalf("Postgres holds %d fires, want %d (or they differ)", len(got), len(want))
	}

	if got := storedParked(t, s); !reflect.DeepEqual(got, wantParked) {
		t.Fatalf("Postgres parked %v, want %v", parkCodes(got), parkCodes(wantParked))
	}
}

// firesJSON is each fire as JSON with ingested_at cleared, so the stored and the in-memory copies compare.
func firesJSON(t *testing.T, rows map[string]HazardEvent) map[string]string {
	t.Helper()

	out := map[string]string{}

	for ref, h := range rows {
		h.IngestedAt, h.OccurredAt, h.RawAttributes = time.Time{}, h.OccurredAt.UTC(), nil
		if h.WithdrawnAt != nil {
			w := h.WithdrawnAt.UTC()
			h.WithdrawnAt = &w
		}

		b, err := json.Marshal(h)
		if err != nil {
			t.Fatal(err)
		}

		out[ref] = string(b)
	}

	return out
}

func storedFires(t *testing.T, s *store) map[string]string {
	t.Helper()

	rows := map[string]HazardEvent{}

	for shard := range numShards {
		got, err := listShardOn(context.Background(), s.db, shard, nil, 100000, listFilter{Asc: true, Source: sourceSatelit})
		if err != nil {
			t.Fatal(err)
		}

		for _, h := range got {
			if err := json.Unmarshal(h.RawAttributes, &h.Attributes); err != nil {
				t.Fatal(err)
			}

			rows[h.SourceRefID] = h
		}
	}

	return firesJSON(t, rows)
}

func storedParked(t *testing.T, s *store) map[string]string {
	t.Helper()

	rows, err := s.db.QueryContext(context.Background(), `SELECT message_id, reason FROM failed_messages WHERE source = 'SATELIT'`)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = rows.Close() }()

	out := map[string]string{}

	for rows.Next() {
		var id, reason string
		if err := rows.Scan(&id, &reason); err != nil {
			t.Fatal(err)
		}

		out[id] = reason
	}

	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	return out
}
