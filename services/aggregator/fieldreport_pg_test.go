package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// A NUL passes JSON and UTF-8 checks, so only Postgres can refuse it. The refusal must be recognised as final, not as a store outage.
func TestFieldReportNULIsRefusedByPostgres(t *testing.T) { //nolint:paralleltest // shares one database
	s := testStore(t, 16)

	r := fieldReport{ReportID: "nul-1", Description: "banjir \x00 x", ReporterID: "a@b.c", ReporterRole: "warga"}

	_, err := insertFieldReportOn(context.Background(), s.db, r, "c1")
	if err == nil || !rejectedByStore(err) {
		t.Fatalf("want a SQLSTATE 22 error, got %v", err)
	}

	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "22021" {
		t.Fatalf("code %v, want 22021", err)
	}
}

func TestFieldReportPostgres(t *testing.T) { //nolint:paralleltest,gocyclo // shares one database; one flat scenario
	s := testStore(t, 16)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, "TRUNCATE field_reports"); err != nil {
		t.Fatal(err)
	}

	mig, err := os.ReadFile("migrations/013_field_reports.sql")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.db.ExecContext(ctx, string(mig)); err != nil {
		t.Fatalf("migration applied twice: %v", err)
	}

	if err := s.insertFailed(ctx, newFailedMessage(sourceFieldReport, []byte("{"), "malformed", false, time.Now())); err != nil {
		t.Fatalf("FIELD_REPORT row: %v", err)
	}

	count := func(distinct bool) (n int) {
		q := "SELECT count(*) FROM field_reports"
		if distinct {
			q = "SELECT count(DISTINCT report_id) FROM field_reports"
		}

		if err := s.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
			t.Fatal(err)
		}

		return n
	}

	run := func(ids []string, workers int) (fresh int) {
		var (
			wg sync.WaitGroup
			mu sync.Mutex
		)

		ch := make(chan string)

		for range workers {
			wg.Go(func() {
				for id := range ch {
					r, err := parseFieldReport(id, reportBody(id, nil))
					if err != nil {
						t.Error(err)

						continue
					}

					changed, err := insertFieldReportOn(ctx, s.db, r, "c")
					if err != nil {
						t.Error(err)
					}

					if changed {
						mu.Lock()
						fresh++
						mu.Unlock()
					}
				}
			})
		}

		for _, id := range ids {
			ch <- id
		}

		close(ch)
		wg.Wait()

		return fresh
	}

	same := make([]string, 20)
	for i := range same {
		same[i] = "same-id"
	}

	if fresh := run(same, 20); fresh != 1 || count(false) != 1 {
		t.Fatalf("20 parallel saves: fresh=%d rows=%d", fresh, count(false))
	}

	many := make([]string, 1000)
	for i := range many {
		many[i] = fmt.Sprintf("many-%d", i)
	}

	run(many, 8)

	if count(false) != 1001 || count(true) != 1001 {
		t.Fatalf("1000 distinct: rows=%d distinct=%d", count(false), count(true))
	}
}
