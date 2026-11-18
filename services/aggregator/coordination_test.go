package main

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestCoordDBFallsBackToDataPool(t *testing.T) {
	t.Parallel()

	s := &store{db: &sql.DB{}}
	if s.coordDB() != s.db {
		t.Fatal("without a coordination pool the data pool must be used")
	}

	s.coord = &sql.DB{}
	if s.coordDB() != s.coord {
		t.Fatal("the coordination pool must win when set")
	}
}

func TestOpenCoordinationKeepsOnePoolForEmptyOrSameURL(t *testing.T) {
	t.Parallel()

	s := &store{db: &sql.DB{}}

	for _, url := range []string{"", "postgres://x/data"} {
		if err := s.openCoordination(url, "postgres://x/data", 2); err != nil || s.coord != nil {
			t.Fatalf("url %q: coord=%v err=%v", url, s.coord, err)
		}
	}
}

func TestOpenCoordinationFailsOnUnreachableDatabase(t *testing.T) {
	t.Parallel()

	s := &store{db: &sql.DB{}}
	if err := s.openCoordination("postgres://u:p@127.0.0.1:1/none?sslmode=disable&connect_timeout=1", "other", 2); err == nil {
		t.Fatal("an unreachable coordination database must be an error")
	}
}

// Real Postgres with two separate databases. Needs CANONICAL_DB_TEST_URL and COORDINATION_DB_TEST_URL.
func TestLeaseCursorAndTokenUseTheCoordinationPool(t *testing.T) { //nolint:paralleltest // shares tables with other runs
	dataURL, coordURL := os.Getenv("CANONICAL_DB_TEST_URL"), os.Getenv("COORDINATION_DB_TEST_URL")
	if dataURL == "" || coordURL == "" || dataURL == coordURL {
		t.Skip("CANONICAL_DB_TEST_URL and a different COORDINATION_DB_TEST_URL needed")
	}

	ctx := context.Background()

	t.Setenv("DATABASE_URL", dataURL)
	t.Setenv("COORDINATION_DATABASE_URL", coordURL)

	s, err := loadStore()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = s.close() })

	if s.coord == nil {
		t.Fatal("a different coordination url must open a second pool")
	}

	files, err := filepath.Glob("migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}

	sort.Strings(files)

	for _, db := range []*sql.DB{s.db, s.coord} {
		for _, f := range files {
			ddl, err := os.ReadFile(f) //nolint:gosec // test reads the project's own migration files
			if err != nil {
				t.Fatal(err)
			}

			if _, err := db.ExecContext(ctx, string(ddl)); err != nil {
				t.Fatal(err)
			}
		}
	}

	const (
		src = "TEST_COORD"
		tok = "TEST-COORD-TOKEN"
	)

	t.Cleanup(func() {
		for _, db := range []*sql.DB{s.db, s.coord} {
			_, _ = db.ExecContext(ctx, `DELETE FROM source_cursors WHERE source = $1`, src)
			_, _ = db.ExecContext(ctx, `DELETE FROM source_token WHERE source = $1`, tok)
			_, _ = db.ExecContext(ctx, `DELETE FROM source_lease WHERE source = 'SATELIT'`)
		}
	})

	count := func(db *sql.DB, q string, arg any) int {
		t.Helper()

		var n int
		if err := db.QueryRowContext(ctx, q, arg).Scan(&n); err != nil {
			t.Fatal(err)
		}

		return n
	}

	if _, ok, err := (pgLease{db: s.coordDB()}).acquire(ctx, "SATELIT", newHazardID(), time.Minute); err != nil || !ok {
		t.Fatalf("lease acquire: ok=%v err=%v", ok, err)
	}

	if err := s.setCursor(ctx, src, "c1"); err != nil {
		t.Fatal(err)
	}

	exp := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	if err := (pgTokenStore{db: s.coordDB(), source: tok}).save(ctx, tokenFile{AccessToken: "a", RefreshToken: "r", ExpiresAt: exp}, 1); err != nil {
		t.Fatal(err)
	}

	for name, q := range map[string]string{
		"lease":  `SELECT count(*) FROM source_lease WHERE source = $1 AND holder IS NOT NULL`,
		"cursor": `SELECT count(*) FROM source_cursors WHERE source = $1`,
		"token":  `SELECT count(*) FROM source_token WHERE source = $1`,
	} {
		arg := map[string]string{"lease": "SATELIT", "cursor": src, "token": tok}[name]

		if n := count(s.coord, q, arg); n != 1 {
			t.Fatalf("%s: coordination database holds %d rows, want 1", name, n)
		}

		if n := count(s.db, q, arg); n != 0 {
			t.Fatalf("%s: data database holds %d rows, want 0", name, n)
		}
	}

	if c, err := s.getCursor(ctx, src); err != nil || c != "c1" {
		t.Fatalf("cursor read back: %q, %v", c, err)
	}
}
