package main

import (
	"context"
	"testing"
)

func TestSourceCursorRoundTrip(t *testing.T) {
	s := testStore(t, 2)
	ctx := context.Background()

	const src = "TEST_CURSOR"

	t.Cleanup(func() { _, _ = s.db.ExecContext(ctx, `DELETE FROM source_cursors WHERE source = $1`, src) })

	if c, err := s.getCursor(ctx, src); err != nil || c != "" {
		t.Fatalf("empty store: %q, %v", c, err)
	}

	for _, want := range []string{"first", "second"} {
		if err := s.setCursor(ctx, src, want); err != nil {
			t.Fatal(err)
		}

		if c, err := s.getCursor(ctx, src); err != nil || c != want {
			t.Fatalf("got %q, %v, want %q", c, err, want)
		}
	}
}
