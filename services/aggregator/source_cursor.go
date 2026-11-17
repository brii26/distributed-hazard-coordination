package main

import (
	"context"
	"database/sql"
	"errors"
)

// The saved cursor of source, or "" when none was saved yet.
func (s *store) getCursor(ctx context.Context, source string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	var c string

	err := s.db.QueryRowContext(ctx, `SELECT cursor FROM source_cursors WHERE source = $1`, source).Scan(&c)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}

	return c, err
}

// Saves the cursor of source, replacing the old one.
func (s *store) setCursor(ctx context.Context, source, cursor string) error {
	ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	_, err := s.db.ExecContext(ctx, `
INSERT INTO source_cursors (source, cursor, updated_at) VALUES ($1, $2, now())
ON CONFLICT (source) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()`, source, cursor)

	return err
}
