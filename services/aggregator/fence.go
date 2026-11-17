package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// A write carried a token lower than the highest one this store has seen for the source.
var errFenced = errors.New("fenced")

type fencedError struct {
	source string
	token  int64
}

func (e *fencedError) Error() string {
	return fmt.Sprintf("fenced: %s write with token %d is stale", e.source, e.token)
}

func (e *fencedError) Is(target error) bool { return target == errFenced }

// Records the token as the highest seen, or reports no row when a higher one is already stored. The conflict path locks the row, so concurrent writers of one source take turns and the check holds until commit.
const fenceSQL = `
INSERT INTO write_fence (source, max_token) VALUES ($1, $2)
ON CONFLICT (source) DO UPDATE SET max_token = EXCLUDED.max_token
WHERE write_fence.max_token <= EXCLUDED.max_token
RETURNING max_token`

// Runs inside the write transaction. A context without a lease epoch is unfenced (dev mode, tests): nothing is checked or recorded.
func fenceTx(ctx context.Context, tx *sql.Tx, source string) error {
	token, ok := leaseEpoch(ctx)
	if !ok {
		return nil
	}

	var kept int64

	err := tx.QueryRowContext(ctx, fenceSQL, source, token).Scan(&kept)
	if errors.Is(err, sql.ErrNoRows) {
		return &fencedError{source: source, token: token}
	}

	return err
}
