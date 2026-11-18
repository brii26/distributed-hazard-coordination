-- Where each polled source stopped, so a restart or a new leader carries on instead of reading from the start.
-- Idempotent in case it is applied by hand again.
CREATE TABLE IF NOT EXISTS source_cursors (
    source     VARCHAR(32) PRIMARY KEY,
    cursor     TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
