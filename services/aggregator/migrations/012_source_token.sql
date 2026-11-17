-- OAuth token pair of a polled source, handed from one lease holder to the next. epoch is the lease epoch of the writer and a lower epoch never overwrites a higher one.
-- Lives in the same database as source_lease. Idempotent in case it is applied by hand again.
CREATE TABLE IF NOT EXISTS source_token (
    source        TEXT PRIMARY KEY,
    access_token  TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL,
    epoch         BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
