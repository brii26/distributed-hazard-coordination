-- One lease row per polled source, see doc/P2_LEASE_DESIGN.md. epoch is the fencing token and only grows.
-- Idempotent in case it is applied by hand again.
CREATE TABLE IF NOT EXISTS source_lease (
    source     VARCHAR(16) PRIMARY KEY CHECK (source IN ('BMKG','PVMBG')),
    holder     UUID,
    epoch      BIGINT NOT NULL DEFAULT 0,
    expires_at TIMESTAMPTZ
);
