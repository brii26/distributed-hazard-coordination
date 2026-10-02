-- Canonical Store schema, see doc/TRD_M1.md section 6.
-- Mounted into postgres' /docker-entrypoint-initdb.d, so it runs on first
-- start of a fresh volume. Idempotent in case it is applied by hand again.
CREATE TABLE IF NOT EXISTS hazard_events (
    hazard_id     UUID PRIMARY KEY,
    source        VARCHAR(16) NOT NULL CHECK (source IN ('BMKG','PVMBG')),
    source_ref_id VARCHAR(255) NOT NULL,
    hazard_type   VARCHAR(16) NOT NULL CHECK (hazard_type IN ('SEISMIC','VOLCANIC')),
    severity      VARCHAR(16) CHECK (severity IN ('NORMAL','WASPADA','SIAGA','AWAS')),
    area_name     TEXT,
    latitude      DOUBLE PRECISION,
    longitude     DOUBLE PRECISION,
    occurred_at   TIMESTAMPTZ NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL,
    attributes    JSONB,
    UNIQUE (source, source_ref_id)
);

CREATE INDEX IF NOT EXISTS idx_hazard_type_occurred ON hazard_events (hazard_type, occurred_at);
