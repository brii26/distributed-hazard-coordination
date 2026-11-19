-- Field reports from the public queue, one row per report_id. Idempotent in case it is applied by hand again.
CREATE TABLE IF NOT EXISTS field_reports (
    report_id      TEXT PRIMARY KEY,
    shard          SMALLINT NOT NULL CHECK (shard BETWEEN 0 AND 15),
    description    TEXT NOT NULL,
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION,
    kind           TEXT,
    reporter_id    TEXT NOT NULL,
    reporter_role  TEXT NOT NULL,
    received_at    TIMESTAMPTZ,
    stored_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    correlation_id TEXT
);

-- Malformed field reports are parked here too, so the source check must allow them.
ALTER TABLE failed_messages DROP CONSTRAINT IF EXISTS failed_messages_source_check;
ALTER TABLE failed_messages ADD CONSTRAINT failed_messages_source_check CHECK (source IN ('BMKG','PVMBG','SATELIT','SENSOR_PANTAI','FIELD_REPORT'));
