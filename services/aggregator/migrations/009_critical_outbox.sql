-- Critical Sensor Pantai observations waiting to reach the critical-alerts queue.
-- A row is written before the upsert and marked sent once the broker confirmed it, so a crash or a broker outage cannot lose an alert.
-- Idempotent in case it is applied by hand again.
CREATE TABLE IF NOT EXISTS critical_outbox (
    observation_id  VARCHAR(255) PRIMARY KEY,
    correlation_id  TEXT NOT NULL DEFAULT '',
    body            BYTEA NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_critical_outbox_unsent ON critical_outbox (created_at, observation_id) WHERE sent_at IS NULL;
