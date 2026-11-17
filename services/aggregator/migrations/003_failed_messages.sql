-- Messages the mapper rejected, kept byte for byte so they can be inspected or replayed.
-- Idempotent in case it is applied by hand again.
CREATE TABLE IF NOT EXISTS failed_messages (
    message_id     TEXT PRIMARY KEY,
    source         VARCHAR(16) NOT NULL CHECK (source IN ('BMKG','PVMBG','SATELIT','SENSOR_PANTAI')),
    received_at    TIMESTAMPTZ NOT NULL,
    payload        BYTEA NOT NULL,
    payload_sha256 BYTEA NOT NULL,
    payload_size   INT NOT NULL,
    oversize       BOOLEAN NOT NULL DEFAULT FALSE,
    reason         TEXT NOT NULL,
    retryable      BOOLEAN NOT NULL,
    attempts       INT NOT NULL DEFAULT 0,
    status         VARCHAR(16) NOT NULL CHECK (status IN ('retrying','parked','recovered')),
    source_ref_id  VARCHAR(255),
    last_error_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_failed_status_received ON failed_messages (status, received_at, message_id);
