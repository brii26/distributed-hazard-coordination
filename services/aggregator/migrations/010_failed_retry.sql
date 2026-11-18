-- Retry bookkeeping for failed_messages. Idempotent in case it is applied by hand again.
-- payload keeps the record as received; mapped_event is the mapped HazardEvent the retry worker writes.
ALTER TABLE failed_messages ADD COLUMN IF NOT EXISTS mapped_event     JSONB;
ALTER TABLE failed_messages ADD COLUMN IF NOT EXISTS next_attempt_at  TIMESTAMPTZ;
ALTER TABLE failed_messages ADD COLUMN IF NOT EXISTS correlation_id   TEXT;

-- Last write time of a hazard, moved by every real change. The retry guard compares it with the failure time (ingested_at never changes on update).
ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_failed_due ON failed_messages (next_attempt_at) WHERE status = 'retrying';
