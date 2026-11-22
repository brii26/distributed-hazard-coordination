-- failed_messages also lives in coordination-db (#409), which runs 003 and this file only, not 010 or 013
-- (both also touch store tables). The failed_messages parts of 010 and 013, same definitions, safe to run again.
ALTER TABLE failed_messages ADD COLUMN IF NOT EXISTS mapped_event     JSONB;
ALTER TABLE failed_messages ADD COLUMN IF NOT EXISTS next_attempt_at  TIMESTAMPTZ;
ALTER TABLE failed_messages ADD COLUMN IF NOT EXISTS correlation_id   TEXT;

CREATE INDEX IF NOT EXISTS idx_failed_due ON failed_messages (next_attempt_at) WHERE status = 'retrying';

ALTER TABLE failed_messages DROP CONSTRAINT IF EXISTS failed_messages_source_check;
ALTER TABLE failed_messages ADD CONSTRAINT failed_messages_source_check CHECK (source IN ('BMKG','PVMBG','SATELIT','SENSOR_PANTAI','FIELD_REPORT'));
