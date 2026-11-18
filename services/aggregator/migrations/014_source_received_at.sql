-- When the message that wrote the stored row was received. The replay guard compares it on a source-time tie (updated_at is the write time, which is the replay time for a replayed row).
-- Idempotent in case it is applied by hand again. Existing rows start at their last write time.
ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS source_received_at TIMESTAMPTZ;
UPDATE hazard_events SET source_received_at = updated_at WHERE source_received_at IS NULL;
ALTER TABLE hazard_events ALTER COLUMN source_received_at SET DEFAULT now();
ALTER TABLE hazard_events ALTER COLUMN source_received_at SET NOT NULL;
