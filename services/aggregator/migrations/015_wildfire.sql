-- Lets the store keep satellite fires (P4): source SATELIT and hazard type WILDFIRE.
-- version is the feed offset that last changed a fire; a write with a lower version is refused, so a replay never rolls a fire back. Other sources stay at 0.
-- withdrawn_at marks a fire that shrank below 2 detections or was merged into another; the audit API leaves it out of count and checksum.
-- Idempotent in case it is applied by hand again. The CHECK lists match 007_coastal.sql (keep in sync); this file widens stores that already ran the old 007.
ALTER TABLE hazard_events DROP CONSTRAINT IF EXISTS hazard_events_source_check;
ALTER TABLE hazard_events ADD CONSTRAINT hazard_events_source_check
    CHECK (source IN ('BMKG','PVMBG','SENSOR_PANTAI','SATELIT'));

ALTER TABLE hazard_events DROP CONSTRAINT IF EXISTS hazard_events_hazard_type_check;
ALTER TABLE hazard_events ADD CONSTRAINT hazard_events_hazard_type_check
    CHECK (hazard_type IN ('SEISMIC','VOLCANIC','COASTAL','WILDFIRE'));

ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS withdrawn_at TIMESTAMPTZ;
