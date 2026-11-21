-- Lets the store keep Sensor Pantai observations: source SENSOR_PANTAI and hazard type COASTAL.
-- Idempotent in case it is applied by hand again. The lists also hold SATELIT and WILDFIRE (keep in sync with 015_wildfire.sql), so applying this again after fires exist does not fail.
ALTER TABLE hazard_events DROP CONSTRAINT IF EXISTS hazard_events_source_check;
ALTER TABLE hazard_events ADD CONSTRAINT hazard_events_source_check
    CHECK (source IN ('BMKG','PVMBG','SENSOR_PANTAI','SATELIT'));

ALTER TABLE hazard_events DROP CONSTRAINT IF EXISTS hazard_events_hazard_type_check;
ALTER TABLE hazard_events ADD CONSTRAINT hazard_events_hazard_type_check
    CHECK (hazard_type IN ('SEISMIC','VOLCANIC','COASTAL','WILDFIRE'));
