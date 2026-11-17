-- The lease table also holds the Sensor Pantai and satellite sources. Safe on a fresh and on an existing database.
ALTER TABLE source_lease DROP CONSTRAINT IF EXISTS source_lease_source_check;
ALTER TABLE source_lease ADD CONSTRAINT source_lease_source_check
    CHECK (source IN ('BMKG','PVMBG','SATELIT','SENSOR_PANTAI'));
