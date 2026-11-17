-- P1 needs an empty store: older rows keep a random v4 hazard_id and NULL shard (the upsert does not update them); see doc/P1_STORAGE_DESIGN.md.
-- Shard of each record, fnv32a(hazard_id) % 16. Rows written before this
-- stay NULL until the repair loop fills them.
ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS shard SMALLINT;

CREATE INDEX IF NOT EXISTS idx_hazard_shard ON hazard_events (shard);
