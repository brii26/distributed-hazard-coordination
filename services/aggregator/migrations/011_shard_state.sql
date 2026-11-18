-- Which shards a store node holds in full. A missing row means not synced. Idempotent in case it is applied by hand again.
CREATE TABLE IF NOT EXISTS shard_state (
    shard      SMALLINT PRIMARY KEY CHECK (shard BETWEEN 0 AND 15),
    synced     BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The rebalancer takes its own lease, so the source check must allow its name.
ALTER TABLE source_lease DROP CONSTRAINT IF EXISTS source_lease_source_check;
ALTER TABLE source_lease ADD CONSTRAINT source_lease_source_check CHECK (source IN ('BMKG','PVMBG','SATELIT','SENSOR_PANTAI','rebalance'));
