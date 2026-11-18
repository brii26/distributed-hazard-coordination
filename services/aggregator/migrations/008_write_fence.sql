-- Highest lease fencing token a store node has accepted per source. Each node keeps its own row.
-- Idempotent in case it is applied by hand again.
CREATE TABLE IF NOT EXISTS write_fence (
    source    TEXT PRIMARY KEY,
    max_token BIGINT NOT NULL
);
