-- User DB schema, see doc/TRD_M1.md section 5.
-- Mounted into postgres' /docker-entrypoint-initdb.d (fresh volume only).
CREATE TABLE IF NOT EXISTS users (
    user_id       VARCHAR(255) PRIMARY KEY,
    email         VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role          VARCHAR(32) NOT NULL   -- Media | Lapangan | Operator
);

CREATE TABLE IF NOT EXISTS refresh_tokens (
    refresh_token VARCHAR(64) PRIMARY KEY,
    user_id       VARCHAR(255) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
