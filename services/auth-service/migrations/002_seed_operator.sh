#!/bin/sh
# Seeds the initial Operator account (runs once, on a fresh volume).
# password_hash = hex(sha256(password || SECRET_SALT)), per doc/TRD_M1.md
# section 4.1; the Login RPC must compute it the same way.
set -eu

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -v email="$SEED_OPERATOR_EMAIL" -v pw="$SEED_OPERATOR_PASSWORD" -v salt="$SECRET_SALT" <<'SQL'
INSERT INTO users (user_id, email, password_hash, role)
VALUES (gen_random_uuid()::text, :'email',
        encode(sha256(convert_to(:'pw' || :'salt', 'UTF8')), 'hex'), 'Operator')
ON CONFLICT (email) DO NOTHING;
SQL
