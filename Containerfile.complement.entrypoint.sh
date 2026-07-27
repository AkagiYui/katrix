#!/usr/bin/env bash
# katrix-complement-entrypoint.sh — start a per-server Postgres + Katrix pair
# for Complement. Complement expects the homeserver to be reachable on :8008
# (client API) and :8448 (federation API) immediately after start, and to use
# SERVER_NAME as the homeserver name.
#
# State lives under /data so Complement can snapshot it; the Postgres data
# directory is /data/pg and the katrix signing key is /data/signing.key.

set -euo pipefail

: "${SERVER_NAME:?Complement must set SERVER_NAME}"
PGDATA="/data/pg"
KATRIX_DIR="/data/katrix"

# Postgres needs to run as the postgres user; /data is owned by root in
# Complement's container, so chown it to postgres before initdb.
mkdir -p "$PGDATA" "$KATRIX_DIR"
chown -R postgres:postgres /data

# Initialise the cluster once.
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  su-exec postgres pg_ctl initdb -D "$PGDATA" -o --auth=trust --encoding=UTF8
fi

# Start Postgres on the default socket inside the data dir.
su-exec postgres pg_ctl -D "$PGDATA" -l /data/pg.log -o "-c listen_addresses=''" -o "-c unix_socket_directories=$PGDATA" start -w

# Create the katrix database (idempotent).
su-exec postgres psql -h "$PGDATA" -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='katrix'" \
  | grep -q 1 || su-exec postgres createdb -h "$PGDATA" katrix

export KATRIX_SERVER_NAME="$SERVER_NAME"
export KATRIX_DATABASE_DSN="postgres:///katrix?host=/data/pg&sslmode=disable"
export KATRIX_SIGNING_KEY_PATH="$KATRIX_DIR/signing.key"
export KATRIX_MEDIA_STORE_PATH="$KATRIX_DIR/media"
export KATRIX_PUBLIC_BASE_URL="https://$SERVER_NAME"

# Touch the CA file so SSL_CERT_FILE points at a real path even when Complement
# has not mounted one (avoids a noisy open error on first outbound request).
mkdir -p /ca
touch /ca/complement-ca.crt 2>/dev/null || true

# Run katrix in the foreground. Complement sends SIGTERM to stop the server.
exec su-exec katrix /usr/local/bin/katrix serve