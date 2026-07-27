#!/usr/bin/env bash
# katrix-complement-entrypoint.sh — start a per-server Postgres + Katrix pair
# for Complement. Complement launches a fresh container per test from the base
# image, so we clone the pre-initialised template cluster (fast) instead of
# running initdb on every start, keeping startup well under Complement's
# readiness timeout.

set -euo pipefail

: "${SERVER_NAME:?Complement must set SERVER_NAME}"
TEMPLATE="/var/lib/postgresql/template"
PGDATA="/data/pg"
SOCKDIR="/tmp/pg"
KATRIX_DIR="/data/katrix"

mkdir -p "$PGDATA" "$SOCKDIR" "$KATRIX_DIR"
chown -R postgres:postgres /data "$SOCKDIR"

# Clone the template cluster if this container has no data dir yet (the common
# Complement case). cp -a preserves permissions; we then clear the old socket
# and pidfile that the template may carry.
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  cp -a "$TEMPLATE/." "$PGDATA/"
  rm -f "$PGDATA/postmaster.pid"
fi

# Start Postgres on a unix socket inside SOCKDIR (no TCP listener).
su-exec postgres pg_ctl -D "$PGDATA" -l /data/pg.log -w \
  -o "-c listen_addresses=''" \
  -o "-c unix_socket_directories=$SOCKDIR" \
  -o "-c log_min_messages=PANIC" \
  start >/dev/null 2>&1

# Wait until the socket accepts connections (max ~5s).
for i in $(seq 1 50); do
  if su-exec postgres psql -h "$SOCKDIR" -U postgres -d postgres -c "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

# Create the katrix database (idempotent).
su-exec postgres psql -h "$SOCKDIR" -U postgres -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname='katrix'" | grep -q 1 \
  || su-exec postgres createdb -h "$SOCKDIR" -U postgres katrix

export KATRIX_SERVER_NAME="$SERVER_NAME"
export KATRIX_DATABASE_DSN="postgres://postgres@localhost/katrix?host=$SOCKDIR&sslmode=disable"
export KATRIX_SIGNING_KEY_PATH="$KATRIX_DIR/signing.key"
export KATRIX_MEDIA_STORE_PATH="$KATRIX_DIR/media"
export KATRIX_PUBLIC_BASE_URL="https://$SERVER_NAME"
export KATRIX_LISTEN_CLIENT=":8008"
export KATRIX_LISTEN_FEDERATION=":8448"
export KATRIX_FEDERATION_ENABLED="${KATRIX_FEDERATION_ENABLED:-true}"
export KATRIX_REGISTRATION_ENABLED="${KATRIX_REGISTRATION_ENABLED:-true}"

# Touch the CA file so SSL_CERT_FILE points at a real path even when Complement
# has not mounted one (avoids a noisy open error on first outbound request).
mkdir -p /ca
touch /ca/complement-ca.crt 2>/dev/null || true

# Run katrix in the foreground (PID 1) so Complement's SIGTERM reaches it
# directly.
exec su-exec katrix /usr/local/bin/katrix serve