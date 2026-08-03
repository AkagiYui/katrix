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
# /data is owned by postgres (for the PG data dir); the katrix runtime dir and
# signing key must be writable by the katrix user, so chown just that subdir.
chown -R postgres:postgres /data "$SOCKDIR"
mkdir -p "$KATRIX_DIR"
chown -R katrix:katrix "$KATRIX_DIR"

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
# Public base URL is deliberately NOT set: Complement runs the homeserver
# behind a mitmproxy reverse proxy, so the reachable client URL is the proxy's
# host port, not "https://$SERVER_NAME". Without public_base_url, katrix (like
# Synapse's complement image) serves no client well-known, and clients keep
# their working proxy URL instead of being redirected to an unreachable one
# (the matrix-rust-sdk's respect_login_well_known would switch homeserver URLs
# and fail every request).
export KATRIX_LISTEN_CLIENT=":8008"
export KATRIX_LISTEN_FEDERATION=":8448"
export KATRIX_FEDERATION_ENABLED="${KATRIX_FEDERATION_ENABLED:-true}"
export KATRIX_REGISTRATION_ENABLED="${KATRIX_REGISTRATION_ENABLED:-true}"
# Complement's URL-preview fixture (web.NewServer) is served at
# host.docker.internal:PORT, which resolves to a reserved range; let the SSRF
# guard reach it. Production deployments leave this off.
export KATRIX_SSRF_ALLOW_PRIVATE_IPS="${KATRIX_SSRF_ALLOW_PRIVATE_IPS:-true}"

# Trust Complement's CA for OUTBOUND federation requests too: SSL_CERT_FILE
# points at /ca/complement-ca.crt, so when Complement mounts its CA (at
# /complement/ca/ca.crt) we must copy it there. Without the copy the file stays
# empty and every outbound make_join/send_join/key request fails TLS with
# "certificate signed by unknown authority". When no CA is mounted (local
# runs), an empty file just means the default system roots are used.
mkdir -p /ca
if [ -s "/complement/ca/ca.crt" ]; then
  chmod a+r "/complement/ca/ca.crt" 2>/dev/null || true
  cp "/complement/ca/ca.crt" /ca/complement-ca.crt
else
  touch /ca/complement-ca.crt 2>/dev/null || true
fi

# Generate a TLS leaf certificate signed by Complement's CA so the federation
# listener (:8448) can serve HTTPS. Complement mounts its CA at
# /complement/ca/ca.crt + /complement/ca/ca.key; the homeserver must present a
# certificate signed by that CA. If the CA is not mounted (local runs), fall
# back to plain HTTP federation.
COMPLEMENT_CA="/complement/ca/ca.crt"
COMPLEMENT_CA_KEY="/complement/ca/ca.key"
FED_CERT="$KATRIX_DIR/fed.crt"
FED_KEY="$KATRIX_DIR/fed.key"
if [ -s "$COMPLEMENT_CA" ] && [ -s "$COMPLEMENT_CA_KEY" ]; then
  # Ensure the katrix user can read the CA files (Complement mounts them as root).
  chmod a+r "$COMPLEMENT_CA" "$COMPLEMENT_CA_KEY" 2>/dev/null || true
  su-exec katrix /usr/local/bin/katrix gencert \
    -ca-cert "$COMPLEMENT_CA" \
    -ca-key "$COMPLEMENT_CA_KEY" \
    -server-name "$SERVER_NAME" \
    -out-cert "$FED_CERT" \
    -out-key "$FED_KEY" >/dev/null 2>&1 && \
  export KATRIX_FEDERATION_TLS_CERT="$FED_CERT" \
         KATRIX_FEDERATION_TLS_KEY="$FED_KEY"
fi

# Run katrix in the foreground (PID 1) so Complement's SIGTERM reaches it
# directly.
exec su-exec katrix /usr/local/bin/katrix serve