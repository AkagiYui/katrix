#!/bin/bash
#
# katrix_sytest.sh — run SyTest against katrix.
#
# Ported from sytest's scripts/dendrite_sytest.sh. Invoked by the
# sytest-katrix image's /bootstrap.sh (which sets SYTEST_TARGET=katrix and
# downloads a sytest checkout into /sytest). Expects the katrix source tree
# bind-mounted at /src and a sytest checkout (with Katrix.pm grafted into
# lib/SyTest/Homeserver/ and this script into scripts/) at /sytest.
#
# katrix differs from dendrite in that it ships a single `katrix` binary with
# a `serve` subcommand (no separate generate-keys / generate-config helpers),
# and migrations + signing-key creation happen automatically inside `serve`.

set -ex

cd /sytest

mkdir -p /work

# Make sure all Perl deps are installed -- this is done in the docker build so
# will only install packages added since the last Docker build.
./install-deps.pl

# Start the database
su -c 'eatmydata /usr/lib/postgresql/*/bin/pg_ctl -w -D $PGDATA start' postgres

# Create required databases (katrix uses a single DB per server, like dendrite,
# since all tables live in one schema).
su -c 'for i in pg1 pg2 sytest_template; do psql -c "CREATE DATABASE $i;"; done' postgres

export PGUSER=postgres
export POSTGRES_DB_1=pg1
export POSTGRES_DB_2=pg2
export GOBIN=/tmp/bin

# Write out the per-server database.yaml config (server-0 / server-1) that
# Katrix.pm reads via _get_dbconfig.
./scripts/prep_sytest_for_postgres.sh

# Build katrix from source
echo >&2 "--- Building katrix from source"
cd /src
mkdir -p $GOBIN

if [[ -z ${COVER} || ${COVER} -eq 0 ]]; then
    go install -buildvcs=false -race=${RACE_DETECTION:-0} -v ./cmd/katrix
else
    go test -c -cover -covermode=atomic -race=${RACE_DETECTION:-0} -buildvcs=false \
        -o $GOBIN/katrix -coverpkg "github.com/AkagiYui/katrix/..." ./cmd/katrix
fi
cd -

# Run the tests
echo >&2 "+++ Running tests"

TEST_STATUS=0
mkdir -p /logs
./run-tests.pl -I Katrix::Monolith -d $GOBIN \
    -W /src/sytest/whitelist -B /src/sytest/blacklist \
    -O tap --all --work-directory="/work" --exclude-deprecated \
    "$@" > /logs/results.tap &
pid=$!

# make sure that we kill the test runner on SIGTERM, SIGINT, etc
trap 'kill $pid' TERM INT
wait $pid || TEST_STATUS=$?
trap - TERM INT

if [ $TEST_STATUS -ne 0 ]; then
    echo >&2 -e "run-tests \e[31mFAILED\e[0m: exit code $TEST_STATUS"
else
    echo >&2 -e "run-tests \e[32mPASSED\e[0m"
fi

# Check for new tests to be added to the test whitelist
/src/sytest/show-expected-fail-tests.sh /logs/results.tap /src/sytest/whitelist \
    /src/sytest/blacklist | tee /work/show_expected_fail_tests_output.txt || TEST_STATUS=$?

echo >&2 "--- Copying assets"

# Copy out the logs
rsync -r --ignore-missing-args --min-size=1B -av /work/server-0 /work/server-1 /logs \
    --include "*/" --include="*.log.*" --include="*.log" --exclude="*"
find /logs | xargs -r chmod go+rX

# Sytest compliance report
(cd /src && ./sytest/are-we-synapse-yet.py /logs/results.tap) || true

exit $TEST_STATUS