#!/bin/sh
set -eu

# The local-only fixture has no persistent data or reusable credentials.
image=postgres@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0
container_id=$(docker run --detach --rm \
    --label io.synod.fixture=postgres-integration \
    --publish 127.0.0.1::5432 \
    --env POSTGRES_HOST_AUTH_METHOD=trust \
    --env POSTGRES_DB=synod_test \
    "$image")
trap 'if ! docker rm --force "$container_id" >/dev/null 2>&1; then printf "%s\n" "Could not remove PostgreSQL fixture $container_id" >&2; fi' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

attempt=0
until docker exec "$container_id" pg_isready --host 127.0.0.1 --username postgres --dbname synod_test >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 30 ]; then
        printf '%s\n' 'PostgreSQL fixture did not become ready after 30 readiness checks.' >&2
        exit 1
    fi
    sleep 1
done

address=$(docker port "$container_id" 5432/tcp)
export SYNOD_TEST_DATABASE_URL="postgres://postgres@$address/synod_test?sslmode=disable"
if [ "$#" -eq 0 ]; then
    set -- go test -race ./...
fi
"$@"
