#!/bin/sh
# Test live SQLite copies on Linux, including the pinned-directory VFS.
set -eu
if [ -n "${SYNOD_TEST_DOCKER_HOST:-}" ]; then
    export DOCKER_HOST="$SYNOD_TEST_DOCKER_HOST"
fi
fixture_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
fixture_modules=$(go env GOMODCACHE)
fixture_temporary=$(mktemp -d)
fixture_container=''
cleanup() {
    if [ -n "$fixture_container" ]; then docker rm -f "$fixture_container" >/dev/null; fi
    rm -rf "$fixture_temporary"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
fixture_cache=${SYNOD_TEST_GO_CACHE:-$fixture_temporary/cache}
mkdir -p "$fixture_cache"
fixture_image=golang@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd
fixture_container=$(docker create --network none --label synod.test=sqlite \
    -e GOMAXPROCS=4 -e GOTOOLCHAIN=local -e GOCACHE=/cache \
    -v "$fixture_root:/src:ro" -v "$fixture_modules:/go/pkg/mod:ro" -v "$fixture_cache:/cache" \
    -w /src "$fixture_image" \
    go test -race -p 1 -count=1 -timeout=3m ./internal/agent/backup \
    -run 'TestSQLite|TestTreeCopiesLiveSQLite' "$@")
docker start -a "$fixture_container"
fixture_status=$(docker inspect --format '{{.State.ExitCode}}' "$fixture_container")
exit "$fixture_status"
