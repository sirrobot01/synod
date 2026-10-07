#!/bin/sh
# Test app restore on Linux, where the test reads named volume data from the Docker data root.
# The work directory has the same path in the test container and on the Docker host.
# It is local to the Docker host, so Docker Desktop does not share it through macOS.
# Pass test binary flags, such as -test.run NAME or -test.v.
set -eu
if [ -n "${SYNOD_TEST_DOCKER_HOST:-}" ]; then
    export DOCKER_HOST="$SYNOD_TEST_DOCKER_HOST"
fi
fixture_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
fixture_modules=$(go env GOMODCACHE)
fixture_temporary=$(mktemp -d)
fixture_work=/var/tmp/synod-restore-test-$(basename "$fixture_temporary")
fixture_container=''
fixture_cli=docker@sha256:b1805116a6a86cc591b5d5f60a910a0715cdcc9d18d866ad68b1457ead25c35c
cleanup() {
    if [ -n "$fixture_container" ]; then docker rm -f "$fixture_container" >/dev/null; fi
    docker run --rm --network none -v /var/tmp:/host-tmp --entrypoint rm "$fixture_cli" -rf "/host-tmp/$(basename "$fixture_work")" >/dev/null || true
    rm -rf "$fixture_temporary"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
fixture_cache=${SYNOD_TEST_GO_CACHE:-$fixture_temporary/cache}
mkdir -p "$fixture_cache"
fixture_image=golang@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd
docker pull -q "$fixture_cli" >/dev/null
fixture_container=$(docker create --network none --label synod.test=restore \
    -e GOMAXPROCS=4 -e GOTOOLCHAIN=local -e GOCACHE=/cache -e GOFLAGS=-buildvcs=false \
    -e FIXTURE_WORK="$fixture_work" -e SYNOD_TEST_DOCKER_HOST=unix:///var/run/docker.sock \
    -v /var/run/docker.sock:/var/run/docker.sock -v /var/lib/docker/volumes:/var/lib/docker/volumes \
    -v "$fixture_work:$fixture_work" \
    --mount "type=image,source=$fixture_cli,target=/docker-cli" \
    -v "$fixture_root:/src:ro" -v "$fixture_modules:/go/pkg/mod:ro" -v "$fixture_cache:/cache" \
    -w /src "$fixture_image" sh -ec '
mkdir -p /root/.docker/cli-plugins
ln -s /docker-cli/usr/local/libexec/docker/cli-plugins/docker-compose /root/.docker/cli-plugins/docker-compose
ln -s /docker-cli/usr/local/bin/docker /usr/local/bin/docker
go test -c -race -o /tmp/restore.test ./internal/agent/restore
cd internal/agent/restore
TMPDIR=$FIXTURE_WORK exec /tmp/restore.test -test.count=1 -test.timeout=10m "$@"' sh "$@")
docker start -a "$fixture_container"
fixture_status=$(docker inspect --format '{{.State.ExitCode}}' "$fixture_container")
exit "$fixture_status"
