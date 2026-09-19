#!/bin/sh
set -eu

# The local-only fixture has no persistent data or reusable credentials.
image=quay.io/coreos/etcd@sha256:dfd3941bf6ced5fdb700f9b2d98b22b7bca7ceee13aec16224f93ff30d9a59c4
container_id=$(docker run --detach --rm \
    --label io.synod.fixture=etcd-integration \
    --publish 127.0.0.1::2379 \
    "$image" \
    /usr/local/bin/etcd \
    --name synod-test \
    --data-dir /tmp/synod-test-etcd \
    --listen-client-urls http://0.0.0.0:2379 \
    --advertise-client-urls http://127.0.0.1:2379 \
    --listen-peer-urls http://127.0.0.1:2380 \
    --initial-advertise-peer-urls http://127.0.0.1:2380 \
    --initial-cluster synod-test=http://127.0.0.1:2380)
trap 'if ! docker rm --force "$container_id" >/dev/null 2>&1; then printf "%s\n" "Could not remove etcd fixture $container_id" >&2; fi' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

attempt=0
until docker exec "$container_id" etcdctl --endpoints http://127.0.0.1:2379 endpoint health >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 30 ]; then
        printf '%s\n' 'etcd fixture did not become ready after 30 readiness checks.' >&2
        exit 1
    fi
    sleep 1
done

address=$(docker port "$container_id" 2379/tcp)
export SYNOD_TEST_ETCD_ENDPOINT="$address"
if [ "$#" -eq 0 ]; then
    set -- ./scripts/test-postgres.sh
fi
"$@"
