#!/bin/sh
# Build the pinned MinIO test server. Run a command with a temporary S3 endpoint.
set -eu

if [ -n "${SYNOD_TEST_DOCKER_HOST:-}" ]; then
    export DOCKER_HOST="$SYNOD_TEST_DOCKER_HOST"
fi
fixture_arch=$(docker info --format '{{.Architecture}}')
case "$fixture_arch" in
    aarch64|arm64) fixture_arch=arm64 ;;
    x86_64|amd64) fixture_arch=amd64 ;;
    *) echo 'The test daemon must use amd64 or arm64.' >&2; exit 1 ;;
esac
fixture_image="synod-test-minio:9e49d5e7a648-$fixture_arch"
fixture_dir=$(mktemp -d)
fixture_container=''
cleanup() {
    if [ -n "$fixture_container" ]; then docker rm -f "$fixture_container" >/dev/null; fi
    rm -rf "$fixture_dir"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

if ! docker image inspect "$fixture_image" >/dev/null 2>&1; then
    go mod download -json github.com/minio/minio@v0.0.0-20251015172955-9e49d5e7a648 > "$fixture_dir/module.json"
    fixture_source=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["Dir"])' "$fixture_dir/module.json")
    (cd "$fixture_source" && GOMAXPROCS=4 CGO_ENABLED=0 GOOS=linux GOARCH="$fixture_arch" go build -p 2 -mod=readonly -o "$fixture_dir/minio" .)
    cat > "$fixture_dir/Dockerfile" <<'DOCKERFILE'
FROM scratch
COPY minio /minio
ENTRYPOINT ["/minio"]
DOCKERFILE
    docker build -q -t "$fixture_image" "$fixture_dir" >/dev/null
fi

SYNOD_TEST_S3_ACCESS_KEY=synod-test
SYNOD_TEST_S3_SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
export SYNOD_TEST_S3_ACCESS_KEY SYNOD_TEST_S3_SECRET_KEY
fixture_container=$(docker run --rm -d -p 127.0.0.1::9000 \
    -e "MINIO_ROOT_USER=$SYNOD_TEST_S3_ACCESS_KEY" \
    -e "MINIO_ROOT_PASSWORD=$SYNOD_TEST_S3_SECRET_KEY" \
    "$fixture_image" server /data --address :9000)
fixture_port=$(docker port "$fixture_container" 9000/tcp)
SYNOD_TEST_S3_ENDPOINT="http://$fixture_port"
export SYNOD_TEST_S3_ENDPOINT
python3 - "$SYNOD_TEST_S3_ENDPOINT" <<'PY'
import sys,time,urllib.request
for attempt in range(100):
    try:
        with urllib.request.urlopen(sys.argv[1]+'/minio/health/ready',timeout=1) as response:
            if response.status==200: break
    except OSError: pass
    time.sleep(.1)
else: raise SystemExit('The temporary MinIO server did not become ready.')
PY
"$@"
