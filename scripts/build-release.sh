#!/bin/sh
# Build the two Linux releases from the committed web assets.
set -eu
umask 022

version=${1:-}
[ "$#" -ge 1 ] && [ "$#" -le 3 ] || { echo "Usage: scripts/build-release.sh VERSION [OUTPUT_DIR] [--allow-dirty]" >&2; exit 1; }
case "$version" in
    *[!A-Za-z0-9.-]*) echo "The version contains invalid characters." >&2; exit 1 ;;
esac
printf '%s\n' "$version" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$' || {
    echo "Use vMAJOR.MINOR.PATCH, with an optional prerelease suffix." >&2; exit 1;
}
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output=${2:-$root/dist/releases/$version}
case "$output" in /*) ;; *) output=$PWD/$output ;; esac
[ ! -e "$output" ] && [ ! -L "$output" ] || { echo "The output already exists. Select a new directory." >&2; exit 1; }
cd "$root"
revision=$(git rev-parse HEAD)
if [ -n "$(git status --porcelain --untracked-files=normal)" ]; then
    [ "${3:-}" = --allow-dirty ] || { echo "Commit the release source first. Use --allow-dirty only for a local test build." >&2; exit 1; }
    revision=$revision-dirty
elif [ "$#" -eq 3 ] && [ "$3" != --allow-dirty ]; then
    echo "Unknown build option." >&2; exit 1
fi
release_go=go$(awk '$1 == "go" { print $2 }' go.mod)
[ -s internal/web/dist/index.html ] || { echo "Build the web UI before the release." >&2; exit 1; }
parent=$(dirname -- "$output")
mkdir -p "$parent"
staging=$(mktemp -d "$parent/.synod-release.XXXXXX")
trap 'rm -rf "$staging"' EXIT
trap 'exit 1' HUP INT TERM
for arch in amd64 arm64; do
    echo "Build $version for linux/$arch."
    CGO_ENABLED=0 GOOS=linux GOARCH=$arch GOAMD64=v1 GOARM64=v8.0 GOEXPERIMENT= GOFLAGS= GOTOOLCHAIN=$release_go \
        go build -trimpath -buildvcs=false \
        -ldflags="-s -w -buildid= -X github.com/sirrobot01/synod/internal/buildinfo.Version=$version -X github.com/sirrobot01/synod/internal/buildinfo.Revision=$revision" \
        -o "$staging/synod-linux-$arch" ./cmd/synod
done
cp install.sh "$staging/install.sh"
chmod 755 "$staging/install.sh" "$staging/synod-linux-amd64" "$staging/synod-linux-arm64"
printf '{\n  "version": "%s",\n  "revision": "%s",\n  "go": "%s",\n  "platforms": ["linux/amd64", "linux/arm64"]\n}\n' "$version" "$revision" "$release_go" > "$staging/release.json"
(cd "$staging"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum synod-linux-amd64 synod-linux-arm64 install.sh release.json > SHA256SUMS
    else
        shasum -a 256 synod-linux-amd64 synod-linux-arm64 install.sh release.json > SHA256SUMS
    fi
)
mv "$staging" "$output"
echo "Release files: $output"
