#!/bin/sh
# Install a verified Synod release. This does not initialize or start services.
set -eu
umask 077

version=
install_dir=/usr/local/bin
repository=https://github.com/sirrobot01/synod
base_url=$repository/releases/download
from_dir=
upgrade=false
while [ "$#" -gt 0 ]; do
    case "$1" in
        --version|--install-dir|--base-url|--from-dir)
            [ "$#" -ge 2 ] || { echo "Missing value for $1." >&2; exit 1; }
            case "$1" in
                --version) version=$2 ;;
                --install-dir) install_dir=$2 ;;
                --base-url) base_url=$2 ;;
                --from-dir) from_dir=$2 ;;
            esac
            shift 2 ;;
        --upgrade) upgrade=true; shift ;;
        --help|-h)
            cat <<'HELP'
Usage: sh install.sh [--version v0.1.0] [--install-dir /usr/local/bin]
                     [--base-url https://host/releases/download]
                     [--from-dir /path/to/release] [--upgrade]

Without --version, the installer selects the latest GitHub release.
Synod supports Linux amd64 and arm64.
The installer verifies SHA-256 checksums before it installs the binary.
Use --upgrade to replace an existing binary and save it as synod.previous.
Restart the controller and agents after an upgrade. Keep their state directories.
Use --from-dir for a release copied to a device without Internet access.
HELP
            exit 0 ;;
        *) echo "Unknown option. Run sh install.sh --help." >&2; exit 1 ;;
    esac
done
case "$install_dir" in /*) ;; *) echo "Use an absolute --install-dir." >&2; exit 1 ;; esac
[ "$(uname -s)" = Linux ] || { echo "Synod releases support Linux only." >&2; exit 1; }
case "$(uname -m)" in
    x86_64|amd64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) echo "Synod releases support amd64 and arm64 only." >&2; exit 1 ;;
esac
if command -v sha256sum >/dev/null 2>&1; then
    checksum=sha256sum
elif command -v shasum >/dev/null 2>&1; then
    checksum=shasum
else
    echo "Install sha256sum or shasum to verify this release." >&2; exit 1
fi
if [ -z "$from_dir" ]; then
    case "$base_url" in https://*) ;; *) echo "Use an HTTPS --base-url." >&2; exit 1 ;; esac
    command -v curl >/dev/null 2>&1 || { echo "Install curl to download a release." >&2; exit 1; }
else
    case "$from_dir" in /*) ;; *) echo "Use an absolute --from-dir." >&2; exit 1 ;; esac
fi
if [ -z "$version" ]; then
    [ -z "$from_dir" ] && [ "$base_url" = "$repository/releases/download" ] || {
        echo "Use --version with --from-dir or --base-url." >&2; exit 1;
    }
    # GitHub redirects releases/latest to the page of the newest release tag.
    latest=$(curl --fail --silent --show-error --location --head --proto '=https' --proto-redir '=https' --tlsv1.2 \
        --connect-timeout 10 --max-time 60 --output /dev/null --write-out '%{url_effective}' "$repository/releases/latest") || {
        echo "The latest release could not be found. Use --version." >&2; exit 1;
    }
    version=${latest##*/}
fi
case "$version" in
    *[!A-Za-z0-9.-]*) echo "The version contains invalid characters." >&2; exit 1 ;;
esac
printf '%s\n' "$version" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$' || {
    echo "Use --version vMAJOR.MINOR.PATCH, with an optional prerelease suffix." >&2; exit 1;
}
mkdir -p "$install_dir" || { echo "Use a writable --install-dir, or run with sudo." >&2; exit 1; }
lock=$install_dir/.synod-install.lock
mkdir "$lock" 2>/dev/null || { echo "Another installer holds $lock. Wait for it to finish. If it stopped, remove the empty lock directory and retry." >&2; exit 1; }
staging=
trap '[ -z "$staging" ] || rm -rf "$staging"; rmdir "$lock"' EXIT
trap 'exit 1' HUP INT TERM
[ ! -L "$install_dir/synod" ] || { echo "The target is a symbolic link. Select another --install-dir." >&2; exit 1; }
[ ! -e "$install_dir/synod" ] || [ -f "$install_dir/synod" ] || { echo "The target is not a regular file. Select another --install-dir." >&2; exit 1; }
if [ "$upgrade" = true ]; then
    [ -f "$install_dir/synod" ] || { echo "No binary is installed. Omit --upgrade for a fresh install." >&2; exit 1; }
    [ ! -L "$install_dir/synod.previous" ] && { [ ! -e "$install_dir/synod.previous" ] || [ -f "$install_dir/synod.previous" ]; } || {
        echo "synod.previous is not a regular file. Move it before the upgrade." >&2; exit 1;
    }
fi
staging=$(mktemp -d "$install_dir/.synod-install.XXXXXX")
asset=synod-linux-$arch
for file in SHA256SUMS "$asset"; do
    if [ -n "$from_dir" ]; then
        cp "$from_dir/$file" "$staging/$file"
    else
        curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
            --connect-timeout 10 --max-time 300 --retry 2 \
            "${base_url%/}/$version/$file" --output "$staging/$file"
    fi
done
for file in "$asset"; do
    expected=$(awk -v name="$file" '$2 == name { print $1 }' "$staging/SHA256SUMS")
    [ "${#expected}" -eq 64 ] && printf '%s\n' "$expected" | grep -Eq '^[0-9a-fA-F]{64}$' || {
        echo "The release checksum is absent or ambiguous. Download the release again." >&2; exit 1;
    }
    if [ "$checksum" = sha256sum ]; then
        actual=$(sha256sum "$staging/$file" | cut -d ' ' -f 1)
    else
        actual=$(shasum -a 256 "$staging/$file" | cut -d ' ' -f 1)
    fi
    [ "$actual" = "$expected" ] || { echo "The release checksum differs. The installed binary was not changed." >&2; exit 1; }
done
chmod 755 "$staging/$asset"
# The binary reports "synod version VERSION (REVISION; linux/ARCH)". Run it only after verification.
[ "$("$staging/$asset" --version | cut -d ' ' -f 3)" = "$version" ] || {
    echo "The release version differs from --version. Select the matching release." >&2; exit 1;
}
if [ -e "$install_dir/synod" ]; then
    if cmp -s "$staging/$asset" "$install_dir/synod"; then
        echo "Synod $version is already installed at $install_dir/synod."
        exit 0
    fi
    [ "$upgrade" = true ] || { echo "A different binary is installed. Follow the upgrade procedure and use --upgrade." >&2; exit 1; }
    # Save the old inode before replacing it. Running processes keep that inode.
    if ! cmp -s "$install_dir/synod" "$install_dir/synod.previous"; then
        ln "$install_dir/synod" "$staging/previous"
        mv -f "$staging/previous" "$install_dir/synod.previous"
    fi
    mv -f "$staging/$asset" "$install_dir/synod"
    echo "Installed Synod $version at $install_dir/synod. Saved the old binary as synod.previous."
    echo "Restart the Synod controller and agents. Keep their existing configuration and state."
    exit 0
fi
# A hard link publishes the verified file atomically and refuses a concurrent install.
ln "$staging/$asset" "$install_dir/synod" || { echo "The binary could not be installed. Check the target and retry." >&2; exit 1; }
echo "Installed Synod $version at $install_dir/synod."
echo "Read synod server init --help on the controller, or synod agent join --help on a device."
