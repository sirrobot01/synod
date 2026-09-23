"""Fixed storage guards for Synod's pinned Patroni member image."""

import argparse
import fcntl
import ipaddress
import json
import os
from pathlib import Path, PurePosixPath
import pwd
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import uuid

from psycopg2.extensions import make_dsn, parse_dsn

PRIVATE = Path("/run/synod-patroni")
ROOT = Path("/var/lib/postgresql/data")
DATA = ROOT / "patroni"
STAGE = ROOT / ".synod-patroni-stage"
OUTSIDE = Path("/var/lib/synod-patroni")
IDENTITY = ROOT / ".synod-patroni-identity.json"
COMPLETE = ROOT / ".synod-patroni-complete.json"


class Refused(Exception):
    pass


def interrupted(signum, frame):
    raise Refused("native work interrupted")


def native(arguments, timeout):
    process = subprocess.Popen(arguments, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               start_new_session=True)
    try:
        result = process.wait(timeout=timeout)
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        raise
    if result != 0:
        raise Refused("native tool refused the operation")


def directory(path):
    if not stat.S_ISDIR(path.lstat().st_mode):
        raise Refused("directory identity refused")


def record(path):
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    with os.fdopen(descriptor, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
            raise Refused("private record refused")
        data = source.read(65537)
        if len(data) > 65536:
            raise Refused("record size refused")
        return json.loads(data)


def persist(path, value):
    # Exclusive creation makes an uncertain prior write a refusal, never a retry.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as target:
        json.dump(value, target, sort_keys=True, separators=(",", ":"))
        target.flush()
        os.fsync(target.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def system_id(path):
    directory(path)
    directory(path / "global")
    for name in ("PG_VERSION", "global/pg_control"):
        if not stat.S_ISREG((path / name).lstat().st_mode):
            raise Refused("native control file refused")
    if (path / "PG_VERSION").read_text().strip() != "17":
        raise Refused("native version refused")
    result = subprocess.run(
        ["/usr/local/bin/pg_controldata", str(path)],
        env={**os.environ, "LC_ALL": "C"}, capture_output=True, check=True, timeout=15,
    )
    for line in result.stdout.decode("ascii").splitlines():
        if line.startswith("Database system identifier:"):
            value = line.split(":", 1)[1].strip()
            if value.isascii() and value.isdigit() and 0 < int(value) < 2**64:
                return value
    raise Refused("native system identity missing")


def main():
    os.umask(0o077)
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("run", "init", "clone"))
    parser.add_argument("--scope")
    parser.add_argument("--datadir")
    parser.add_argument("--role")
    parser.add_argument("--connstring")
    parser.add_argument("--keep_data")
    args = parser.parse_args()
    if os.geteuid() == 0:
        if args.action != "run":
            raise Refused("native hook must run as postgres")
        owner = pwd.getpwnam("postgres")
        directory(ROOT)
        directory(PRIVATE)
        OUTSIDE.mkdir(mode=0o700, exist_ok=True)
        directory(OUTSIDE)
        for path in (ROOT, PRIVATE, OUTSIDE):
            os.chmod(path, 0o700)
            os.chown(path, owner.pw_uid, owner.pw_gid)
        for path in PRIVATE.iterdir():
            if not stat.S_ISREG(path.lstat().st_mode):
                raise Refused("private file refused")
            os.chown(path, owner.pw_uid, owner.pw_gid)
            os.chmod(path, 0o600)
        os.execv("/usr/local/bin/gosu", ["gosu", "postgres", "/usr/bin/python3",
                                       str(PRIVATE / "guard.py"), *sys.argv[1:]])
    if os.geteuid() != pwd.getpwnam("postgres").pw_uid:
        raise Refused("native owner refused")
    for path in (ROOT, PRIVATE, OUTSIDE):
        directory(path)
    config = record(PRIVATE / "config.json")
    settings = config["synod"]
    identity = {key: settings[key] for key in (
        "installation_id", "cluster_id", "resource_id", "volume_id", "provision_id"
    )}
    for value in identity.values():
        parsed = uuid.UUID(value)
        if str(parsed) != value or parsed.int == 0:
            raise Refused("provision identity refused")
    identity["format"] = 1
    required = settings["required_free_bytes"]
    if type(required) is not int or not 0 < required < 2**63:
        raise Refused("initial storage allowance refused")
    if (config["scope"] != identity["cluster_id"] or config["name"] != identity["resource_id"]
            or config["namespace"] != "/synod-patroni/" + identity["installation_id"] + "/"
            or config["postgresql"]["data_dir"] != str(DATA)):
        raise Refused("configuration identity refused")
    if args.action == "run":
        descriptor = os.open(OUTSIDE / "member.lock", os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise Refused("native member lock refused")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.set_inheritable(descriptor, True)
    outside = record(OUTSIDE / "identity.json")
    inside = record(IDENTITY)
    if outside is None and inside is None and args.action == "run":
        if settings["allow_initialization"] is not True or any(ROOT.iterdir()):
            raise Refused("initialization not authorized for empty storage")
        if any(path.name != "member.lock" for path in OUTSIDE.iterdir()):
            raise Refused("outside intent already exists")
        persist(OUTSIDE / "identity.json", identity)
        persist(IDENTITY, identity)
        outside = inside = identity
    if outside != identity or inside != identity:
        raise Refused("storage identity mismatch")
    completed = record(OUTSIDE / "complete.json")
    volume_complete = record(COMPLETE)
    attempt = record(OUTSIDE / "attempt.json")
    if args.action == "run":
        if completed is not None:
            if completed != volume_complete or completed.get("identity") != identity:
                raise Refused("completion identity mismatch")
            if attempt is None or attempt.get("identity") != identity or attempt.get("method") not in ("init", "clone"):
                raise Refused("initialization attempt identity mismatch")
            if completed.get("system_id") != system_id(DATA):
                raise Refused("native system identity mismatch")
        elif volume_complete is not None or attempt is not None or STAGE.exists() or STAGE.is_symlink() or DATA.exists() or DATA.is_symlink():
            raise Refused("incomplete native attempt requires explicit recovery")
        elif settings["allow_initialization"] is not True:
            raise Refused("native initialization is disabled")
        if completed is None:
            space = os.statvfs(ROOT)
            if space.f_bavail * space.f_frsize < required:
                raise Refused("insufficient space for initial native work")
        os.execv("/opt/patroni/bin/patroni", ["patroni", str(PRIVATE / "config.json")])
    if completed is not None or volume_complete is not None or attempt is not None:
        raise Refused("native initialization already attempted")
    if settings["allow_initialization"] is not True:
        raise Refused("native initialization is disabled")
    space = os.statvfs(ROOT)
    if space.f_bavail * space.f_frsize < required:
        raise Refused("insufficient space for initial native work")
    if STAGE.exists() or STAGE.is_symlink():
        raise Refused("staged native data already exists")
    if DATA.exists() or DATA.is_symlink():
        directory(DATA)
        if any(DATA.iterdir()):
            raise Refused("existing native data refused")
        DATA.rmdir()
    if args.action == "clone":
        if args.scope != identity["cluster_id"] or args.datadir != str(DATA) or args.role != "replica" or args.keep_data != "True":
            raise Refused("native clone parameters refused")
        source = parse_dsn(args.connstring or "")
        if (source.get("user") != "synod_repl" or source.get("sslmode") != "verify-full"
                or source.get("sslrootcert") != str(PRIVATE / "ca.crt") or source.get("password")
                or os.environ.get("PGPASSFILE") != str(PRIVATE / "pgpass")):
            raise Refused("native clone authentication refused")
        address = ipaddress.ip_address(source["host"])
        if not any(address in ipaddress.ip_network(network) for network in settings["peer_networks"]):
            raise Refused("native clone source refused")
        port = int(source["port"])
        if not 0 < port < 65536:
            raise Refused("native clone port refused")
        connstring = make_dsn(host=str(address), port=port, user="synod_repl", dbname="postgres",
                              sslmode="verify-full", sslrootcert=str(PRIVATE / "ca.crt"), connect_timeout=5)
    persist(OUTSIDE / "attempt.json", {"identity": identity, "method": args.action})
    STAGE.mkdir(mode=0o700)
    staged_data = STAGE / "data"
    if args.action == "init":
        password_file = PRIVATE / "initdb-password"
        descriptor = os.open(password_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "w") as target:
            target.write(config["postgresql"]["authentication"]["superuser"]["password"] + "\n")
        try:
            native(["/usr/local/bin/initdb", "-D", str(staged_data), "-U", "synod_admin",
                    "--pwfile=" + str(password_file), "--encoding=UTF8", "--data-checksums",
                    "--auth-host=scram-sha-256", "--auth-local=trust"], 120)
        finally:
            password_file.unlink()
    else:
        archives = STAGE / "archives"
        native(["/usr/local/bin/pg_basebackup", "--dbname=" + connstring, "-D", str(archives),
                "--format=tar", "--wal-method=stream", "--checkpoint=fast", "--no-password",
                "--manifest-checksums=SHA256", "--no-clean"], 300)
        if {path.name for path in archives.iterdir()} != {"base.tar", "pg_wal.tar", "backup_manifest"}:
            raise Refused("external tablespaces or unknown backup files refused")
        staged_data.mkdir(mode=0o700)
        for name in ("base.tar", "pg_wal.tar"):
            archive_path = archives / name
            if not stat.S_ISREG(archive_path.lstat().st_mode):
                raise Refused("backup archive identity refused")
            destination = staged_data if name == "base.tar" else staged_data / "pg_wal"
            directory(destination)
            with tarfile.open(archive_path, mode="r|") as archive:
                for member in archive:
                    path = PurePosixPath(member.name)
                    if path.is_absolute() or ".." in path.parts or not (member.isdir() or member.isfile()):
                        raise Refused("backup archive path or type refused")
                    target = destination / member.name
                    if member.isfile() and (target.exists() or target.is_symlink()):
                        raise Refused("duplicate backup file refused")
                    archive.extract(member, destination, filter="data")
        manifest = archives / "backup_manifest"
        if not stat.S_ISREG(manifest.lstat().st_mode):
            raise Refused("backup manifest identity refused")
        shutil.copyfile(manifest, staged_data / "backup_manifest")
        native(["/usr/local/bin/pg_verifybackup", str(staged_data)], 120)
        native(["/usr/local/bin/initdb", "--sync-only", "-D", str(staged_data)], 120)
    completed = {"identity": identity, "system_id": system_id(staged_data)}
    os.rename(staged_data, DATA)
    descriptor = os.open(ROOT, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    persist(COMPLETE, completed)
    persist(OUTSIDE / "complete.json", completed)


if __name__ == "__main__":
    try:
        main()
    except Refused as error:
        print("Synod Patroni storage guard: " + str(error), file=sys.stderr)
        sys.exit(1)
    except Exception:
        # Native tool output and configuration can contain secret material.
        print("Synod Patroni storage guard: native initialization or storage check failed", file=sys.stderr)
        sys.exit(1)
