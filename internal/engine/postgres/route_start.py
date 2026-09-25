"""Restore the fixed pooler or router files and run the native process as postgres."""

import http.client
import json
import os
import pwd
from pathlib import Path
import signal
import ssl
import stat
import subprocess
import sys

KINDS = {
    "synod-pooler": {
        "source": Path("/etc/synod-pooler"),
        "private": Path("/run/synod-pooler"),
        "files": {"pgbouncer.ini", "userlist.txt", "ca.crt", "member.crt", "member.key", "role.json"},
        "exec": ["/usr/bin/pgbouncer", "/run/synod-pooler/pgbouncer.ini"],
    },
    "synod-router": {
        "source": Path("/etc/synod-router"),
        "private": Path("/run/synod-router"),
        "files": {"haproxy.cfg", "ca.crt", "check.pem"},
        "exec": ["/usr/sbin/haproxy", "-W", "-db", "-f", "/run/synod-router/haproxy.cfg"],
    },
}


# ROLE_INTERVAL is how often the pooler reads its member's role.
ROLE_INTERVAL = 2


def main():
    """Restore the protected files, drop to postgres, and return the kind to run."""
    os.umask(0o077)
    if os.geteuid() != 0 or len(sys.argv) != 2 or sys.argv[1] not in KINDS:
        raise ValueError("invocation")
    kind = KINDS[sys.argv[1]]
    descriptor = os.open(kind["source"], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        if info.st_uid != 0 or info.st_gid != 0 or stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError("source directory")
        if set(os.listdir(descriptor)) != kind["files"]:
            raise ValueError("source file set")
        material = {}
        for name in sorted(kind["files"]):
            source = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
            with os.fdopen(source, "rb") as file:
                info = os.fstat(file.fileno())
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_gid != 0
                        or stat.S_IMODE(info.st_mode) not in (0o400, 0o600) or info.st_nlink != 1
                        or not 0 < info.st_size <= 131072):
                    raise ValueError("source file")
                data = file.read(131073)
                if len(data) != info.st_size:
                    raise ValueError("source size")
                material[name] = data
    finally:
        os.close(descriptor)
    user = pwd.getpwnam("postgres")
    private = kind["private"]
    private.mkdir(mode=0o700, exist_ok=True)
    info = private.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid not in (0, user.pw_uid) or any(private.iterdir()):
        raise ValueError("private directory")
    os.chown(private, user.pw_uid, user.pw_gid)
    os.chmod(private, 0o700)
    for name, data in material.items():
        target = os.open(private / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(target, "wb") as file:
            file.write(data)
        os.chown(private / name, user.pw_uid, user.pw_gid)
    os.setgroups([])
    os.setgid(user.pw_gid)
    os.setuid(user.pw_uid)
    return kind


def member_role(context, address, port):
    """Return True for the primary, False for another role, or None when the answer is unclear."""
    try:
        connection = http.client.HTTPSConnection(address, port, timeout=ROLE_INTERVAL, context=context)
        try:
            connection.request("GET", "/primary")
            status = connection.getresponse().status
        finally:
            connection.close()
    except (OSError, http.client.HTTPException):
        return None
    return {200: True, 503: False}.get(status)


def supervise(kind):
    """Run PgBouncer and start it again when the member's role changes.

    PgBouncer gives new clients the startup parameters of the pool's first server login, so
    in_hot_standby stays at the value from before a promotion or a demotion. A new PgBouncer
    process has an empty pool. A stop signal goes to PgBouncer, and the exit status is PgBouncer's.
    """
    private = kind["private"]
    settings = json.loads((private / "role.json").read_text())
    address, port = settings["address"], settings["rest_port"]
    context = ssl.create_default_context(cafile=str(private / "ca.crt"))
    context.load_cert_chain(str(private / "member.crt"), str(private / "member.key"))
    stopping = []
    process = [subprocess.Popen(kind["exec"])]

    def forward(signum, _frame):
        stopping.append(signum)
        process[0].send_signal(signum)

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT):
        signal.signal(signum, forward)
    observed = member_role(context, address, port)
    while True:
        try:
            return process[0].wait(timeout=ROLE_INTERVAL)
        except subprocess.TimeoutExpired:
            pass
        if stopping:
            continue
        current = member_role(context, address, port)
        if current is None:
            continue
        if observed is None or current == observed:
            observed = current
            continue
        observed = current
        print("Synod pooler: member role changed; restarting PgBouncer", file=sys.stderr, flush=True)
        process[0].send_signal(signal.SIGQUIT)
        process[0].wait()
        process[0] = subprocess.Popen(kind["exec"])
        if stopping:
            process[0].send_signal(stopping[0])


if __name__ == "__main__":
    try:
        chosen = main()
    except Exception:
        print("Synod route startup: protected configuration refused", file=sys.stderr)
        sys.exit(1)
    if "role.json" not in chosen["files"]:
        os.execv(chosen["exec"][0], chosen["exec"])
    sys.exit(supervise(chosen))
