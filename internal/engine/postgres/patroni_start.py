"""Restore the fixed Patroni runtime files without contacting a Synod service."""

import os
from pathlib import Path
import stat
import sys

SOURCE = Path("/etc/synod-patroni")
PRIVATE = Path("/run/synod-patroni")
FILES = {"config.json", "ca.crt", "member.crt", "member.key", "dcs-ca.crt", "dcs-client.crt", "dcs-client.key"}


def main():
    os.umask(0o077)
    if os.geteuid() != 0 or len(sys.argv) != 4 or sys.argv[1] != "synod-patroni":
        raise ValueError("invocation")
    descriptor = os.open(SOURCE, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        if info.st_uid != 0 or info.st_gid != 0 or stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError("source directory")
        if set(os.listdir(descriptor)) != FILES:
            raise ValueError("source file set")
        material = {}
        for name in sorted(FILES):
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
    PRIVATE.mkdir(mode=0o700, exist_ok=True)
    info = PRIVATE.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or any(PRIVATE.iterdir()):
        raise ValueError("private directory")
    os.chmod(PRIVATE, 0o700)
    material["guard.py"] = sys.argv[2].encode("utf-8")
    material["application.py"] = sys.argv[3].encode("utf-8")
    for name, data in material.items():
        descriptor = os.open(PRIVATE / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as target:
            target.write(data)
    os.execv("/usr/bin/python3", ["python3", str(PRIVATE / "guard.py"), "run"])


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("Synod Patroni startup: protected configuration refused", file=sys.stderr)
        sys.exit(1)
