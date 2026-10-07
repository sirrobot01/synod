#!/usr/bin/env python3
"""Install release binaries in disposable directories on server and odin."""

import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import uuid

sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location("m1", Path(__file__).with_name("proof-m1.py"))
m1 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m1)

VERIFY = r'''
import hashlib, json, pathlib, subprocess, sys
root = pathlib.Path(sys.argv[1])
release = root / 'release'
version = json.loads((release / 'release.json').read_text())['version']
command = ['sh', str(release / 'install.sh'), '--version', version, '--from-dir', str(release), '--install-dir', str(root / 'bin')]
result = subprocess.run(command, capture_output=True)
assert result.returncode == 0, 'install failed'
binary = root / 'bin/synod'
output = subprocess.check_output([str(binary), '--version'], text=True).strip()
assert version in output and 'linux/' + sys.argv[2] in output, 'wrong build identity'
original = hashlib.sha256(binary.read_bytes()).hexdigest()
inode = binary.stat().st_ino
assert subprocess.run(command, capture_output=True).returncode == 0, 'repeat install failed'
assert binary.stat().st_ino == inode, 'repeat install replaced the binary'
assert binary.stat().st_mode & 0o777 == 0o755, 'wrong binary mode'
asset = release / ('synod-linux-' + sys.argv[2])
with asset.open('ab') as target:
    target.write(b'corrupt-proof')
assert subprocess.run(command, capture_output=True).returncode != 0, 'corrupt release installed'
assert hashlib.sha256(binary.read_bytes()).hexdigest() == original, 'old binary changed'
assert not list((root / 'bin').glob('.synod-install.*')), 'temporary download remains'
print(json.dumps({'version': output, 'architecture': sys.argv[2], 'sha256': original, 'repeat_preserved_inode': True, 'corruption_refused': True}))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    root = "/tmp/synod-install-T22-" + uuid.uuid4().hex
    proof = {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(), "passed": False, "hosts": {}, "cleanup_errors": []}
    created = []
    try:
        for host, (_, _, arch, _) in m1.HOSTS.items():
            m1.remote(host, ["mkdir", "-m", "700", root])
            created.append(host)
            m1.remote(host, ["mkdir", "-m", "700", root + "/release"])
            for name in ("install.sh", "release.json", "SHA256SUMS", "synod-linux-" + arch):
                m1.remote(host, ["python3", "-c", m1.WRITE, root + "/release/" + name, "600"], (args.release / name).read_bytes())
            proof["hosts"][host] = json.loads(m1.remote(host, ["python3", "-c", VERIFY, root, arch]))
        proof["passed"] = True
    finally:
        for host in reversed(created):
            try:
                m1.remote(host, ["python3", "-c", "import shutil,sys;shutil.rmtree(sys.argv[1])", root])
            except Exception:
                proof["cleanup_errors"].append(host)
                proof["passed"] = False
        args.evidence.write_text(json.dumps(proof, indent=2) + "\n")
        print(json.dumps(proof))
    if not proof["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
