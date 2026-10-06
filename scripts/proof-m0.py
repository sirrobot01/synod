#!/usr/bin/env python3
"""Prove init, join, approval, and restart on server and odin.

Get the owner's approval before you run this script.
It writes temporary files and starts temporary processes on both hosts.
It does not install services or create, stop, or copy containers.
"""

import argparse
import hashlib
import json
import pathlib
import shlex
import subprocess
import time
import uuid

HOSTS = {
    "server": ("main@192.168.0.219", False, "amd64"),
    "odin": ("dietpi@192.168.0.10", True, "arm64"),
}

START = """
import pathlib, subprocess, sys
root, role = pathlib.Path(sys.argv[1]), sys.argv[2]
with (root / (role + '.log')).open('ab') as log:
    process = subprocess.Popen(sys.argv[3:], stdin=subprocess.DEVNULL,
                               stdout=log, stderr=log, start_new_session=True)
(root / (role + '.pid')).write_text(str(process.pid))
"""

STOP = """
import os, pathlib, signal, sys, time
root, role = pathlib.Path(sys.argv[1]), sys.argv[2]
marker = root / (role + '.pid')
if marker.exists():
    pid = int(marker.read_text())
    executable = pathlib.Path('/proc') / str(pid) / 'exe'
    if executable.exists() and executable.resolve() == root / 'synod':
        os.kill(pid, signal.SIGTERM)
        for _ in range(50):
            if not executable.exists():
                break
            time.sleep(0.1)
        if executable.exists() and executable.resolve() == root / 'synod':
            os.kill(pid, signal.SIGKILL)
    marker.unlink()
"""


def remote(host, arguments, data=None):
    """Run one command through SSH. Do not expose tokens in a subprocess error."""
    address, privileged, _ = HOSTS[host]
    command = ["sudo", "-n", *arguments] if privileged else arguments
    try:
        result = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
             "-o", "ConnectTimeout=5", address, shlex.join(command)],
            input=data, capture_output=True, timeout=55,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise RuntimeError(f"SSH command did not finish on {host}") from None
    if result.returncode:
        raise RuntimeError(f"Proof command failed on {host}, exit {result.returncode}")
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", required=True, type=pathlib.Path,
                        help="Directory with synod-linux-amd64 and synod-linux-arm64")
    parser.add_argument("--evidence", required=True, type=pathlib.Path,
                        help="New private local directory for the result and daemon logs")
    options = parser.parse_args()
    binaries = {host: options.binaries / f"synod-linux-{arch}" for host, (_, _, arch) in HOSTS.items()}
    for binary in binaries.values():
        if not binary.is_file():
            parser.error(f"Missing binary: {binary}")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    options.evidence.mkdir(mode=0o700)
    root = "/tmp/synod-m0-T03-" + uuid.uuid4().hex
    created = []
    proof = {"revision": revision, "remote_directory": root, "passed": False,
             "binaries": {host: hashlib.sha256(binary.read_bytes()).hexdigest()
                          for host, binary in binaries.items()}}
    controller = [root + "/synod", "server", "--config", root + "/controller/controller.json",
                  "--listen", "0.0.0.0:18443", "--agent-listen", "0.0.0.0:18444"]
    cli = [root + "/synod", "--json", "--api-url", "https://192.168.0.219:18443",
           "--ca-file", root + "/controller/api-ca.crt",
           "--token-file", root + "/controller/admin.token"]
    failure = None
    try:
        listeners = remote("server", ["ss", "-H", "-ltn", "sport = :18443 or sport = :18444"])
        if listeners.strip():
            raise RuntimeError("Ports 18443 and 18444 must be free on server")
        for host, binary in binaries.items():
            remote(host, ["mkdir", "-m", "700", root])
            created.append(host)
            remote(host, ["python3", "-c",
                          "import os,sys; p=sys.argv[1]; f=open(p,'xb'); f.write(sys.stdin.buffer.read()); f.close(); os.chmod(p,0o700)",
                          root + "/synod"], binary.read_bytes())

        remote("server", [root + "/synod", "server", "init", "--dir", root + "/controller",
                          "--address", "192.168.0.219", "--systemd-dir", root + "/no-systemd"])
        remote("server", ["python3", "-c", START, root, "controller", *controller])
        for attempt in range(40):
            try:
                remote("server", [*cli, "installation", "get"])
                break
            except RuntimeError:
                if attempt == 39:
                    raise
                time.sleep(0.25)

        ids = {}
        for host in HOSTS:
            minted = json.loads(remote("server", [*cli, "node", "join-token"]))
            remote(host, [root + "/synod", "agent", "join", minted["join_token"],
                          "--dir", root + "/agent", "--systemd-dir", root + "/no-systemd"])
            ids[host] = remote(host, ["cat", root + "/agent/node-id"]).decode().strip()
            remote(host, ["python3", "-c", START, root, "agent", root + "/synod",
                          "agent", "run", "--config", root + "/agent/agent.json"])

        for attempt in range(40):
            nodes = json.loads(remote("server", [*cli, "node", "list"])).get("nodes", [])
            by_id = {node["id"]: node for node in nodes}
            if all(by_id.get(ids[host], {}).get("architecture") == HOSTS[host][2] for host in HOSTS):
                break
            if attempt == 39:
                raise RuntimeError("Both agents must connect and report their architectures")
            time.sleep(0.5)
        for host in HOSTS:
            approved = json.loads(remote("server", [*cli, "node", "approve", ids[host]]))["node"]
            if not approved["approved_at"]:
                raise RuntimeError(f"Approval was not saved for {host}")

        nodes = json.loads(remote("server", [*cli, "node", "list"]))["nodes"]
        before = {node["id"]: node["connected_at"] for node in nodes}
        remote("server", ["python3", "-c", STOP, root, "controller"])
        remote("server", ["python3", "-c", START, root, "controller", *controller])
        for attempt in range(40):
            try:
                nodes = json.loads(remote("server", [*cli, "node", "list"])).get("nodes", [])
            except RuntimeError:
                nodes = []
            by_id = {node["id"]: node for node in nodes}
            if all(by_id.get(ids[host], {}).get("approved_at") and
                   by_id[ids[host]]["connected_at"] > before[ids[host]] for host in HOSTS):
                break
            if attempt == 39:
                raise RuntimeError("Agents did not reconnect after the controller restart")
            time.sleep(0.5)
        proof["nodes"] = nodes
        proof["passed"] = True
    except (RuntimeError, OSError, ValueError, KeyError) as error:
        failure = str(error)
        proof["failure"] = failure
    finally:
        cleanup_errors = []
        processes = [(host, "agent") for host in reversed(created)]
        if "server" in created:
            processes.append(("server", "controller"))
        for host, role in processes:
            try:
                remote(host, ["python3", "-c", STOP, root, role])
                log = remote(host, ["python3", "-c",
                                   "import pathlib,sys; p=pathlib.Path(sys.argv[1]); sys.stdout.buffer.write(p.read_bytes() if p.exists() else b'')",
                                   root + "/" + role + ".log"])
                (options.evidence / f"{host}-{role}.log").write_bytes(log)
            except RuntimeError as error:
                cleanup_errors.append(str(error))
        if not cleanup_errors:
            for host in created:
                try:
                    remote(host, ["python3", "-c", "import shutil,sys; shutil.rmtree(sys.argv[1])", root])
                except RuntimeError as error:
                    cleanup_errors.append(str(error))
        proof["cleanup_errors"] = cleanup_errors
        (options.evidence / "result.json").write_text(json.dumps(proof, indent=2) + "\n")
    if failure or proof["cleanup_errors"]:
        raise SystemExit(f"M0 proof incomplete. Read {options.evidence / 'result.json'}.")
    print(f"M0 proof passed. Evidence: {options.evidence / 'result.json'}")


if __name__ == "__main__":
    main()
