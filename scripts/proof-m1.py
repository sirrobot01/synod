#!/usr/bin/env python3
"""Back up and restore disposable projects on server and odin.

Get the owner's approval before execution. This proof starts temporary agents,
a controller, MinIO, and one test container on each host. Only the test apps get
protection settings. No services are installed. Cleanup removes the fixtures.
"""

import argparse
import hashlib
import io
import json
import os
import pathlib
import secrets
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import time
import uuid

HOSTS = {
    "server": ("main@192.168.0.219", False, "amd64", "192.168.0.219"),
    "odin": ("dietpi@192.168.0.10", True, "arm64", "192.168.0.10"),
}
IMAGE = "alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"
START = """
import json, os, pathlib, subprocess, sys
root, role = pathlib.Path(sys.argv[1]), sys.argv[2]
env = os.environ.copy()
if role == 'minio':
    env.update(json.loads((root / 'minio-env.json').read_text()))
with (root / (role + '.log')).open('ab') as log:
    process = subprocess.Popen(sys.argv[3:], stdin=subprocess.DEVNULL,
                               stdout=log, stderr=log, env=env, start_new_session=True)
(root / (role + '.pid')).write_text(str(process.pid))
"""
STOP = """
import os, pathlib, signal, sys, time
root, role = pathlib.Path(sys.argv[1]), sys.argv[2]
marker = root / (role + '.pid')
if marker.exists():
    pid = int(marker.read_text())
    executable = pathlib.Path('/proc') / str(pid) / 'exe'
    expected = root / ('minio' if role == 'minio' else 'synod')
    if executable.exists() and executable.resolve() == expected:
        os.kill(pid, signal.SIGTERM)
        for _ in range(600):
            if not executable.exists():
                break
            time.sleep(0.1)
        if executable.exists() and executable.resolve() == expected:
            raise SystemExit('temporary process did not stop')
    marker.unlink()
"""
WRITE = """
import os, sys
with open(sys.argv[1], 'xb') as f:
    os.chmod(f.fileno(), int(sys.argv[2], 8))
    f.write(sys.stdin.buffer.read())
"""


def remote(host, arguments, data=None):
    """Use the host's existing SSH identity. Keep command secrets out of errors."""
    address, privileged, _, _ = HOSTS[host]
    command = ["sudo", "-n", *arguments] if privileged else arguments
    try:
        result = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
             "-o", "ConnectTimeout=5", address, shlex.join(command)],
            input=data, capture_output=True, timeout=180,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise RuntimeError(f"SSH command did not finish on {host}") from None
    if result.returncode:
        raise RuntimeError(f"Proof command failed on {host}, exit {result.returncode}")
    return result.stdout


def wait(check, message, seconds=150):
    """Wait for one bounded, read-only proof condition."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(1)
    raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", required=True, type=pathlib.Path,
                        help="Directory with synod-linux-amd64 and synod-linux-arm64")
    parser.add_argument("--minio", required=True, type=pathlib.Path,
                        help="Pinned MinIO linux/amd64 binary")
    parser.add_argument("--verifier", required=True, type=pathlib.Path,
                        help="Native repository test binary with TestM1HostProof")
    parser.add_argument("--evidence", required=True, type=pathlib.Path,
                        help="New private directory for the result and daemon logs")
    options = parser.parse_args()
    binaries = {host: options.binaries / f"synod-linux-{arch}"
                for host, (_, _, arch, _) in HOSTS.items()}
    for binary in [*binaries.values(), options.minio, options.verifier]:
        if not binary.is_file():
            parser.error(f"Missing binary: {binary}")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    options.evidence.mkdir(mode=0o700)
    local = pathlib.Path(tempfile.mkdtemp(prefix="synod-m1-private-"))
    root = "/tmp/synod-m1-T10-" + uuid.uuid4().hex
    fixture = "synod-m1-" + uuid.uuid4().hex[:20]
    password, s3_secret = secrets.token_hex(24), secrets.token_hex(24)
    project = root + "/project"
    created, containers, ids, baseline = [], {}, {}, {}
    proof = {"revision": revision, "remote_directory": root, "passed": False,
             "checks": [], "copies": [], "binaries": {
                 host: hashlib.sha256(binary.read_bytes()).hexdigest()
                 for host, binary in binaries.items()},
             "minio_sha256": hashlib.sha256(options.minio.read_bytes()).hexdigest()}
    controller = [root + "/synod", "server", "--config", root + "/controller/controller.json",
                  "--listen", "0.0.0.0:18443", "--agent-listen", "0.0.0.0:18444"]
    cli = [root + "/synod", "--json", "--api-url", "https://192.168.0.219:18443",
           "--ca-file", root + "/controller/api-ca.crt",
           "--token-file", root + "/controller/admin.token"]
    spec = {"stage": "prepare", "endpoint": "http://192.168.0.219:19000", "bucket": fixture,
            "credentials": {"password": password, "access_key_id": "synod-m1",
                            "secret_access_key": s3_secret}, "copies": []}
    spec_file = local / "proof.json"
    failure = None

    def request(*arguments):
        return json.loads(remote("server", [*cli, *arguments]))

    def delivery():
        listing = request("destination", "list")["destinations"]
        return len(listing["delivery"]) == 2 and all(d["current"] for d in listing["delivery"])

    def verifier():
        spec_file.write_text(json.dumps(spec))
        spec_file.chmod(0o600)
        env = dict(os.environ, SYNOD_M1_PROOF_SPEC=str(spec_file))
        result = subprocess.run([str(options.verifier.resolve()), "-test.run", "^TestM1HostProof$",
                                 "-test.v", "-test.timeout", "3m"],
                                env=env, capture_output=True, timeout=200)
        log = result.stdout + result.stderr
        if password.encode() in log or s3_secret.encode() in log:
            raise RuntimeError("Verifier output contains a credential")
        (options.evidence / (spec["stage"] + "-restore.log")).write_bytes(log)
        if result.returncode:
            raise RuntimeError("Repository proof failed; read the private verifier log")

    try:
        for host, binary in binaries.items():
            ports = ":18445" if host == "odin" else ":18443 or sport = :18444 or sport = :18445 or sport = :19000 or sport = :19001"
            if remote(host, ["ss", "-H", "-ltn", "sport = " + ports]).strip():
                raise RuntimeError(f"Temporary proof ports are occupied on {host}")
            arch = remote(host, ["docker", "image", "inspect", IMAGE, "--format", "{{.Architecture}}"])
            if arch.decode().strip() != HOSTS[host][2]:
                raise RuntimeError(f"Pinned fixture image is missing or incompatible on {host}")
            before = remote(host, ["docker", "ps", "--no-trunc", "--format", "{{.ID}}"]).decode().split()
            baseline[host] = {}
            for container in before:
                baseline[host][container] = remote(host, ["docker", "inspect", "--format", "{{.State.Running}} {{.State.StartedAt}}", container])
            remote(host, ["mkdir", "-m", "700", root])
            created.append(host)
            remote(host, ["python3", "-c", WRITE, root + "/synod", "700"], binary.read_bytes())
            remote(host, ["mkdir", "-m", "700", project])
            name = fixture + "-" + host
            marker = "private-m1-state-" + secrets.token_hex(16)
            environment = "PRIVATE=private-m1-env-" + secrets.token_hex(16) + "\n"
            command = "trap 'printf %s " + marker + " > /config/state; exit 0' TERM; touch /config/ready; while :; do sleep 0.1; done"
            compose = "services:\n  app:\n    image: " + IMAGE + "\n    network_mode: none\n    stop_grace_period: 2s\n    command: " + json.dumps(["sh", "-c", command]) + "\n    volumes:\n      - " + project + ":/config\n"
            for filename, contents in {"compose.yaml": compose, ".env": environment, "state": "before-stop"}.items():
                remote(host, ["python3", "-c", WRITE, project + "/" + filename, "600"], contents.encode())
            # Save the name first so cleanup also handles a lost create response.
            containers[host] = name
            remote(host, ["docker", "create", "--name", name, "--network", "none", "--stop-timeout", "2",
                          "--label", "synod.proof=" + fixture,
                          "--label", "com.docker.compose.project=" + name,
                          "--label", "com.docker.compose.project.working_dir=" + project,
                          "--label", "com.docker.compose.project.config_files=" + project + "/compose.yaml",
                          "--label", "com.docker.compose.service=app", "--mount",
                          "type=bind,src=" + project + ",dst=/config", IMAGE, "sh", "-c", command])
            remote(host, ["docker", "start", name])
            wait(lambda: remote(host, ["python3", "-c", "import pathlib,sys; print(int(pathlib.Path(sys.argv[1]).exists()))", project + "/ready"]).strip() == b"1", "The test app did not start")
            proof[host] = {"project_name": name, "architecture": HOSTS[host][2]}
            spec[host] = {"state": marker, "environment": environment}

        remote("server", ["python3", "-c", WRITE, root + "/minio", "700"], options.minio.read_bytes())
        remote("server", ["python3", "-c", WRITE, root + "/minio-env.json", "600"],
               json.dumps({"MINIO_ROOT_USER": "synod-m1", "MINIO_ROOT_PASSWORD": s3_secret,
                           "MINIO_BROWSER": "off"}).encode())
        remote("server", ["python3", "-c", START, root, "minio", root + "/minio", "server", root + "/s3",
                          "--address", "0.0.0.0:19000", "--console-address", "127.0.0.1:19001"])
        wait(lambda: remote("server", ["python3", "-c", "import urllib.request\ntry:\n print(urllib.request.urlopen('http://127.0.0.1:19000/minio/health/ready',timeout=1).status)\nexcept OSError:\n print(0)"]).strip() == b"200", "Temporary MinIO did not become ready")
        markers = {host: spec.pop(host) for host in HOSTS}
        verifier()
        remote("server", [root + "/synod", "server", "init", "--dir", root + "/controller",
                          "--address", "192.168.0.219", "--systemd-dir", root + "/no-systemd"])
        remote("server", ["python3", "-c", START, root, "controller", *controller])
        for attempt in range(40):
            try:
                spec["installation_id"] = request("installation", "get")["installation"]["installation_id"]
                break
            except RuntimeError:
                if attempt == 39:
                    raise
                time.sleep(.5)
        for host in HOSTS:
            minted = request("node", "join-token")
            remote(host, [root + "/synod", "agent", "join", minted["join_token"], "--dir", root + "/agent",
                          "--systemd-dir", root + "/no-systemd", "--destination-listen", "0.0.0.0:18445",
                          "--destination-address", HOSTS[host][3] + ":18445"])
            ids[host] = remote(host, ["cat", root + "/agent/node-id"]).decode().strip()
            remote(host, ["python3", "-c", START, root, "agent", root + "/synod", "agent", "run", "--config", root + "/agent/agent.json"])
        wait(lambda: all(any(n["id"] == ids[host] and n["architecture"] == HOSTS[host][2] for n in request("node", "list").get("nodes", [])) for host in HOSTS), "Both device architectures were not reported")
        for host in HOSTS:
            request("node", "approve", ids[host])
        for host in HOSTS:
            app = wait(lambda: next((a for a in request("app", "list").get("apps", []) if a["node_id"] == ids[host] and a["definition"]["name"] == containers[host]), None), "The test project was not discovered")
            proof[host]["app_id"] = app["id"]
        for filename, values in {"peer.json": {"password": password}, "cloud.json": spec["credentials"]}.items():
            remote("server", ["python3", "-c", WRITE, root + "/" + filename, "600"], json.dumps(values).encode())
        peer = request("destination", "add", "m1-peer", "--type", "device", "--device", ids["odin"], "--path", root + "/repository", "--credentials-file", root + "/peer.json")["destination"]["id"]
        cloud = request("destination", "add", "m1-cloud", "--type", "s3", "--endpoint", spec["endpoint"], "--bucket", spec["bucket"], "--region", "us-east-1", "--credentials-file", root + "/cloud.json")["destination"]["id"]
        wait(delivery, "Agents did not save destination settings")
        for host in HOSTS:
            args = ["protect", proof[host]["app_id"], "--destination", peer, "--destination", cloud, "--method", "auto"]
            preview = request(*args)
            request(*args, "--confirm", str(preview["protection"]["revision"]))
        wait(delivery, "Agents did not save protection settings")
        print("Both test projects are protected. Waiting for four snapshots.", flush=True)
        for host in HOSTS:
            for kind, target in [("device", peer), ("s3", cloud)]:
                def completed():
                    for op in request("operation", "list").get("operations", []):
                        if op["app_id"] == proof[host]["app_id"] and op["status"] in ["succeeded", "failed"]:
                            full = request("operation", "get", op["id"])["operation"]
                            if full.get("result", {}).get("destination_id") == target:
                                return full
                    return None
                op = wait(completed, f"No backup result for {host} to {kind}", seconds=240)
                if op["status"] != "succeeded" or op["result"].get("warning_codes"):
                    raise RuntimeError(f"Backup for {host} to {kind} failed or reported a warning: " + op["failure_code"])
                spec["copies"].append({"snapshot_id": op["result"]["snapshot_id"], "app_id": op["app_id"],
                                       "destination_id": target, "kind": kind, "architecture": HOSTS[host][2],
                                       "project": project, **markers[host]})
                proof["copies"].append({"host": host, "kind": kind, "snapshot_id": op["result"]["snapshot_id"],
                                        "downtime_ms": op["result"]["downtime_ms"]})
        for host in HOSTS:
            if remote(host, ["docker", "inspect", "--format", "{{.State.Running}}", containers[host]]).strip() != b"true":
                raise RuntimeError(f"The backup left the test container stopped on {host}")
        proof["checks"].append("Stop backups from amd64 and arm64 succeeded at both destinations and restarted the test apps")
        archive = remote("odin", ["tar", "-C", root + "/repository", "-cf", "-", "."])
        copied = local / "repository"
        copied.mkdir(mode=0o700)
        with tarfile.open(fileobj=io.BytesIO(archive)) as reader:
            reader.extractall(copied, filter="data")
        spec["repository_path"], spec["stage"] = str(copied), "verify"
        verifier()
        proof["checks"].append("All four snapshots restored Compose files, private definitions, environment files, and final stopped data")
        proof["checks"].append("Both encrypted repositories kept recovery settings and contain no plaintext private markers")
        for host in HOSTS:
            for container, before in baseline[host].items():
                if remote(host, ["docker", "inspect", "--format", "{{.State.Running}} {{.State.StartedAt}}", container]) != before:
                    raise RuntimeError(f"A pre-existing container changed state on {host}; no corrective action was taken")
        proof["checks"].append("Pre-existing running containers kept their start times and remained running")
        proof["passed"] = True
    except (RuntimeError, OSError, ValueError, KeyError, subprocess.TimeoutExpired, tarfile.TarError) as error:
        failure = str(error)
        proof["failure"] = failure
    finally:
        cleanup = []
        if ids:
            try:
                # The installation is temporary. Only its two fixture apps were enabled.
                preview = request("protect", "--all", "--off")
                request("protect", "--all", "--off", "--confirm", str(preview["protection"]["revision"]))
            except (RuntimeError, ValueError, KeyError):
                cleanup.append("Temporary protection shutdown was not confirmed")
        for host in reversed(created):
            try:
                remote(host, ["python3", "-c", STOP, root, "agent"])
            except RuntimeError as error:
                cleanup.append(str(error))
        for host, name in containers.items():
            try:
                identity = remote(host, ["docker", "ps", "-aq", "--filter", "name=^/" + name + "$", "--filter", "label=synod.proof=" + fixture]).strip()
                if identity:
                    remote(host, ["docker", "rm", "--force", name])
            except RuntimeError as error:
                cleanup.append(str(error))
        if "server" in created:
            for role in ["controller", "minio"]:
                try:
                    remote("server", ["python3", "-c", STOP, root, role])
                except RuntimeError as error:
                    cleanup.append(str(error))
        for host in created:
            for role in (["agent", "controller", "minio"] if host == "server" else ["agent"]):
                try:
                    log = remote(host, ["python3", "-c", "import pathlib,sys; p=pathlib.Path(sys.argv[1]); sys.stdout.buffer.write(p.read_bytes() if p.exists() else b'')", root + "/" + role + ".log"])
                    if password.encode() in log or s3_secret.encode() in log:
                        cleanup.append(f"Credential found in {host} {role} log; log was not copied")
                    else:
                        (options.evidence / f"{host}-{role}.log").write_bytes(log)
                except RuntimeError as error:
                    cleanup.append(str(error))
        if not cleanup:
            for host in created:
                try:
                    remote(host, ["python3", "-c", "import shutil,sys; shutil.rmtree(sys.argv[1])", root])
                except RuntimeError as error:
                    cleanup.append(str(error))
        shutil.rmtree(local)
        proof["cleanup_errors"] = cleanup
        (options.evidence / "result.json").write_text(json.dumps(proof, indent=2) + "\n")
    if failure or proof["cleanup_errors"]:
        raise SystemExit(f"M1 proof incomplete. Read {options.evidence / 'result.json'}.")
    print(f"M1 proof passed. Evidence: {options.evidence / 'result.json'}")


if __name__ == "__main__":
    main()
