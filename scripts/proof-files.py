#!/usr/bin/env python3
"""Prove file restore with disposable local processes and one Docker project."""

import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tarfile
import tempfile
import time
import uuid

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("binary", type=Path)
parser.add_argument("--evidence", type=Path)
args = parser.parse_args()
binary = args.binary.resolve()
docker_host = os.environ["SYNOD_TEST_DOCKER_HOST"]
if not docker_host.startswith("unix://"):
    parser.error("SYNOD_TEST_DOCKER_HOST must name a local Unix socket")
env = {key: value for key, value in os.environ.items() if not key.startswith("SYNOD_")}
env["DOCKER_HOST"] = docker_host
root = Path(tempfile.mkdtemp(prefix="synod-file-proof-")).resolve()
name = "synod-file-proof-" + uuid.uuid4().hex[:12]
processes, logs, checks = [], [], []
phase = "init"


def run(command, okay=True):
    result = subprocess.run(
        [str(value) for value in command], env=env, capture_output=True,
        text=True, timeout=120,
    )
    if okay and result.returncode:
        raise RuntimeError("Command failed during " + phase)
    return result


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def start(command, log):
    handle = open(root / log, "wb")
    logs.append(handle)
    process = subprocess.Popen(
        [str(value) for value in command], env=env, stdout=handle, stderr=handle,
    )
    processes.append(process)
    return process


def wait(check):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.5)
    raise RuntimeError("Timeout during " + phase)


def cli(*command, okay=True):
    result = run([binary, "--json", "--attempts", "1", *command], okay)
    return json.loads(result.stdout)


try:
    ctl, agent, project = root / "controller", root / "agent", root / "project"
    project.mkdir()
    original = b"private-file-restore-proof\x00\xff"
    (project / "state").write_bytes(original)
    (project / ".env").write_text("SECRET=private-file-env\n")
    (project / "empty").touch()
    (project / "compose.yaml").write_text(
        "services:\n  app:\n"
        "    image: alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8\n"
        "    command: [sleep, '3600']\n    network_mode: none\n"
        "    volumes: ['./:/config']\n"
    )
    run([binary, "server", "init", "--dir", ctl, "--address", "127.0.0.1",
         "--systemd-dir", root / "absent"])
    api_port, stream_port = port(), port()
    env.update(
        SYNOD_API_URL=f"https://127.0.0.1:{api_port}",
        SYNOD_CA_FILE=str(ctl / "api-ca.crt"),
        SYNOD_ADMIN_TOKEN_FILE=str(ctl / "admin.token"),
    )
    server_args = [binary, "server", "--config", ctl / "controller.json",
                   "--listen", f"127.0.0.1:{api_port}",
                   "--agent-listen", f"127.0.0.1:{stream_port}"]
    controller = start(server_args, "controller.log")
    phase = "wait for controller"
    wait(lambda: "installation" in cli("installation", "get", okay=False))
    phase = "join"
    token = cli("node", "join-token")["join_token"]
    run([binary, "agent", "join", token, "--dir", agent, "--docker-socket",
         docker_host.removeprefix("unix://"), "--destination-listen", f"127.0.0.1:{port()}",
         "--systemd-dir", root / "absent"])
    run(["docker", "compose", "-p", name, "-f", project / "compose.yaml", "up", "-d"])
    start([binary, "agent", "run", "--config", agent / "agent.json"], "agent.log")
    phase = "approve agent"
    node = wait(lambda: next((n for n in cli("node", "list").get("nodes", [])
                             if n.get("architecture")), None))["id"]
    cli("node", "approve", node)
    phase = "discover fixture"
    app = wait(lambda: next((a for a in cli("app", "list").get("apps", [])
                            if a["definition"]["name"] == name), None))["id"]
    credentials = root / "repository.json"
    credentials.write_text(json.dumps({"password": "private-t15-repository-password"}))
    credentials.chmod(0o600)
    cli("destination", "add", "fixture", "--device", node, "--path",
        root / "repository", "--credentials-file", credentials)
    phase = "deliver destination"
    wait(lambda: any(d["current"] for d in cli("destination", "list")["destinations"]["delivery"]))
    phase = "backup fixture"
    operation = cli("backup", app, "--destination", "fixture", "--method", "stop")["operation"]
    assert operation["status"] == "succeeded"
    snapshot = operation["result"]["snapshot_id"]
    phase = "browse snapshot"
    assert cli("snapshot", "list", app)["files"]["snapshots"][0]["id"] == snapshot
    entries = cli("file", "list", app, project, "--snapshot", snapshot)["files"]["entries"]
    assert {e["name"] for e in entries} >= {"state", ".env", "compose.yaml", "empty"}
    checks.append("CLI lists a real backup and saved paths through the controller and mTLS agent stream")
    (project / "state").write_bytes(b"changed-after-backup")
    phase = "download saved file"
    output = root / "download"
    cli("file", "get", app, project / "state", "--snapshot", snapshot, "--output", output)
    assert output.read_bytes() == original and output.stat().st_mode & 0o777 == 0o600
    cli("file", "get", app, project / "empty", "--output", root / "empty-download")
    assert (root / "empty-download").read_bytes() == b""
    checks.append("Binary and empty files match the snapshot after source changes; output mode is 0600")
    phase = "download directory"
    archive = root / "download.tar"
    cli("file", "get", app, project, "--output", archive)
    with tarfile.open(archive) as tar:
        assert tar.extractfile("project/state").read() == original
        assert tar.extractfile("project/.env").read() == b"SECRET=private-file-env\n"
    checks.append("Directory tar contains saved data and the environment file")
    phase = "refuse existing output and missing paths"
    assert cli("file", "get", app, project / "state", "--output", output, okay=False).get("error")
    missing = root / "missing-output"
    assert cli("file", "get", app, project / "missing", "--output", missing, okay=False).get("error")
    assert not missing.exists() and output.read_bytes() == original
    checks.append("Existing output is preserved; a missing path leaves no output")
    phase = "controller restart"
    controller.terminate()
    controller.wait(timeout=20)
    start(server_args, "controller-restarted.log")
    wait(lambda: "installation" in cli("installation", "get", okay=False))
    wait(lambda: any(d["current"] for d in cli("destination", "list")["destinations"]["delivery"]))
    assert cli("snapshot", "list", app)["files"]["snapshots"][0]["id"] == snapshot
    checks.append("Browsing resumes after controller restart and agent reconnection")
    for path in root.glob("*.log"):
        content = path.read_bytes()
        for secret in [original, b"private-file-env", b"private-t15-repository-password"]:
            assert secret not in content
    checks.append("Controller and agent logs contain no file content or repository password")
finally:
    cleanup = run(["docker", "compose", "-p", name, "-f", root / "project/compose.yaml",
                   "down", "--volumes"], okay=False)
    for process in reversed(processes):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    for handle in logs:
        handle.close()
    shutil.rmtree(root)

assert cleanup.returncode == 0, "Remove the disposable Compose project failed"
evidence = {"passed": True, "task": "T15", "checks": checks, "cleanup_errors": []}
if args.evidence:
    args.evidence.write_text(json.dumps(evidence, indent=2) + "\n")
print(json.dumps(evidence))
