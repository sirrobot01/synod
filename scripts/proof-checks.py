#!/usr/bin/env python3
"""Prove checks and isolated drills through the CLI, controller, and agent."""

import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
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
proof_ids = []
passed = False


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
    agent_process = start([binary, "agent", "run", "--config", agent / "agent.json"], "agent.log")
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
    source_id = run(["docker", "ps", "-q", "--filter", "label=com.docker.compose.project=" + name]).stdout.strip()
    source_started = json.loads(run(["docker", "inspect", source_id]).stdout)[0]["State"]["StartedAt"]
    phase = "check repository content"
    checked = cli("destination", "check", "fixture", "--on", node)["operation"]
    assert checked["status"] == "succeeded" and checked["result"]["read"] >= 1
    checks.append("A manual integrity check reads real encrypted content through the agent")
    phase = "drill during controller outage"
    submitted = cli("restore", app, "--to", node, "--drill", "--wait=false")["operation"]
    proof_ids.append(submitted["id"])
    label = "io.synod.restore.operation=" + submitted["id"]
    scratch = wait(lambda: run(["docker", "ps", "-q", "--filter", "label=" + label]).stdout.strip())
    containers = json.loads(run(["docker", "inspect", scratch]).stdout)
    assert all(c["HostConfig"]["NetworkMode"] == "none" and not c["HostConfig"]["PortBindings"] for c in containers)
    assert subprocess.run(["docker", "exec", scratch, "cat", "/config/state"], env=env, capture_output=True, timeout=30, check=True).stdout == original
    controller.terminate()
    controller.wait(timeout=20)
    wait(lambda: not run(["docker", "ps", "-aq", "--filter", "label=" + label]).stdout.strip())
    with sqlite3.connect(str(agent / "enrollment") + ".db") as journal:
        wait(lambda: journal.execute("SELECT state FROM records WHERE id=?", (submitted["id"],)).fetchone() == ("terminal",))
    agent_process.terminate()
    agent_process.wait(timeout=20)
    controller = start(server_args, "controller-restarted.log")
    agent_process = start([binary, "agent", "run", "--config", agent / "agent.json"], "agent-restarted.log")
    wait(lambda: "installation" in cli("installation", "get", okay=False))
    phase = "deliver saved drill after restart"
    result = wait(lambda: (op if (op := cli("operation", "get", submitted["id"])["operation"])["status"] == "succeeded" else None))
    assert result["result"]["snapshot_id"] == snapshot and result["result"]["containers"] == 1
    status = next(a for a in cli("app", "status")["protection"]["apps"] if a["app"]["id"] == app)
    tested_at = status["restore_tested_at"]
    assert status["last_drill"]["id"] == submitted["id"]
    checks.append("An isolated drill completes without the controller and delivers its result after both processes restart")
    phase = "force a drill failure"
    failed = cli("restore", app, "--to", node, "--drill", "--wait=false")["operation"]
    proof_ids.append(failed["id"])
    label = "io.synod.restore.operation=" + failed["id"]
    scratch = wait(lambda: run(["docker", "ps", "-q", "--filter", "label=" + label]).stdout.strip())
    run(["docker", "stop", "--time", "1", scratch])
    result = wait(lambda: (op if (op := cli("operation", "get", failed["id"])["operation"])["status"] == "failed" else None))
    assert result["result"]["failure_code"] == "drill_container_stopped"
    assert not run(["docker", "ps", "-aq", "--filter", "label=" + label]).stdout.strip()
    assert not (agent / "restores" / failed["id"]).exists()
    status = next(a for a in cli("app", "status")["protection"]["apps"] if a["app"]["id"] == app)
    assert status["restore_tested_at"] == tested_at and status["last_drill"]["state"] == "failed"
    assert json.loads(run(["docker", "inspect", source_id]).stdout)[0]["State"]["StartedAt"] == source_started
    checks.append("A forced container stop fails the drill, removes scratch resources, and keeps the prior tested date and source uptime")
    for path in root.glob("*.log"):
        content = path.read_bytes()
        for secret in [original, b"private-file-env", b"private-t15-repository-password"]:
            assert secret not in content
    checks.append("Logs contain no saved file data or repository password")
    passed = True
finally:
    for process in reversed(processes):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    cleanup_errors = []
    for label in ["com.docker.compose.project=" + name, *["io.synod.restore.operation=" + value for value in proof_ids]]:
        ids = run(["docker", "ps", "-aq", "--filter", "label=" + label]).stdout.split()
        if ids and run(["docker", "rm", "-f", *ids], okay=False).returncode:
            cleanup_errors.append("containers")
        ids = run(["docker", "volume", "ls", "-q", "--filter", "label=" + label]).stdout.split()
        if ids and run(["docker", "volume", "rm", *ids], okay=False).returncode:
            cleanup_errors.append("volumes")
    for handle in logs:
        handle.close()
    shutil.rmtree(root)
    evidence = {"passed": passed and not cleanup_errors, "task": "T18", "phase": phase, "checks": checks, "cleanup_errors": cleanup_errors}
    if args.evidence:
        args.evidence.write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence))

assert passed and not cleanup_errors
