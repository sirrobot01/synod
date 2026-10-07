#!/usr/bin/env python3
"""Prove isolated database drills and Shoutrrr alerts on server and odin.

Use disposable projects only. Get host approval before execution.
The receiver stays on server's loopback interface. No external account receives alerts.
"""

import argparse
import hashlib
import json
from pathlib import Path
import runpy
import secrets
import subprocess
import uuid

shared = runpy.run_path(str(Path(__file__).with_name("proof-m1.py")))
remote, wait = shared["remote"], shared["wait"]
HOSTS, START, STOP, WRITE = (shared[key] for key in ("HOSTS", "START", "STOP", "WRITE"))
IMAGE = "postgres@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0"
RECEIVER = r"""
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import json, os, sys
root = Path(sys.argv[1])
os.umask(0o077)
class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        size = int(self.headers.get('Content-Length', '0'))
        if size > 8192:
            self.send_response(413)
            self.end_headers()
            return
        body = self.rfile.read(size).decode()
        with (root / 'received.jsonl').open('a') as target:
            target.write(json.dumps({'message': body}) + '\n')
        self.send_response(204)
        self.end_headers()
    def log_message(self, *args):
        pass
server = HTTPServer(('127.0.0.1', 18446), Receiver)
(root / 'receiver-ready').touch()
server.serve_forever()
"""
STOP_RECEIVER = r"""
import os, pathlib, signal, sys, time
root = pathlib.Path(sys.argv[1])
marker = root / 'receiver.pid'
if marker.exists():
    pid = int(marker.read_text())
    command = pathlib.Path('/proc') / str(pid) / 'cmdline'
    expected = str(root / 'receiver.py').encode()
    if command.exists() and expected in command.read_bytes().split(b'\0'):
        os.kill(pid, signal.SIGTERM)
        for _ in range(100):
            if not command.exists() or not command.read_bytes():
                break
            time.sleep(0.1)
        if command.exists() and expected in command.read_bytes().split(b'\0'):
            raise SystemExit('temporary receiver did not stop')
    marker.unlink()
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    binaries = {host: args.binaries / ("synod-linux-" + info[2]) for host, info in HOSTS.items()}
    for binary in binaries.values():
        if not binary.is_file():
            parser.error("Missing binary: " + str(binary))
    args.evidence.mkdir(mode=0o700)
    root = "/tmp/synod-m4-T20-" + uuid.uuid4().hex
    fixture = "synod-m4-" + uuid.uuid4().hex[:20]
    project, volume = root + "/project", fixture + "_data"
    password, database_password, url_token = (secrets.token_hex(24) for _ in range(3))
    marker = "private-m4-state-" + secrets.token_hex(16)
    created, ids, baseline, drills = [], {}, {}, []
    proof = {"passed": False, "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
             "remote_directory": root, "project": fixture, "checks": [],
             "binaries": {host: hashlib.sha256(binary.read_bytes()).hexdigest() for host, binary in binaries.items()}}
    phase, failure = "prepare", None

    def request(*arguments):
        return json.loads(remote("server", [root + "/synod", "--json", "--attempts", "1",
                         "--api-url", f"https://{HOSTS['server'][3]}:18443", "--ca-file", root + "/controller/api-ca.crt",
                         "--token-file", root + "/controller/admin.token", *arguments]))

    def ready():
        try:
            return request("installation", "get").get("installation")
        except (RuntimeError, ValueError):
            return None

    def terminal(operation):
        current = request("operation", "get", operation)["operation"]
        return current if current["status"] in ("succeeded", "failed", "cancelled") else None

    def scratch(operation):
        found = remote("odin", ["docker", "ps", "-q", "--filter", "label=io.synod.restore.operation=" + operation,
                                "--filter", "label=io.synod.proof=true"]).decode().split()
        return found[0] if len(found) == 1 else None

    def query(host, container, sql):
        return remote(host, ["docker", "exec", container, "psql", "-v", "ON_ERROR_STOP=1", "-U", "proof_admin", "-d", "postgres", "-qAt", "-c", sql]).decode().strip()

    def database_ready(host, container):
        try:
            return query(host, container, "SELECT 1") == "1"
        except RuntimeError:
            return False

    def delivered(operation):
        return next((a for a in request("notification", "status")["notifications"]["alerts"]
                     if a.get("operation_id") == operation and a["state"] == "sent"), None)

    try:
        for host, binary in binaries.items():
            if remote(host, ["ss", "-H", "-ltn", "sport = :18443 or sport = :18444 or sport = :18445 or sport = :18446"]).strip():
                raise RuntimeError("Proof ports are occupied on " + host)
            if remote(host, ["docker", "image", "inspect", IMAGE, "--format", "{{.Architecture}}"] ).decode().strip() != HOSTS[host][2]:
                raise RuntimeError("Pinned PostgreSQL image has the wrong architecture on " + host)
            baseline[host] = {}
            for container in remote(host, ["docker", "ps", "-q", "--no-trunc"]).decode().split():
                baseline[host][container] = remote(host, ["docker", "inspect", "--format", "{{.State.Running}} {{.State.StartedAt}}", container])
            remote(host, ["mkdir", "-m", "700", root])
            created.append(host)
            remote(host, ["python3", "-c", WRITE, root + "/synod", "700"], binary.read_bytes())
        print("Prepared server and odin. Recorded existing container start times.", flush=True)
        phase = "start source database"
        remote("server", ["mkdir", "-m", "700", project])
        compose = {"services": {"database": {"image": IMAGE, "container_name": fixture,
                   "network_mode": "none", "labels": {"synod.proof": fixture},
                   "environment": {"POSTGRES_USER": "proof_admin", "POSTGRES_PASSWORD": database_password},
                   "volumes": ["data:/var/lib/postgresql/data", project + ":/config:ro"]}},
                   "volumes": {"data": {"name": volume, "labels": {"synod.proof": fixture}}}}
        remote("server", ["python3", "-c", WRITE, project + "/compose.json", "600"], json.dumps(compose).encode())
        remote("server", ["python3", "-c", WRITE, project + "/state", "600"], marker.encode())
        remote("server", ["docker", "compose", "-p", fixture, "-f", project + "/compose.json", "up", "-d"])
        wait(lambda: database_ready("server", fixture), "Source database did not start")
        query("server", fixture, "CREATE TABLE drill_proof(id integer PRIMARY KEY, value text NOT NULL); "
              "INSERT INTO drill_proof SELECT g,'saved-row-' || g FROM generate_series(1,100) g; CREATE ROLE proof_reader LOGIN;")
        source_started = remote("server", ["docker", "inspect", "--format", "{{.State.StartedAt}}", fixture])
        phase = "join and approve devices"
        remote("server", [root + "/synod", "server", "init", "--dir", root + "/controller", "--address", HOSTS["server"][3], "--systemd-dir", root + "/no-systemd"])
        remote("server", ["python3", "-c", START, root, "controller", root + "/synod", "server", "--config", root + "/controller/controller.json", "--listen", "0.0.0.0:18443", "--agent-listen", "0.0.0.0:18444"])
        proof["installation_id"] = wait(ready, "Controller did not start")["installation_id"]
        for host in HOSTS:
            token = request("node", "join-token")["join_token"]
            remote(host, [root + "/synod", "agent", "join", token, "--dir", root + "/agent", "--systemd-dir", root + "/no-systemd", "--destination-listen", "0.0.0.0:18445", "--destination-address", HOSTS[host][3] + ":18445"])
            ids[host] = remote(host, ["cat", root + "/agent/node-id"]).decode().strip()
            remote(host, ["python3", "-c", START, root, "agent", root + "/synod", "agent", "run", "--config", root + "/agent/agent.json"])
        wait(lambda: len([n for n in request("node", "list")["nodes"] if n["connected_at"]]) == 2, "Both agents did not connect")
        for node in ids.values():
            request("node", "approve", node)
        app = wait(lambda: next((a for a in request("app", "list").get("apps", []) if a["node_id"] == ids["server"] and a["definition"]["name"] == fixture), None), "Source app was not discovered")
        phase = "configure loopback notification receiver"
        remote("server", ["python3", "-c", WRITE, root + "/receiver.py", "600"], RECEIVER.encode())
        remote("server", ["python3", "-c", START, root, "receiver", "python3", root + "/receiver.py", root])
        wait(lambda: remote("server", ["python3", "-c", "from pathlib import Path; import sys; print(Path(sys.argv[1]).exists())", root + "/receiver-ready"]).strip() == b"True", "Notification receiver did not start")
        settings = {"url": "generic+http://127.0.0.1:18446/" + url_token}
        remote("server", ["python3", "-c", WRITE, root + "/notification.json", "600"], json.dumps(settings).encode())
        request("notification", "configure", "--file", root + "/notification.json")
        remote("server", ["python3", "-c", WRITE, root + "/credentials.json", "600"], json.dumps({"password": password}).encode())
        destination = request("destination", "add", "survivor", "--type", "device", "--device", ids["odin"], "--path", root + "/repository", "--credentials-file", root + "/credentials.json")["destination"]["id"]
        wait(lambda: all(d["current"] for d in request("destination", "list")["destinations"]["delivery"]), "Destination delivery did not finish")
        phase = "back up the database"
        backup = request("backup", app["id"], "--destination", destination, "--method", "auto", "--wait=false")["operation"]
        backup = wait(lambda: terminal(backup["id"]), "Backup did not finish", seconds=300)
        if backup["status"] != "succeeded" or backup["result"].get("warning_codes"):
            raise RuntimeError("Backup failed or reported warnings: " + backup.get("failure_code", ""))
        snapshot = backup["result"]["snapshot_id"]
        if backup["result"]["downtime_ms"] != 0:
            raise RuntimeError("Database backup stopped the source")
        proof.update(source_app_id=app["id"], snapshot_id=snapshot, source_architecture="amd64", drill_architecture="arm64")
        phase = "check repository on odin"
        check = request("destination", "check", destination, "--on", ids["odin"], "--wait=false")["operation"]
        check = wait(lambda: terminal(check["id"]), "Integrity check did not finish")
        if check["status"] != "succeeded" or check["result"]["read"] < 1:
            raise RuntimeError("Integrity check did not read saved content")
        proof["checks"].append("An arm64 agent read and verified a sample of the encrypted repository")
        print("Database backup and repository check passed. Starting an isolated arm64 drill.", flush=True)
        phase = "successful isolated drill"
        drill = request("restore", app["id"], "--to", ids["odin"], "--destination", destination, "--snapshot", snapshot, "--drill", "--wait=false")["operation"]
        drills.append(drill["id"])
        temporary = wait(lambda: scratch(drill["id"]), "Drill container did not start", seconds=300)
        inspected = json.loads(remote("odin", ["docker", "inspect", temporary]))[0]
        if inspected["HostConfig"]["NetworkMode"] != "none" or inspected["HostConfig"]["PortBindings"]:
            raise RuntimeError("Drill container has network access or published ports")
        wait(lambda: database_ready("odin", temporary), "Drill database did not become ready", seconds=20)
        if query("odin", temporary, "SELECT count(*) FROM drill_proof WHERE value='saved-row-' || id") != "100":
            raise RuntimeError("Drill lost saved database rows")
        if query("odin", temporary, "SELECT count(*) FROM pg_roles WHERE rolname='proof_reader'") != "1":
            raise RuntimeError("Drill lost the saved role")
        if remote("odin", ["docker", "exec", temporary, "cat", "/config/state"]).decode() != marker:
            raise RuntimeError("Drill lost the saved app file")
        if remote("odin", ["docker", "exec", temporary, "uname", "-m"]).strip() != b"aarch64":
            raise RuntimeError("Drill did not run on arm64")
        completed = wait(lambda: terminal(drill["id"]), "Drill did not finish")
        if completed["status"] != "succeeded" or completed["result"]["snapshot_id"] != snapshot:
            raise RuntimeError("Drill failed: " + completed.get("failure_code", ""))
        app_status = next(a for a in request("app", "status")["protection"]["apps"] if a["app"]["id"] == app["id"])
        tested_at = app_status["restore_tested_at"]
        proof.update(successful_drill_id=drill["id"], restored_rows=100, restore_tested_at=tested_at)
        proof["checks"].append("The isolated arm64 drill restored all 100 rows, the saved role, and app files, then passed its observation period")
        print("The isolated database drill passed. Forcing a second drill to fail.", flush=True)
        phase = "forced drill failure"
        failed = request("restore", app["id"], "--to", ids["odin"], "--destination", destination, "--snapshot", snapshot, "--drill", "--wait=false")["operation"]
        drills.append(failed["id"])
        temporary = wait(lambda: scratch(failed["id"]), "Second drill did not start", seconds=300)
        remote("odin", ["docker", "stop", "--time", "1", temporary])
        completed = wait(lambda: terminal(failed["id"]), "Failed drill did not report cleanup")
        if completed["status"] != "failed" or completed["result"]["failure_code"] != "drill_container_stopped":
            raise RuntimeError("Forced stop did not produce the expected drill failure")
        phase = "notification delivery"
        alert = wait(lambda: delivered(failed["id"]), "Shoutrrr did not deliver the drill alert")
        received = remote("server", ["cat", root + "/received.jsonl"])
        if alert["kind"] != "drill_failed" or alert["attempts"] != 1 or failed["id"].encode() not in received or alert["id"].encode() not in received:
            raise RuntimeError("The receiver did not get the failed operation and alert IDs")
        app_status = next(a for a in request("app", "status")["protection"]["apps"] if a["app"]["id"] == app["id"])
        if app_status["restore_tested_at"] != tested_at or app_status["last_drill"]["state"] != "failed":
            raise RuntimeError("Failed drill erased the successful tested date")
        proof.update(failed_drill_id=failed["id"], alert_id=alert["id"], delivery_attempts=alert["attempts"])
        proof["checks"].append("A forced container stop failed the drill and sent its operation ID through Shoutrrr to the loopback receiver")
        phase = "drill cleanup and capacity"
        for operation in drills:
            if remote("odin", ["docker", "ps", "-aq", "--filter", "label=io.synod.restore.operation=" + operation]).strip():
                raise RuntimeError("A drill container remains")
            if remote("odin", ["docker", "volume", "ls", "-q", "--filter", "label=io.synod.restore.operation=" + operation]).strip():
                raise RuntimeError("A drill volume remains")
            if remote("odin", ["python3", "-c", "import pathlib,sys; print(pathlib.Path(sys.argv[1]).exists())", root + "/agent/restores/" + operation]).strip() != b"False":
                raise RuntimeError("A drill directory remains")
        capacity = wait(lambda: next((s for s in request("destination", "list")["destinations"]["space"] if s["destination_id"] == destination and s["known"]), None), "Destination capacity was not reported")
        if capacity["total_bytes"] <= 0 or not 0 <= capacity["available_bytes"] <= capacity["total_bytes"]:
            raise RuntimeError("Invalid filesystem capacity")
        proof["checks"].append("Both drills removed all scratch resources; status kept the last good tested date and showed current destination capacity")
        phase = "source and household safety"
        if remote("server", ["docker", "inspect", "--format", "{{.State.StartedAt}}", fixture]) != source_started:
            raise RuntimeError("Proof restarted the source app")
        if query("server", fixture, "SELECT count(*) FROM drill_proof") != "100":
            raise RuntimeError("Proof changed the source database")
        for host, containers in baseline.items():
            for container, state in containers.items():
                if remote(host, ["docker", "inspect", "--format", "{{.State.Running}} {{.State.StartedAt}}", container]) != state:
                    raise RuntimeError("A pre-existing container changed state on " + host)
        proof["checks"].append("The source app and all pre-existing containers kept their start times; source data stayed unchanged")
        proof["passed"] = True
    except (RuntimeError, OSError, ValueError, KeyError, StopIteration) as error:
        failure = f"{phase}: {error}"
        proof["failure"] = failure
    finally:
        cleanup = []
        # Stop the source project first. Then stop agents and their controller.
        if "server" in created:
            try:
                if remote("server", ["python3", "-c", "import pathlib,sys; print(pathlib.Path(sys.argv[1]).exists())", project + "/compose.json"]).strip() == b"True":
                    remote("server", ["docker", "compose", "-p", fixture, "-f", project + "/compose.json", "down", "--volumes"])
            except RuntimeError as error:
                cleanup.append(str(error))
        for host in reversed(created):
            try:
                remote(host, ["python3", "-c", STOP, root, "agent"])
                for operation in drills:
                    label = "io.synod.restore.operation=" + operation
                    containers = remote(host, ["docker", "ps", "-aq", "--filter", "label=" + label]).decode().split()
                    if containers:
                        remote(host, ["docker", "rm", "-fv", *containers])
                    volumes = remote(host, ["docker", "volume", "ls", "-q", "--filter", "label=" + label]).decode().split()
                    if volumes:
                        remote(host, ["docker", "volume", "rm", *volumes])
                remote(host, ["python3", "-c", STOP, root, "controller"])
                remote(host, ["python3", "-c", STOP_RECEIVER, root])
            except RuntimeError as error:
                cleanup.append(str(error))
        for host in created:
            for role in ("agent", "controller", "receiver"):
                try:
                    log = remote(host, ["python3", "-c", "import pathlib,sys; p=pathlib.Path(sys.argv[1]); sys.stdout.buffer.write(p.read_bytes() if p.exists() else b'')", root + "/" + role + ".log"])
                    if any(value.encode() in log for value in (password, database_password, url_token, marker)):
                        cleanup.append(f"Private data found in {host} {role} log; log was not copied")
                    else:
                        (args.evidence / f"{host}-{role}.log").write_bytes(log)
                except RuntimeError as error:
                    cleanup.append(str(error))
        if not cleanup:
            for host in created:
                try:
                    remote(host, ["python3", "-c", "import shutil,sys; shutil.rmtree(sys.argv[1])", root])
                except RuntimeError as error:
                    cleanup.append(str(error))
        proof["cleanup_errors"] = cleanup
        if cleanup:
            proof["passed"] = False
        (args.evidence / "result.json").write_text(json.dumps(proof, indent=2) + "\n")
    if failure or cleanup:
        raise SystemExit("M4 proof incomplete. Read " + str(args.evidence / "result.json"))
    print("M4 proof passed. Evidence: " + str(args.evidence / "result.json"), flush=True)


if __name__ == "__main__":
    main()
