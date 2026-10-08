#!/usr/bin/env python3
"""Prove database consistency and disaster recovery across server and odin.

Use disposable projects only. Get host approval before execution.
The proof deletes its own controller, agent state, source project, and volume.
It uses the SSH and process cleanup functions from proof-m1.py.
"""

import argparse
import datetime
import hashlib
import json
from pathlib import Path
import runpy
import secrets
import time
import uuid

shared = runpy.run_path(str(Path(__file__).with_name("proof-m1.py")))
remote, wait = shared["remote"], shared["wait"]
HOSTS, START, STOP, WRITE = (shared[key] for key in ("HOSTS", "START", "STOP", "WRITE"))
IMAGE = "postgres@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0"
HELPER_IMAGE = shared["IMAGE"]
WRITER = r"""set -eu
exec 2>/tmp/synod-proof-error
trap 'result=$?; printf "%s\n" "$result" > /tmp/synod-proof-exit; touch /tmp/synod-proof-done' EXIT
while [ ! -e /tmp/synod-proof-stop ]; do
    id=$(psql -v ON_ERROR_STOP=1 -U app_admin -d postgres -qAt -c "WITH added AS (INSERT INTO proof_rows(value) VALUES ('writer-value') RETURNING id), changed AS (UPDATE proof_count SET total=(SELECT id FROM added) RETURNING total) SELECT total FROM changed")
    printf '%s %s\n' "$id" "$(date +%s%N)" >> /tmp/synod-proof-acks
    sleep 0.02
done
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    binaries = {host: args.binaries / ("synod-linux-" + details[2]) for host, details in HOSTS.items()}
    for binary in binaries.values():
        if not binary.is_file():
            parser.error("Missing binary: " + str(binary))
    args.evidence.mkdir(mode=0o700)
    root = "/tmp/synod-m3-T17-" + uuid.uuid4().hex
    fixture = "synod-m3-" + uuid.uuid4().hex[:20]
    volume = fixture + "_pgdata"
    project = root + "/project"
    password, database_password = "", secrets.token_hex(24)
    marker = "private-m3-state-" + secrets.token_hex(16)
    proof = {"passed": False, "remote_directory": root, "project": fixture, "checks": [],
             "binaries": {host: hashlib.sha256(binary.read_bytes()).hexdigest() for host, binary in binaries.items()}}
    created, ids, baseline, restores = [], {}, {}, []
    controller_host = "server"
    phase, failure = "prepare", None

    def request(*arguments):
        address = HOSTS[controller_host][3]
        return json.loads(remote(controller_host, [root + "/synod", "--json", "--attempts", "1",
                         "--api-url", f"https://{address}:18443", "--ca-file", root + "/controller/api-ca.crt",
                         "--token-file", root + "/controller/admin.token", *arguments]))

    def ready():
        try:
            return request("installation", "get").get("installation")
        except (RuntimeError, ValueError):
            return None

    def query(host, sql):
        return remote(host, ["docker", "exec", fixture, "psql", "-v", "ON_ERROR_STOP=1", "-U", "app_admin", "-d", "postgres", "-qAt", "-c", sql]).decode().strip()

    def database_ready(host):
        try:
            remote(host, ["docker", "exec", fixture, "sh", "-ec", '[ "$(cat /proc/1/comm)" = postgres ]'])
            return query(host, "SELECT 1") == "1"
        except RuntimeError:
            return False

    def delivery():
        listing = request("destination", "list")["destinations"]
        return listing["delivery"] and all(d["current"] for d in listing["delivery"])

    def terminal(operation):
        current = request("operation", "get", operation)["operation"]
        return current if current["status"] in ("succeeded", "failed", "cancelled") else None

    try:
        for host, binary in binaries.items():
            if remote(host, ["ss", "-H", "-ltn", "sport = :18443 or sport = :18444 or sport = :18445"]).strip():
                raise RuntimeError("Proof ports are occupied on " + host)
            for image in (IMAGE, HELPER_IMAGE):
                arch = remote(host, ["docker", "image", "inspect", image, "--format", "{{.Architecture}}"])
                if arch.decode().strip() != HOSTS[host][2]:
                    raise RuntimeError("Pinned image has the wrong architecture on " + host)
            baseline[host] = {}
            for container in remote(host, ["docker", "ps", "-q", "--no-trunc"]).decode().split():
                baseline[host][container] = remote(host, ["docker", "inspect", "--format", "{{.State.Running}} {{.State.StartedAt}}", container])
            remote(host, ["mkdir", "-m", "700", root])
            created.append(host)
            remote(host, ["python3", "-c", WRITE, root + "/synod", "700"], binary.read_bytes())
        print("Prepared both hosts and recorded existing container start times.", flush=True)
        phase = "start source database"
        remote("server", ["mkdir", "-m", "700", project])
        compose = {"services": {"database": {"image": IMAGE, "container_name": fixture,
                   "network_mode": "none", "labels": {"synod.proof": fixture},
                   "environment": {"POSTGRES_USER": "app_admin", "POSTGRES_PASSWORD": database_password},
                   "volumes": ["pgdata:/var/lib/postgresql/data", project + ":/config:ro"]}},
                   "volumes": {"pgdata": {"name": volume, "labels": {"synod.proof": fixture}}}}
        remote("server", ["python3", "-c", WRITE, project + "/compose.json", "600"], json.dumps(compose).encode())
        remote("server", ["python3", "-c", WRITE, project + "/state", "600"], marker.encode())
        remote("server", ["docker", "compose", "-p", fixture, "-f", project + "/compose.json", "up", "-d"])
        wait(lambda: database_ready("server"), "Source database did not start")
        query("server", "CREATE TABLE proof_rows(id bigserial PRIMARY KEY, value text NOT NULL); "
              "INSERT INTO proof_rows(value) SELECT repeat(md5(g::text),128) FROM generate_series(1,5000) g; "
              "CREATE TABLE proof_count(total bigint NOT NULL); INSERT INTO proof_count VALUES(5000); CREATE ROLE proof_reader LOGIN;")
        source_started = remote("server", ["docker", "inspect", "--format", "{{.State.StartedAt}}", fixture])
        phase = "join and approve devices"
        remote("server", [root + "/synod", "server", "init", "--dir", root + "/controller", "--address", HOSTS["server"][3], "--systemd-dir", root + "/no-systemd"])
        # The recovery kit is the only copy of the repository password that this proof keeps.
        kit = remote("server", ["cat", root + "/controller/recovery-kit.txt"]).decode().splitlines()
        password = kit[kit.index("Repository password:") + 1].strip()
        remote("server", ["python3", "-c", START, root, "controller", root + "/synod", "server", "--config", root + "/controller/controller.json", "--listen", "0.0.0.0:18443", "--agent-listen", "0.0.0.0:18444"])
        installation = wait(ready, "Controller did not start")["installation_id"]
        for host in HOSTS:
            token = request("node", "join-token")["join_token"]
            remote(host, [root + "/synod", "agent", "join", token, "--dir", root + "/agent", "--systemd-dir", root + "/no-systemd", "--destination-listen", "0.0.0.0:18445", "--destination-address", HOSTS[host][3] + ":18445"])
            ids[host] = remote(host, ["cat", root + "/agent/node-id"]).decode().strip()
            remote(host, ["python3", "-c", START, root, "agent", root + "/synod", "agent", "run", "--config", root + "/agent/agent.json"])
        wait(lambda: len([n for n in request("node", "list")["nodes"] if n["connected_at"]]) == 2, "Both agents did not connect")
        for node in ids.values():
            request("node", "approve", node)
        app = wait(lambda: next((a for a in request("app", "list").get("apps", []) if a["node_id"] == ids["server"] and a["definition"]["name"] == fixture), None), "Source app was not discovered")
        destination = request("destination", "add", "survivor", "--type", "device", "--device", ids["odin"], "--path", root + "/repository")["destination"]["id"]
        wait(delivery, "Both agents did not save the destination")
        phase = "back up during database writes"
        remote("server", ["docker", "exec", "-d", fixture, "sh", "-ec", WRITER])
        wait(lambda: int(query("server", "SELECT max(id) FROM proof_rows")) > 5005, "The database writer did not start")
        before = int(query("server", "SELECT max(id) FROM proof_rows"))
        backup = request("backup", app["id"], "--destination", destination, "--method", "auto", "--wait=false")["operation"]
        backup = wait(lambda: terminal(backup["id"]), "Backup did not finish", seconds=300)
        if backup["status"] != "succeeded" or backup["result"].get("warning_codes"):
            raise RuntimeError("Backup failed or reported warnings: " + backup.get("failure_code", ""))
        remote("server", ["docker", "exec", fixture, "touch", "/tmp/synod-proof-stop"])
        wait(lambda: remote("server", ["docker", "exec", fixture, "sh", "-c", "test -e /tmp/synod-proof-done && echo done || true"]).strip() == b"done", "The writer did not finish")
        writer_exit = remote("server", ["docker", "exec", fixture, "cat", "/tmp/synod-proof-exit"]).strip()
        if writer_exit != b"0":
            raise RuntimeError("The writer exited with code " + writer_exit.decode())
        after = int(query("server", "SELECT max(id) FROM proof_rows"))
        acknowledgements = [tuple(map(int, row.split())) for row in remote("server", ["docker", "exec", fixture, "cat", "/tmp/synod-proof-acks"]).decode().splitlines()]
        started = int(datetime.datetime.fromisoformat(backup["result"]["started_at"].replace("Z", "+00:00")).timestamp() * 1_000_000_000)
        completed = int(datetime.datetime.fromisoformat(backup["result"]["completed_at"].replace("Z", "+00:00")).timestamp() * 1_000_000_000)
        prior = max([before, *(row for row, stamp in acknowledgements if stamp < started)])
        during = sum(started <= stamp <= completed for _, stamp in acknowledgements)
        saved = next(s for s in request("snapshot", "list", app["id"])["files"]["snapshots"] if s["id"] == backup["result"]["snapshot_id"])
        if after <= before or during == 0 or saved["method"] != "dump" or not saved.get("databases") or saved["architecture"] != "amd64" or backup["result"]["downtime_ms"] != 0:
            raise RuntimeError("Backup did not prove active writes with no database stop")
        if remote("server", ["docker", "inspect", "--format", "{{.State.StartedAt}}", fixture]) != source_started:
            raise RuntimeError("Backup restarted the source database")
        snapshot = backup["result"]["snapshot_id"]
        proof.update(installation_id=installation, source_app_id=app["id"], snapshot_id=snapshot,
                     rows_before_request=before, required_rows=prior, writes_during_backup=during, rows_after_backup=after)
        proof["checks"].append("The PostgreSQL dump kept the source running and accepted writes during backup")
        print(f"Backup passed with {during} writes during backup. Deleting only the proof installation and source data.", flush=True)
        phase = "delete controller and source state"
        remote("server", ["docker", "compose", "-p", fixture, "-f", project + "/compose.json", "down", "--volumes"])
        for host in HOSTS:
            remote(host, ["python3", "-c", STOP, root, "agent"])
        remote("server", ["python3", "-c", STOP, root, "controller"])
        for host in HOSTS:
            paths = [root + "/agent"]
            if host == "server":
                paths.extend([root + "/controller", project])
            remote(host, ["python3", "-c", "import shutil,sys; [shutil.rmtree(p) for p in sys.argv[1:]]", *paths])
        phase = "recover controller on odin"
        remote("odin", ["python3", "-c", WRITE, root + "/password", "600"], password.encode())
        remote("odin", [root + "/synod", "server", "init", "--dir", root + "/controller", "--address", HOSTS["odin"][3], "--from-repository", root + "/repository", "--password-file", root + "/password", "--systemd-dir", root + "/no-systemd"])
        controller_host = "odin"
        remote("odin", ["python3", "-c", START, root, "controller", root + "/synod", "server", "--config", root + "/controller/controller.json", "--listen", "0.0.0.0:18443", "--agent-listen", "0.0.0.0:18444"])
        if wait(ready, "Recovered controller did not start")["installation_id"] != installation:
            raise RuntimeError("Recovery changed the installation ID")
        recovered_kit = remote("odin", ["cat", root + "/controller/recovery-kit.txt"]).decode()
        if password not in recovered_kit:
            raise RuntimeError("The recovered controller wrote another recovery kit password")
        if any(n["approved_at"] or n["connected_at"] for n in request("node", "list")["nodes"]):
            raise RuntimeError("Recovery trusted an old device")
        recovered_app = next(a for a in request("app", "list")["apps"] if a["definition"]["name"] == fixture)
        if recovered_app["id"] != app["id"] or recovered_app["present"]:
            raise RuntimeError("Recovery lost the historical app")
        recovered_node = next(n for n in request("node", "list")["nodes"] if n["architecture"] == "arm64")
        token = request("node", "join-token", "--device", recovered_node["id"])["join_token"]
        remote("odin", [root + "/synod", "agent", "join", token, "--dir", root + "/agent", "--systemd-dir", root + "/no-systemd", "--destination-listen", "0.0.0.0:18445", "--destination-address", HOSTS["odin"][3] + ":18445"])
        remote("odin", ["python3", "-c", START, root, "agent", root + "/synod", "agent", "run", "--config", root + "/agent/agent.json"])
        wait(lambda: any(n["id"] == recovered_node["id"] and n["connected_at"] for n in request("node", "list")["nodes"]), "Recovered agent did not connect")
        request("node", "approve", recovered_node["id"])
        wait(delivery, "Recovered agent did not save the destination")
        proof["checks"].append("A destination added without a password used the recovery kit, and the kit password alone rebuilt the installation on odin without old controller or agent files")
        print("Recovered the controller and repository device on odin. Restoring the amd64 source app on arm64.", flush=True)
        phase = "restore across architectures"
        result = request("restore", recovered_app["id"], "--to", recovered_node["id"], "--snapshot", snapshot, "--wait=false")["operation"]
        restores.append(result["id"])
        result = wait(lambda: terminal(result["id"]), "Restore did not finish", seconds=600)
        if result["status"] != "succeeded":
            raise RuntimeError("Restore failed: " + result.get("failure_code", ""))
        wait(lambda: database_ready("odin"), "Restored database did not start")
        count, last, counter = map(int, query("odin", "SELECT count(*),max(id),(SELECT total FROM proof_count) FROM proof_rows").split("|"))
        if count != last or count != counter or count < prior or int(query("odin", f"SELECT count(*) FROM proof_rows WHERE id<={prior}")) != prior:
            raise RuntimeError("Restored rows lost an acknowledged write or crossed a transaction boundary")
        if query("odin", "SELECT count(*) FROM pg_roles WHERE rolname='proof_reader'") != "1":
            raise RuntimeError("Restore lost the saved database role")
        if remote("odin", ["docker", "exec", fixture, "cat", "/config/state"]).decode() != marker:
            raise RuntimeError("Restore changed the saved app file")
        if remote("odin", ["docker", "exec", fixture, "uname", "-m"]).strip() != b"aarch64":
            raise RuntimeError("Restored app did not run on arm64")
        proof.update(restored_rows=count, restore_operation_id=result["id"], source_architecture="amd64", target_architecture="arm64")
        proof["checks"].append("The restored arm64 app kept every write acknowledged before backup, the transaction counter, the saved role, and app files")
        phase = "check existing containers"
        for host, containers in baseline.items():
            for container, state in containers.items():
                if remote(host, ["docker", "inspect", "--format", "{{.State.Running}} {{.State.StartedAt}}", container]) != state:
                    raise RuntimeError("A pre-existing container changed state on " + host)
        proof["checks"].append("Pre-existing running containers kept their start times on both hosts")
        proof["passed"] = True
    except (RuntimeError, OSError, ValueError, KeyError, StopIteration) as error:
        failure = f"{phase}: {error}"
        proof["failure"] = failure
        if phase == "back up during database writes":
            try:
                diagnostic = remote("server", ["docker", "exec", fixture, "sh", "-c", "cat /tmp/synod-proof-error /tmp/synod-proof-exit 2>/dev/null || true"]).decode()
                for value in [v for v in (password, database_password, marker) if v]:
                    diagnostic = diagnostic.replace(value, "[private]")
                proof["writer_diagnostic"] = diagnostic[:2000]
                diagnostic = remote("server", ["sh", "-c", "docker logs " + fixture + " 2>&1"]).decode()
                for value in [v for v in (password, database_password, marker) if v]:
                    diagnostic = diagnostic.replace(value, "[private]")
                proof["database_log"] = diagnostic[-12000:]
                proof["memory_events"] = remote("server", ["docker", "exec", fixture, "cat", "/sys/fs/cgroup/memory.events", "/sys/fs/cgroup/memory.max"]).decode()
            except RuntimeError:
                pass
    finally:
        cleanup = []
        for host in reversed(created):
            try:
                remote(host, ["python3", "-c", STOP, root, "agent"])
                for label in ["synod.proof=" + fixture, *["io.synod.restore.operation=" + op for op in restores]]:
                    containers = remote(host, ["docker", "ps", "-aq", "--filter", "label=" + label]).decode().split()
                    if containers:
                        remote(host, ["docker", "rm", "-fv", *containers])
                    volumes = remote(host, ["docker", "volume", "ls", "-q", "--filter", "label=" + label]).decode().split()
                    if volumes:
                        remote(host, ["docker", "volume", "rm", *volumes])
                remote(host, ["python3", "-c", STOP, root, "controller"])
            except RuntimeError as error:
                cleanup.append(str(error))
        for host in created:
            for role in ("agent", "controller"):
                try:
                    log = remote(host, ["python3", "-c", "import pathlib,sys; p=pathlib.Path(sys.argv[1]); sys.stdout.buffer.write(p.read_bytes() if p.exists() else b'')", root + "/" + role + ".log"])
                    if any(secret.encode() in log for secret in [v for v in (password, database_password, marker) if v]):
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
        (args.evidence / "result.json").write_text(json.dumps(proof, indent=2) + "\n")
    if failure or cleanup:
        raise SystemExit("M2/M3 proof incomplete. Read " + str(args.evidence / "result.json"))
    print("M2/M3 proof passed. Evidence: " + str(args.evidence / "result.json"), flush=True)


if __name__ == "__main__":
    main()
