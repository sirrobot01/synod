#!/usr/bin/env python3
"""Prove the v0.1 release on server and odin: install, protect, lose a device, restore.

Use disposable projects only. The owner gave standing approval for these hosts.
The controller and the odin agent run as temporary systemd services on odin.
The server agent runs as a temporary process, because server has no passwordless sudo.
That agent cannot read Docker named volumes, so the server app keeps its files in bind mounts.
Cleanup removes the services, processes, fixtures, and the proof directory.
"""

import argparse
import datetime
import hashlib
import http.cookiejar
import json
from pathlib import Path
import runpy
import secrets
import ssl
import subprocess
import time
import urllib.request
import uuid

shared = runpy.run_path(str(Path(__file__).with_name("proof-m1.py")))
remote, wait = shared["remote"], shared["wait"]
HOSTS, START, STOP, WRITE, IMAGE = (shared[key] for key in ("HOSTS", "START", "STOP", "WRITE", "IMAGE"))
DATABASE_IMAGE = "postgres@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0"
UNITS = ("synod-agent", "synod-server")
READ = "import pathlib,sys; p=pathlib.Path(sys.argv[1]); sys.stdout.buffer.write(p.read_bytes() if p.exists() else b'')"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", required=True, type=Path, help="Release directory from scripts/build-release.sh")
    parser.add_argument("--evidence", required=True, type=Path, help="New private directory for the result and logs")
    args = parser.parse_args()
    release = json.loads((args.release / "release.json").read_text())
    version = release["version"]
    args.evidence.mkdir(mode=0o700)
    root = "/tmp/synod-release-T25-" + uuid.uuid4().hex
    fixture = "synod-release-" + uuid.uuid4().hex[:16]
    project = root + "/project"
    address = HOSTS["odin"][3]
    synod = root + "/synod"
    marker, database_password, operator_password = "release-state-" + secrets.token_hex(16), secrets.token_hex(24), secrets.token_hex(24)
    password = ""
    created, units, ids, baseline, restores = [], [], {}, {}, []
    proof = {"passed": False, "version": version, "revision": release["revision"], "remote_directory": root, "project": fixture, "checks": [], "timings": {}}
    phase, failure, since = "prepare", None, ""

    def private_values():
        return [value for value in (password, marker, database_password, operator_password) if value]

    def request(*arguments):
        result = json.loads(remote("odin", ["python3", "-c",
            'import json,subprocess,sys; r=subprocess.run(sys.argv[1:],capture_output=True,text=True); print(json.dumps({"code":r.returncode,"output":r.stdout}))',
            synod, "--json", "--attempts", "1", "--api-url", "https://" + address + ":8443",
            "--ca-file", root + "/controller/api-ca.crt", "--token-file", root + "/controller/admin.token", *arguments]))
        value = json.loads(result["output"] or "{}")
        if result["code"]:
            raise RuntimeError("CLI " + " ".join(arguments[:2]) + ": " + value.get("error", {}).get("code", "unknown"))
        return value

    def ready():
        try:
            return request("installation", "get")["installation"]
        except (RuntimeError, ValueError):
            return None

    def terminal(operation):
        value = request("operation", "get", operation)["operation"]
        return value if value["status"] in ("succeeded", "failed", "cancelled") else None

    def finish(operation, message, seconds=600):
        value = wait(lambda: terminal(operation["id"]), message, seconds=seconds)
        if value["status"] != "succeeded":
            raise RuntimeError(message + ": " + value.get("failure_code", value["status"]))
        return value

    def delivered():
        listing = request("destination", "list")["destinations"]
        return len(listing["delivery"]) == len(ids) and all(d["current"] for d in listing["delivery"])

    def plan(app_id):
        return next((a for a in request("app", "status")["protection"]["apps"] if a["app"]["id"] == app_id), None)

    def query(host, container, sql):
        return remote(host, ["docker", "exec", container, "psql", "-v", "ON_ERROR_STOP=1", "-U", "app", "-d", "postgres", "-qAt", "-c", sql]).decode().strip()

    def database_ready(host, container):
        try:
            remote(host, ["docker", "exec", container, "sh", "-ec", '[ "$(cat /proc/1/comm)" = postgres ]'])
            return query(host, container, "SELECT 1") == "1"
        except RuntimeError:
            return False

    def files_digest(host, container):
        return remote(host, ["docker", "exec", container, "sh", "-ec", "cat /data/config.json; sha256sum /files/blob | cut -d' ' -f1"]).decode()

    def step(name, started):
        proof["timings"][name] = round(time.monotonic() - started, 1)
        print(f"{name}: {proof['timings'][name]} s", flush=True)

    try:
        for host, (_, _, arch, _) in HOSTS.items():
            ports = "sport = :8443 or sport = :8444 or sport = :8445" if host == "odin" else "sport = :18445"
            if remote(host, ["ss", "-H", "-ltn", ports]).strip():
                raise RuntimeError("Proof ports are occupied on " + host)
            for image in (IMAGE, DATABASE_IMAGE):
                if remote(host, ["docker", "image", "inspect", image, "--format", "{{.Architecture}}"]).decode().strip() != arch:
                    raise RuntimeError("A pinned image is absent on " + host)
            baseline[host] = {c: remote(host, ["docker", "inspect", "--format", "{{.State.Running}} {{.State.StartedAt}}", c])
                              for c in remote(host, ["docker", "ps", "-q", "--no-trunc"]).decode().split()}
        since = remote("odin", ["date", "+%Y-%m-%d %H:%M:%S"]).decode().strip()
        if remote("odin", ["sh", "-c", "ls /etc/systemd/system/synod-*.service 2>/dev/null || true"]).strip():
            raise RuntimeError("A Synod service already exists on odin")
        for host, (_, _, arch, _) in HOSTS.items():
            remote(host, ["mkdir", "-m", "700", root, root + "/release"])
            created.append(host)
            for asset in ("install.sh", "release.json", "SHA256SUMS", "synod-linux-" + arch):
                remote(host, ["python3", "-c", WRITE, root + "/release/" + asset, "600"], (args.release / asset).read_bytes())
            started = time.monotonic()
            remote(host, ["sh", root + "/release/install.sh", "--version", version, "--from-dir", root + "/release", "--install-dir", root])
            identity = remote(host, [synod, "--version"]).decode()
            if version not in identity or "linux/" + arch not in identity:
                raise RuntimeError("Wrong installed build on " + host)
            step("install_" + host, started)
        proof["checks"].append("The verified installer put the matching release binary on amd64 and arm64")

        phase = "create the controller"
        started = time.monotonic()
        output = remote("odin", [synod, "server", "init", "--dir", root + "/controller", "--address", address]).decode()
        units.append("synod-server")
        lines = output.splitlines()
        password = lines[lines.index("Repository password:") + 1].strip()
        token = next(line.split()[-1] for line in lines if line.strip().startswith("synod agent join "))
        kit = remote("odin", ["cat", root + "/controller/recovery-kit.txt"]).decode()
        mode = remote("odin", ["stat", "-c", "%a", root + "/controller/recovery-kit.txt"]).decode().strip()
        if password not in kit or mode != "600":
            raise RuntimeError("Init did not write a private recovery kit with the printed password")
        unit = remote("odin", ["systemctl", "cat", "synod-server"]).decode()
        if f"ExecStart={synod} server --config {root}/controller/controller.json" not in unit:
            raise RuntimeError("The controller service does not run the installed binary")
        remote("odin", ["systemctl", "daemon-reload"])
        remote("odin", ["systemctl", "enable", "--now", "synod-server"])
        installation = wait(ready, "Controller did not start")
        if installation["version"] != version:
            raise RuntimeError("The controller reports another version")
        step("controller", started)
        proof["installation_id"] = installation["installation_id"]
        proof["checks"].append("Init printed and wrote a private recovery kit, and the controller ran as a systemd service")

        phase = "join devices"
        started = time.monotonic()
        remote("odin", [synod, "agent", "join", token, "--dir", root + "/agent"])
        units.insert(0, "synod-agent")
        if f"ExecStart={synod} agent run --config {root}/agent/agent.json" not in remote("odin", ["systemctl", "cat", "synod-agent"]).decode():
            raise RuntimeError("The agent service does not run the installed binary")
        remote("odin", ["systemctl", "daemon-reload"])
        remote("odin", ["systemctl", "enable", "--now", "synod-agent"])
        ids["odin"] = remote("odin", ["cat", root + "/agent/node-id"]).decode().strip()
        token = request("node", "join-token")["join_token"]
        remote("server", [synod, "agent", "join", token, "--dir", root + "/agent", "--systemd-dir", root + "/no-systemd",
                          "--destination-listen", "0.0.0.0:18445", "--destination-address", HOSTS["server"][3] + ":18445"])
        ids["server"] = remote("server", ["cat", root + "/agent/node-id"]).decode().strip()
        remote("server", ["python3", "-c", START, root, "agent", synod, "agent", "run", "--config", root + "/agent/agent.json"])
        nodes = wait(lambda: (lambda n: n if len([x for x in n if x["connected_at"]]) == 2 else None)(request("node", "list")["nodes"]), "Both agents did not connect")
        for node in nodes:
            host = next(h for h, i in ids.items() if i == node["id"])
            if node["architecture"] != HOSTS[host][2] or node["agent_version"] != version:
                raise RuntimeError("Device " + host + " reports the wrong architecture or version")
            request("node", "approve", node["id"])
        step("join", started)
        proof["nodes"] = ids
        proof["checks"].append("Both devices joined with the reported architecture and version, and were approved")

        phase = "sign in to the web UI"
        remote("odin", ["python3", "-c", WRITE, root + "/operator-password", "600"], operator_password.encode())
        request("user", "create", "admin", "--password-file", root + "/operator-password")
        ca = args.evidence / "api-ca.crt"
        ca.write_bytes(remote("odin", ["cat", root + "/controller/api-ca.crt"]))
        context = ssl.create_default_context(cafile=str(ca))
        cookies = http.cookiejar.CookieJar()
        browser = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context), urllib.request.HTTPCookieProcessor(cookies))
        origin = "https://" + address + ":8443"
        page = browser.open(origin + "/", timeout=10).read().decode()
        if "<div id=\"app\"" not in page and "<script" not in page:
            raise RuntimeError("The controller did not serve the web UI")
        sign_in = urllib.request.Request(origin + "/v1/sessions", method="POST", data=json.dumps({"username": "admin", "password": operator_password}).encode(),
                                         headers={"Content-Type": "application/json", "Sec-Fetch-Site": "same-origin", "Origin": origin})
        if browser.open(sign_in, timeout=10).status != 201:
            raise RuntimeError("The operator could not sign in")
        apps_page = urllib.request.Request(origin + "/v1/apps", headers={"Sec-Fetch-Site": "same-origin"})
        json.loads(browser.open(apps_page, timeout=10).read())
        proof["checks"].append("The controller served the web UI over the private CA, and an operator session read the app list")

        phase = "start the app"
        remote("server", ["mkdir", "-m", "700", project, project + "/data", project + "/files"])
        compose = {"services": {
            "db": {"image": DATABASE_IMAGE, "container_name": fixture + "-db", "network_mode": "none", "labels": {"synod.proof": fixture},
                   "environment": {"POSTGRES_USER": "app", "POSTGRES_PASSWORD": database_password}, "volumes": ["pgdata:/var/lib/postgresql/data"]},
            "files": {"image": IMAGE, "container_name": fixture + "-files", "network_mode": "none", "labels": {"synod.proof": fixture},
                      "command": ["sh", "-c", "while :; do sleep 1; done"], "volumes": [project + "/data:/data", project + "/files:/files"]}},
            "volumes": {"pgdata": {"name": fixture + "_pgdata", "labels": {"synod.proof": fixture}}}}
        remote("server", ["python3", "-c", WRITE, project + "/compose.json", "600"], json.dumps(compose).encode())
        remote("server", ["python3", "-c", WRITE, project + "/data/config.json", "644"], json.dumps({"marker": marker, "generation": 1}).encode())
        remote("server", ["docker", "compose", "-p", fixture, "-f", project + "/compose.json", "up", "-d"])
        wait(lambda: database_ready("server", fixture + "-db"), "Source database did not start")
        query("server", fixture + "-db", "CREATE TABLE notes(id bigserial PRIMARY KEY, body text NOT NULL); INSERT INTO notes(body) SELECT md5(g::text) FROM generate_series(1,20000) g;")
        remote("server", ["docker", "exec", fixture + "-files", "sh", "-ec", "head -c 33554432 /dev/urandom > /files/blob"])
        source_files = files_digest("server", fixture + "-files")
        app = wait(lambda: next((a for a in request("app", "list").get("apps", []) if a["node_id"] == ids["server"] and a["definition"]["name"] == fixture), None),
                   "The app was not discovered", seconds=180)
        proof["source_app_id"] = app["id"]

        phase = "add a destination"
        destination = request("destination", "add", "peer", "--type", "device", "--device", ids["odin"], "--path", root + "/repository")["destination"]["id"]
        wait(delivered, "Both agents did not save the destination")

        phase = "protect the app"
        started = time.monotonic()
        preview = request("protect", app["id"], "--destination", destination)
        review = json.dumps(preview)
        if "dumped" not in review.lower() or "included" not in review.lower():
            raise RuntimeError("The preview does not show Included and Dumped paths")
        request("protect", app["id"], "--destination", destination, "--confirm", str(preview["protection"]["revision"]))
        def first_backup():
            current = plan(app["id"])
            target = current["targets"][0] if current and current["targets"] else {}
            if target.get("failure_code"):
                raise RuntimeError("The first backup failed: " + target["failure_code"])
            return current if target.get("last_good") else None
        first = wait(first_backup, "The new plan did not run its first backup", seconds=600)
        step("first_backup", started)
        first_snapshot = first["targets"][0]["last_good"]["snapshot_id"]
        snapshots = request("snapshot", "list", app["id"], "--destination", destination)["files"]["snapshots"]
        saved = next(s for s in snapshots if s["id"] == first_snapshot)
        if saved["method"] != "dump" or not saved.get("databases") or saved["architecture"] != "amd64":
            raise RuntimeError("The first snapshot did not dump the database")
        request("file", "get", app["id"], project + "/data/config.json", "--destination", destination, "--via", ids["odin"], "--output", root + "/restored-config.json")
        if json.loads(remote("odin", ["cat", root + "/restored-config.json"]))["marker"] != marker:
            raise RuntimeError("The restored file differs")
        proof["first_snapshot"] = first_snapshot
        proof["checks"].append("Protection ran a dump backup at once, and a file restore through odin returned the saved file")

        phase = "scheduled proofs"
        started = time.monotonic()
        tested = wait(lambda: (lambda p: p if p and p.get("restore_tested_at") else None)(plan(app["id"])), "The scheduled drill did not pass", seconds=900)
        step("scheduled_drill", started)
        maintenance = wait(lambda: next((m for m in request("destination", "list")["destinations"]["maintenance"]
                                         if m["destination_id"] == destination and m.get("last") and m["last"]["state"] in ("succeeded", "failed")), None),
                           "Maintenance did not run", seconds=600)
        if maintenance["last"]["state"] != "succeeded" or maintenance["last"]["result"].get("maintenance") not in ("completed", "claimed"):
            raise RuntimeError("Maintenance did not complete")
        if maintenance["node_id"] != ids["odin"]:
            raise RuntimeError("Maintenance runs on another device than the repository owner")
        check = request("destination", "check", destination, "--on", ids["server"], "--wait=false")["operation"]
        finish(check, "The content sample failed")
        proof["drill"] = tested.get("last_drill")
        proof["maintenance"] = maintenance
        proof["checks"].append("The scheduled drill, the repository maintenance on odin, and a content sample passed")

        phase = "renew a device certificate"
        started = time.monotonic()
        token = request("node", "join-token", "--device", ids["odin"])["join_token"]
        remote("odin", ["systemctl", "stop", "synod-agent"])
        remote("odin", [synod, "agent", "renew", token, "--dir", root + "/agent"])
        remote("odin", ["systemctl", "start", "synod-agent"])
        wait(lambda: any(n["id"] == ids["odin"] and n["connected_at"] and n["approved_at"] for n in request("node", "list")["nodes"]), "The renewed agent did not reconnect")
        wait(delivered, "The renewed agent did not acknowledge its settings")
        step("renewal", started)
        proof["checks"].append("The odin agent renewed its certificate with a bound token and reconnected with its approval")

        phase = "back up a change"
        query("server", fixture + "-db", "INSERT INTO notes(body) SELECT 'second-' || g FROM generate_series(1,500) g;")
        remote("server", ["python3", "-c", "import json,sys; open(sys.argv[1],'w').write(json.dumps({'marker': sys.argv[2], 'generation': 2}))", project + "/data/config.json", marker])
        remote("server", ["docker", "exec", fixture + "-files", "sh", "-ec", "head -c 1048576 /dev/urandom >> /files/blob"])
        rows = int(query("server", fixture + "-db", "SELECT count(*) FROM notes"))
        source_files = files_digest("server", fixture + "-files")
        started = time.monotonic()
        second = finish(request("backup", app["id"], "--destination", destination, "--wait=false")["operation"], "The second backup failed")
        step("second_backup", started)
        proof["second_snapshot"] = second["result"]["snapshot_id"]

        phase = "lose the source device"
        remote("server", ["python3", "-c", STOP, root, "agent"])
        remote("server", ["docker", "compose", "-p", fixture, "-f", project + "/compose.json", "down", "--volumes"])
        remote("server", ["python3", "-c", "import shutil,sys; [shutil.rmtree(p) for p in sys.argv[1:]]", project, root + "/agent"])
        # The Devices screen marks a device with no report for two minutes.
        def silent():
            seen = next(n for n in request("node", "list")["nodes"] if n["id"] == ids["server"])["last_seen_at"]
            return time.time() - datetime.datetime.fromisoformat(seen.replace("Z", "+00:00")).timestamp() > 120
        wait(silent, "The lost device still reports", seconds=240)

        phase = "restore on the other device"
        started = time.monotonic()
        operation = request("restore", app["id"], "--to", ids["odin"], "--destination", destination, "--wait=false")["operation"]
        restores.append(operation["id"])
        restored = finish(operation, "The restore failed")
        step("restore", started)
        label = "io.synod.restore.operation=" + operation["id"]
        containers = {}
        for name in remote("odin", ["docker", "ps", "--filter", "label=" + label, "--format", "{{.Names}}"]).decode().split():
            containers["db" if name.endswith("-db") else "files"] = name
        if set(containers) != {"db", "files"}:
            raise RuntimeError("The restored app does not run both services")
        wait(lambda: database_ready("odin", containers["db"]), "The restored database did not start")
        if int(query("odin", containers["db"], "SELECT count(*) FROM notes")) != rows:
            raise RuntimeError("The restored database lost rows")
        if files_digest("odin", containers["files"]) != source_files:
            raise RuntimeError("The restored files differ")
        if remote("odin", ["docker", "exec", containers["files"], "uname", "-m"]).strip() != b"aarch64":
            raise RuntimeError("The restored app does not run on arm64")
        proof.update(restore_operation_id=operation["id"], restored_rows=rows, restore_result=restored["result"])
        proof["checks"].append("After the source device was lost, the newest backup restored on arm64 with every database row and both bind mounts")

        phase = "check existing containers"
        for host, states in baseline.items():
            for container, state in states.items():
                if remote(host, ["docker", "inspect", "--format", "{{.State.Running}} {{.State.StartedAt}}", container]) != state:
                    raise RuntimeError("A pre-existing container changed on " + host)
        proof["checks"].append("Every pre-existing container kept its start time")
        proof["passed"] = True
    except (RuntimeError, OSError, ValueError, KeyError, StopIteration, IndexError) as error:
        failure = phase + ": " + str(error)
        for value in private_values():
            failure = failure.replace(value, "[private]")
        proof["failure"] = failure
    finally:
        cleanup = []
        logs = {}
        if "odin" in created:
            for unit in units:
                try:
                    logs["odin-" + unit] = remote("odin", ["journalctl", "-u", unit, "--since", since, "--no-pager", "-o", "short-iso"])
                    remote("odin", ["systemctl", "disable", "--now", unit])
                    remote("odin", ["rm", "-f", "/etc/systemd/system/" + unit + ".service"])
                except RuntimeError as error:
                    cleanup.append(str(error))
            if units:
                try:
                    remote("odin", ["systemctl", "daemon-reload"])
                except RuntimeError as error:
                    cleanup.append(str(error))
        for host in reversed(created):
            try:
                if host == "server":
                    remote("server", ["python3", "-c", STOP, root, "agent"])
                    logs["server-agent"] = remote("server", ["python3", "-c", READ, root + "/agent.log"])
                for selector in ["synod.proof=" + fixture, *["io.synod.restore.operation=" + op for op in restores]]:
                    found = remote(host, ["docker", "ps", "-aq", "--filter", "label=" + selector]).decode().split()
                    if found:
                        remote(host, ["docker", "rm", "-fv", *found])
                    volumes = remote(host, ["docker", "volume", "ls", "-q", "--filter", "label=" + selector]).decode().split()
                    if volumes:
                        remote(host, ["docker", "volume", "rm", *volumes])
            except RuntimeError as error:
                cleanup.append(str(error))
        for name, log in logs.items():
            if any(value.encode() in log for value in private_values()):
                cleanup.append("Private data found in the " + name + " log; the log was not copied")
            else:
                (args.evidence / (name + ".log")).write_bytes(log)
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
        raise SystemExit("Release proof incomplete. Read " + str(args.evidence / "result.json"))
    print("Release proof passed. Evidence: " + str(args.evidence / "result.json"), flush=True)


if __name__ == "__main__":
    main()
