#!/usr/bin/env python3
"""Compare Synod backup speed with rsync and tar over SSH between two real hosts.

One host is the source and the other is the destination: server (amd64) and odin (arm64,
a Raspberry Pi 4). The script creates a disposable dataset and a test container on the
source, a temporary Synod installation with its controller on server, and a temporary
SSH key for odin. odin has no SSH client, so rsync and tar always run on server and push
to odin or pull from it. Cleanup removes all of them.
Each case reports wall time. Synod cases also report the agent's backup duration.
"""

import argparse
import json
import pathlib
import secrets
import shlex
import subprocess
import time
import uuid

SERVER, ODIN = "main@192.168.0.219", "dietpi@192.168.0.10"
SERVER_IP, ODIN_IP = "192.168.0.219", "192.168.0.10"
STOPPABLE = shlex.quote("trap 'exit 0' TERM; sleep 86400 & wait")
IMAGE = "alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"
DATASET = """
import os, random, sys
root, small, large = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
rng = random.Random(7)
for i in range(small):
    d = os.path.join(root, "small", "%03d" % (i % 200))
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "f%06d" % i), "wb") as f:
        f.write(os.urandom(rng.randint(1, 64) * 1024))
os.makedirs(os.path.join(root, "large"), exist_ok=True)
for i in range(large):
    with open(os.path.join(root, "large", "blob%d" % i), "wb") as f:
        for _ in range(512):
            f.write(os.urandom(1 << 20))
"""
CHANGE = """
import os, sys
root = sys.argv[1]
for i in range(0, 20000, 100):
    path = os.path.join(root, "small", "%03d" % (i % 200), "f%06d" % i)
    if os.path.exists(path):
        with open(path, "r+b") as f:
            f.write(os.urandom(4096))
with open(os.path.join(root, "large", "blob0"), "ab") as f:
    for _ in range(64):
        f.write(os.urandom(1 << 20))
"""


def ssh(host, command, data=None, timeout=1800, sudo=False):
    if sudo:
        command = "sudo -n " + command
    started = time.monotonic()
    result = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, command],
                            input=data, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{host}: {command[:120]}: exit {result.returncode}: {result.stderr.decode()[-400:]}")
    return result.stdout.decode(), time.monotonic() - started


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", required=True, type=pathlib.Path)
    parser.add_argument("--source", choices=["server", "odin"], default="server")
    parser.add_argument("--odin-storage", choices=["ram", "disk"], default="ram",
                        help="odin directory: tmpfs, or the SD card")
    parser.add_argument("--server-storage", choices=["disk", "ram"], default="disk",
                        help="server directory: its disk, or /dev/shm")
    parser.add_argument("--destination", choices=["device", "local"], default="device",
                        help="Synod destination: a directory on the other host, or a local path on the source")
    parser.add_argument("--skip-baseline", action="store_true", help="skip the rsync and tar cases")
    parser.add_argument("--small", type=int, default=20000)
    parser.add_argument("--large", type=int, default=2, help="number of 512 MiB files")
    parser.add_argument("--repeat", type=int, default=3, help="runs of each incremental case")
    parser.add_argument("--evidence", required=True, type=pathlib.Path)
    options = parser.parse_args()
    run_id = uuid.uuid4().hex[:12]
    marker = f"synod-bench-{run_id}"
    base = {SERVER: (f"/tmp/{marker}" if options.server_storage == "disk" else f"/dev/shm/{marker}"), ODIN: (f"/tmp/{marker}" if options.odin_storage == "ram" else f"/var/tmp/{marker}")}
    ip = {SERVER: SERVER_IP, ODIN: ODIN_IP}
    source, target = (SERVER, ODIN) if options.source == "server" else (ODIN, SERVER)
    privileged = lambda host: host == ODIN
    src, dst = base[source], base[target]
    result = {"run": run_id, "source": options.source, "odin_storage": options.odin_storage, "server_storage": options.server_storage, "destination": options.destination, "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(), "cases": [], "cleanup_errors": []}
    container = None
    for host in (SERVER, ODIN):
        running, _ = ssh(host, "pgrep -x synod || true", sudo=privileged(host))
        if running.strip():
            raise SystemExit(f"{host} already runs a synod process; the cleanup would stop it")
    try:
        ssh(SERVER, f"mkdir -m 700 {base[SERVER]}")
        ssh(ODIN, f"sh -c 'mkdir -p {base[ODIN]} && chmod 777 {base[ODIN]}'", sudo=True)
        ssh(source, f"mkdir -p {src}/project/data")
        _, seconds = ssh(source, f"python3 -c {shlex.quote(DATASET)} {src}/project/data {options.small} {options.large}")
        size, _ = ssh(source, f"du -sb {src}/project/data | cut -f1")
        result["dataset"] = {"bytes": int(size), "small_files": options.small, "large_files": options.large, "large_file_bytes": 512 << 20, "generate_seconds": round(seconds, 1)}

        # Temporary key for rsync and tar. odin's Dropbear rejects a from= option. Cleanup removes the key.
        key = base[SERVER] + "/key"
        ssh(SERVER, f"ssh-keygen -q -t ed25519 -N '' -C {marker} -f {key}")
        public, _ = ssh(SERVER, f"cat {key}.pub")
        ssh(ODIN, f"umask 077; mkdir -p ~/.ssh; echo {shlex.quote(public.strip())} >> ~/.ssh/authorized_keys")
        remote_shell = f"ssh -i {key} -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile={base[SERVER]}/known_hosts"
        cipher, _ = ssh(SERVER, f"{remote_shell} -v {ODIN} true 2>&1 | grep -m1 'cipher:' || true")
        result["ssh_cipher"] = cipher.strip()

        def case(tool, name, command):
            _, seconds = ssh(SERVER, command)
            entry = {"tool": tool, "case": name, "seconds": round(seconds, 2)}
            result["cases"].append(entry)
            print(json.dumps(entry), flush=True)
            return entry

        # tar over SSH is the ceiling for a full copy through one encrypted stream.
        if options.skip_baseline:
            pass
        elif source == SERVER:
            case("tar+ssh", "full", f"tar -C {src}/project -cf - data | {remote_shell} {ODIN} 'mkdir -p {dst}/tar && tar -C {dst}/tar -xf -'")
            rsync = f"rsync -a --delete -e {shlex.quote(remote_shell)} {src}/project/data/ {ODIN}:{dst}/rsync/"
        else:
            case("tar+ssh", "full", f"mkdir -p {dst}/tar && {remote_shell} {ODIN} 'tar -C {src}/project -cf - data' | tar -C {dst}/tar -xf -")
            rsync = f"rsync -a --delete -e {shlex.quote(remote_shell)} {ODIN}:{src}/project/data/ {dst}/rsync/"
        if not options.skip_baseline:
            ssh(target, f"rm -rf {dst}/tar", sudo=privileged(target))
            case("rsync+ssh", "full", rsync)
            ssh(source, f"cp -a {src}/project/data {src}/pristine")
            for _ in range(options.repeat):
                case("rsync+ssh", "unchanged", rsync)
                ssh(source, f"python3 -c {shlex.quote(CHANGE)} {src}/project/data")
                case("rsync+ssh", "small change", rsync)
            ssh(target, f"rm -rf {dst}/rsync", sudo=privileged(target))
            ssh(source, f"rm -rf {src}/project/data && mv {src}/pristine {src}/project/data")

        # Synod: the controller runs on server. Each host runs an agent. The target holds the repository.
        synod = {host: base[host] + "/synod" for host in (SERVER, ODIN)}
        for host, arch in [(SERVER, "amd64"), (ODIN, "arm64")]:
            binary = (options.binaries / f"synod-linux-{arch}").read_bytes()
            ssh(host, f"sh -c 'cat > {synod[host]} && chmod 755 {synod[host]}'", data=binary, sudo=privileged(host))
        controller = base[SERVER] + "/controller"
        ssh(SERVER, f"{synod[SERVER]} server init --dir {controller} --address {SERVER_IP} --systemd-dir {base[SERVER]}/none >/dev/null")
        ssh(SERVER, f"(setsid {synod[SERVER]} server --config {controller}/controller.json --listen 0.0.0.0:18443 --agent-listen 0.0.0.0:18444 >> {base[SERVER]}/controller.log 2>&1 < /dev/null &)")
        cli = f"{synod[SERVER]} --json --api-url https://{SERVER_IP}:18443 --ca-file {controller}/api-ca.crt --token-file {controller}/admin.token"

        def api(*arguments, timeout=1800):
            out, _ = ssh(SERVER, cli + " " + " ".join(shlex.quote(a) for a in arguments), timeout=timeout)
            return json.loads(out)

        def wait(check, message, seconds=120):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                try:
                    value = check()
                except RuntimeError:
                    value = None
                if value:
                    return value
                time.sleep(1)
            raise RuntimeError(message)

        wait(lambda: api("installation", "get"), "controller did not start")
        ids = {}
        for host in (SERVER, ODIN):
            token = api("node", "join-token")["join_token"]
            ssh(host, f"{synod[host]} agent join {token} --dir {base[host]}/agent --systemd-dir {base[host]}/none --destination-listen 0.0.0.0:18445 --destination-address {ip[host]}:18445", sudo=privileged(host))
            node, _ = ssh(host, f"cat {base[host]}/agent/node-id", sudo=privileged(host))
            ids[host] = node.strip()
            ssh(host, f"sh -c '(setsid {synod[host]} agent run --config {base[host]}/agent/agent.json >> {base[host]}/agent.log 2>&1 < /dev/null &)'", sudo=privileged(host))
        wait(lambda: sum(1 for n in api("node", "list").get("nodes", []) if n.get("architecture")) == 2, "agents did not connect")
        for node in ids.values():
            api("node", "approve", node)
        compose = f"services:\n  app:\n    image: {IMAGE}\n    network_mode: none\n    command: [sh, -c, \"trap 'exit 0' TERM; sleep 86400 & wait\"]\n    volumes: ['./data:/data']\n"
        ssh(source, f"cat > {src}/project/compose.yaml", data=compose.encode())
        container = marker
        docker = "sudo -n docker" if privileged(source) else "docker"
        ssh(source, f"{docker} create --name {container} --network none --label synod.bench={marker} "
                    f"--label com.docker.compose.project={container} --label com.docker.compose.service=app "
                    f"--label com.docker.compose.project.working_dir={src}/project "
                    f"--label com.docker.compose.project.config_files={src}/project/compose.yaml "
                    f"--mount type=bind,src={src}/project/data,dst=/data {IMAGE} sh -c {STOPPABLE} >/dev/null && {docker} start {container} >/dev/null")
        app = wait(lambda: next((a for a in api("app", "list").get("apps", []) if a["definition"]["name"] == container), None), "app was not discovered")
        large = [p for p in app["definition"].get("paths", []) if p["status"] != "Included"]
        if large:
            raise RuntimeError(f"benchmark data is not fully included: {large}")
        ssh(SERVER, f"umask 077; cat > {base[SERVER]}/password.json", data=json.dumps({"password": secrets.token_hex(24)}).encode())
        if options.destination == "device":
            api("destination", "add", "bench", "--type", "device", "--device", ids[target], "--path", f"{dst}/repository", "--credentials-file", f"{base[SERVER]}/password.json")
        else:
            target, dst = source, src
            api("destination", "add", "bench", "--type", "local", "--device", ids[source], "--path", f"{src}/repository", "--credentials-file", f"{base[SERVER]}/password.json")
        wait(lambda: all(d["current"] for d in api("destination", "list")["destinations"]["delivery"]), "settings were not delivered")

        def backup(name, method):
            started = time.monotonic()
            op = api("backup", app["id"], "--destination", "bench", "--method", method)["operation"]
            seconds = time.monotonic() - started
            if op["status"] != "succeeded":
                raise RuntimeError(f"synod {name} failed: {op.get('failure_code')}")
            r = op["result"]
            agent = (time.mktime(time.strptime(r["completed_at"][:19], "%Y-%m-%dT%H:%M:%S")) - time.mktime(time.strptime(r["started_at"][:19], "%Y-%m-%dT%H:%M:%S")))
            entry = {"tool": f"synod {method}", "case": name, "seconds": round(seconds, 2), "agent_seconds": agent, "downtime_ms": r.get("downtime_ms", 0), "created_at": op.get("created_at", ""), "started_at": r["started_at"], "completed_at": r["completed_at"]}
            result["cases"].append(entry)
            print(json.dumps(entry), flush=True)

        backup("full", "live")
        for _ in range(options.repeat):
            backup("unchanged", "live")
            ssh(source, f"python3 -c {shlex.quote(CHANGE)} {src}/project/data")
            backup("small change", "live")
        for _ in range(options.repeat):
            backup("unchanged", "stop")
        result["operations"] = [{k: o.get(k) for k in ("kind", "node_id", "status", "failure_code", "created_at", "updated_at")} for o in api("operation", "list").get("operations", [])]
        repo, _ = ssh(target, f"du -sb {dst}/repository | cut -f1", sudo=privileged(target))
        result["synod_repository_bytes"] = int(repo)
        result["passed"] = True
    except (RuntimeError, subprocess.TimeoutExpired, KeyError, ValueError) as error:
        result["failure"] = str(error)
    finally:
        for host, path in [(SERVER, base[SERVER] + "/agent.log"), (SERVER, base[SERVER] + "/controller.log"), (ODIN, base[ODIN] + "/agent.log")]:
            try:
                log, _ = ssh(host, f"cat {path}", sudo=privileged(host), timeout=60)
                name = ("server-" if host == SERVER else "odin-") + pathlib.Path(path).name
                options.evidence.with_suffix("." + name).write_text(log)
            except (RuntimeError, subprocess.TimeoutExpired):
                pass
        steps = []
        if container:
            steps.append((source, f"docker rm -f {container} >/dev/null", privileged(source)))
        steps += [
            (SERVER, "pkill -x synod -U $(id -u) || true", False),
            (ODIN, "pkill -x synod || true", True),
            (ODIN, f"sed -i '/{marker}/d' ~/.ssh/authorized_keys", False),
            (ODIN, f"rm -rf {base[ODIN]}", True),
            (SERVER, f"rm -rf {base[SERVER]}", False),
        ]
        for host, command, sudo in steps:
            try:
                ssh(host, command, sudo=sudo, timeout=120)
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                result["cleanup_errors"].append(str(error))
        summary = {}
        for c in result["cases"]:
            summary.setdefault(c["tool"] + " / " + c["case"], []).append(c["seconds"])
        result["summary"] = {k: {"median": sorted(v)[len(v) // 2], "runs": v} for k, v in summary.items()}
        options.evidence.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({k: v for k, v in result.items() if k not in ("cases", "operations")}, indent=2))
    if not result.get("passed") or result["cleanup_errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
