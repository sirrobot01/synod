"""Read native startup evidence without changing the member or its leadership."""

import base64
import http.client
import json
import os
import pwd
import signal
import ssl
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "/run/synod-patroni")

def main():
    from application import connect
    from guard import COMPLETE, IDENTITY, OUTSIDE, PRIVATE, Refused, interrupted, native, record

    if len(sys.argv) != 1 or os.geteuid() != pwd.getpwnam("postgres").pw_uid:
        raise Refused("health invocation refused")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGALRM, interrupted)
    signal.alarm(20)
    config = record(PRIVATE / "config.json")
    settings = config["synod"]
    completed = record(OUTSIDE / "complete.json")
    if completed is None or completed != record(COMPLETE):
        raise Refused("health completion refused")
    identity = completed["identity"]
    attempt = record(OUTSIDE / "attempt.json")
    if (record(IDENTITY) != identity or record(OUTSIDE / "identity.json") != identity
            or attempt is None or attempt.get("identity") != identity or attempt.get("method") not in ("init", "clone")):
        raise Refused("health storage identity refused")
    # The fixed verifier checks native storage, SQL receipts, and application access.
    native(["/usr/bin/python3", str(PRIVATE / "application.py"), "verify"], 10)
    address, port = config["restapi"]["connect_address"].rsplit(":", 1)
    address = address.strip("[]")
    tls = ssl.create_default_context(cafile=str(PRIVATE / "ca.crt"))
    tls.minimum_version = ssl.TLSVersion.TLSv1_2
    tls.load_cert_chain(str(PRIVATE / "member.crt"), str(PRIVATE / "member.key"))
    connection = http.client.HTTPSConnection(address, int(port), timeout=2, context=tls)
    try:
        password = config["restapi"]["authentication"]["password"]
        authorization = base64.b64encode(("synod_patroni:" + password).encode("ascii")).decode("ascii")
        connection.request("GET", "/patroni", headers={"Authorization": "Basic " + authorization, "Accept": "application/json"})
        response = connection.getresponse()
        data = response.read(65537)
        if response.status != 200 or len(data) > 65536:
            raise Refused("health native status refused")
        status = json.loads(data)
    finally:
        connection.close()
    if (status.get("state") != "running" or status.get("role") not in ("primary", "replica")
            or status.get("database_system_identifier") != completed["system_id"]
            or status.get("patroni") != {"version": "4.1.5", "scope": settings["cluster_id"], "name": settings["resource_id"]}):
        raise Refused("health native identity refused")
    database = connect(config, "postgres", True)
    try:
        with database.cursor() as cursor:
            cursor.execute("SET statement_timeout='3000'")
            cursor.execute("SELECT current_user='synod_admin' AND session_user='synod_admin', "
                           "pg_catalog.pg_is_in_recovery(), system_identifier::text FROM pg_catalog.pg_control_system()")
            authorized, recovery, identity = cursor.fetchone()
            if not authorized or recovery != (status["role"] == "replica") or identity != completed["system_id"]:
                raise Refused("health native role refused")
    finally:
        database.close()
    print("patroni_ready", end="")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("patroni_startup_unknown", end="")
        sys.exit(1)
