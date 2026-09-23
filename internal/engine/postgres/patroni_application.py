"""Fixed application bootstrap and read-only verification for Patroni members."""

import hashlib
import os
import pwd
import signal
import sys
import uuid

sys.dont_write_bytecode = True

import psycopg2
from psycopg2.extras import Json

from guard import COMPLETE, DATA, OUTSIDE, PRIVATE, Refused, interrupted, persist, record, system_id


def connect(config, database, readonly):
    host, port = config["postgresql"]["connect_address"].rsplit(":", 1)
    options = ("-c search_path=pg_catalog -c statement_timeout=20000 -c lock_timeout=5000"
               " -c log_statement=none -c log_min_error_statement=panic"
               " -c log_parameter_max_length_on_error=0")
    options += " -c default_transaction_read_only=on" if readonly else " -c synchronous_commit=local"
    connection = psycopg2.connect(
        host=host.strip("[]"), port=int(port), dbname=database, user="synod_admin",
        password=config["postgresql"]["authentication"]["superuser"]["password"],
        sslmode="verify-full", sslrootcert=str(PRIVATE / "ca.crt"),
        connect_timeout=5, options=options,
    )
    connection.autocommit = True
    return connection


def verify(connection, config, expected):
    with connection.cursor() as cursor:
        cursor.execute("""SELECT n.oid, c.oid FROM pg_catalog.pg_namespace n
            JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid
            JOIN pg_catalog.pg_roles r ON r.rolname='synod_admin'
            WHERE n.nspname='synod_control' AND n.nspowner=r.oid
            AND c.relname='application' AND c.relkind='r' AND c.relowner=r.oid
            AND NOT c.relrowsecurity
            AND NOT EXISTS (SELECT FROM pg_catalog.aclexplode(
                COALESCE(n.nspacl,pg_catalog.acldefault('n',r.oid))) a WHERE a.grantee<>r.oid)
            AND NOT EXISTS (SELECT FROM pg_catalog.aclexplode(
                COALESCE(c.relacl,pg_catalog.acldefault('r',r.oid))) a WHERE a.grantee<>r.oid)""")
        if cursor.fetchone() is None:
            raise Refused("application receipt access mismatch")
        cursor.execute("SELECT identity, admin_oid, app_oid, database_oid, completed FROM synod_control.application")
        rows = cursor.fetchall()
        if len(rows) != 1 or rows[0][0] != expected or rows[0][4] is not True:
            raise Refused("application receipt identity mismatch")
        _, admin, app, database, _ = rows[0]
        cursor.execute("""SELECT r.oid=%s AND r.rolpassword=%s AND r.rolcanlogin
            AND NOT r.rolsuper AND NOT r.rolcreatedb AND NOT r.rolcreaterole
            AND NOT r.rolinherit AND NOT r.rolreplication AND NOT r.rolbypassrls
            AND r.rolvaliduntil IS NULL AND r.rolconnlimit=-1
            AND NOT EXISTS (SELECT FROM pg_catalog.pg_db_role_setting WHERE setrole=r.oid)
            AND NOT EXISTS (SELECT FROM pg_catalog.pg_auth_members WHERE member=r.oid OR roleid=r.oid)
            AND d.oid=%s AND d.datdba=r.oid AND d.datallowconn
            AND NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE datdba=r.oid AND datname<>'app')
            AND pg_catalog.has_database_privilege(r.oid,d.oid,'CONNECT')
            AND pg_catalog.has_database_privilege(r.oid,d.oid,'CREATE')
            AND pg_catalog.has_database_privilege(r.oid,d.oid,'TEMP')
            AND NOT EXISTS (SELECT FROM pg_catalog.pg_database other
                WHERE other.datname IN ('postgres','template1') AND
                (pg_catalog.has_database_privilege(r.oid,other.oid,'CONNECT')
                OR pg_catalog.has_database_privilege(r.oid,other.oid,'TEMP')))
            AND NOT EXISTS (SELECT FROM pg_catalog.aclexplode(
                COALESCE(d.datacl,pg_catalog.acldefault('d',d.datdba))) a
                WHERE a.grantee NOT IN (%s,r.oid))
            AND EXISTS (SELECT FROM pg_catalog.pg_authid WHERE oid=%s AND rolname='synod_admin' AND rolsuper)
            FROM pg_catalog.pg_authid r CROSS JOIN pg_catalog.pg_database d
            WHERE r.rolname='synod_app' AND d.datname='app'""",
            (app, config["synod"]["application_verifier"], database, admin, admin))
        if cursor.fetchone() != (True,):
            raise Refused("application role or database mismatch")
    application = connect(config, "app", True)
    try:
        with application.cursor() as cursor:
            cursor.execute("""SELECT pg_catalog.has_schema_privilege('synod_app','public','USAGE')
                AND pg_catalog.has_schema_privilege('synod_app','public','CREATE')
                AND NOT EXISTS (SELECT FROM pg_catalog.pg_namespace n,
                    LATERAL pg_catalog.aclexplode(COALESCE(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) a
                    WHERE n.nspname='public' AND a.grantee=0)""")
            if cursor.fetchone() != (True,):
                raise Refused("application schema access mismatch")
    finally:
        application.close()


def main():
    os.umask(0o077)
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGALRM, interrupted)
    signal.alarm(90)
    if len(sys.argv) < 2 or sys.argv[1] not in ("bootstrap", "verify"):
        raise Refused("application action refused")
    bootstrap = sys.argv[1] == "bootstrap"
    if len(sys.argv) > (3 if bootstrap else 2) or os.geteuid() != pwd.getpwnam("postgres").pw_uid:
        raise Refused("application invocation refused")
    config = record(PRIVATE / "config.json")
    settings = config["synod"]
    credential = str(uuid.UUID(settings["application_credential_id"]))
    if credential != settings["application_credential_id"] or uuid.UUID(credential).int == 0:
        raise Refused("application credential identity refused")
    version = settings["application_credential_version"]
    if type(version) is not int or version <= 0:
        raise Refused("application credential version refused")
    completed = record(OUTSIDE / "complete.json")
    identity = {key: settings[key] for key in (
        "installation_id", "cluster_id", "resource_id", "volume_id", "provision_id")}
    identity["format"] = 1
    if completed is None or completed != record(COMPLETE) or completed.get("identity") != identity:
        raise Refused("application storage identity refused")
    if completed.get("system_id") != system_id(DATA):
        raise Refused("application native storage mismatch")
    expected = {
        "format": 1, "installation_id": settings["installation_id"], "cluster_id": settings["cluster_id"],
        "application_credential_id": credential, "application_credential_version": version,
        "verifier_fingerprint": hashlib.sha256(settings["application_verifier"].encode("ascii")).hexdigest(),
        "system_id": completed["system_id"],
    }
    connection = connect(config, "postgres", not bootstrap)
    try:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT current_user='synod_admin' AND session_user='synod_admin',
                pg_catalog.pg_is_in_recovery(), system_identifier::text FROM pg_catalog.pg_control_system()""")
            authorized, recovery, native_id = cursor.fetchone()
            if not authorized or native_id != expected["system_id"] or bootstrap and recovery:
                raise Refused("application native identity refused")
            cursor.execute("SELECT pg_catalog.to_regnamespace('synod_control') IS NOT NULL")
            existing = cursor.fetchone()[0]
            if not bootstrap or existing:
                verify(connection, config, expected)
                return
            if record(OUTSIDE / "application-attempt.json") is not None:
                raise Refused("application setup already attempted")
            attempt = record(OUTSIDE / "attempt.json")
            if attempt != {"identity": identity, "method": "init"}:
                raise Refused("application bootstrap requires initial member")
            persist(OUTSIDE / "application-attempt.json", expected)
            cursor.execute("SELECT pg_catalog.pg_try_advisory_lock(1937337956,90002)")
            if cursor.fetchone() != (True,):
                raise Refused("application setup is busy")
            cursor.execute("""SELECT EXISTS (SELECT FROM pg_catalog.pg_authid WHERE rolname='synod_app')
                OR EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname='app')""")
            if cursor.fetchone() != (False,):
                raise Refused("application objects already exist")
            cursor.execute("BEGIN")
            cursor.execute("CREATE SCHEMA synod_control AUTHORIZATION synod_admin")
            cursor.execute("REVOKE ALL ON SCHEMA synod_control FROM PUBLIC")
            cursor.execute("""CREATE TABLE synod_control.application (
                singleton boolean PRIMARY KEY CHECK(singleton), identity jsonb NOT NULL,
                admin_oid oid NOT NULL, app_oid oid NOT NULL, database_oid oid,
                completed boolean NOT NULL DEFAULT false)""")
            cursor.execute("REVOKE ALL ON TABLE synod_control.application FROM PUBLIC")
            cursor.execute("""CREATE ROLE synod_app NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %s""", (settings["application_verifier"],))
            # Patroni enables this role after the hook. Reserve its required database access now.
            cursor.execute("CREATE ROLE synod_rewind NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")
            cursor.execute("GRANT CONNECT ON DATABASE postgres TO synod_rewind")
            cursor.execute("""INSERT INTO synod_control.application(singleton,identity,admin_oid,app_oid)
                SELECT true,%s,a.oid,r.oid FROM pg_catalog.pg_authid a, pg_catalog.pg_authid r
                WHERE a.rolname='synod_admin' AND r.rolname='synod_app'""", (Json(expected),))
            cursor.execute("COMMIT")
            cursor.execute("CREATE DATABASE app OWNER synod_app TEMPLATE template0")
            application = connect(config, "app", False)
            try:
                with application.cursor() as app_cursor:
                    app_cursor.execute("REVOKE ALL ON SCHEMA public FROM PUBLIC")
                    app_cursor.execute("GRANT USAGE,CREATE ON SCHEMA public TO synod_app")
            finally:
                application.close()
            cursor.execute("BEGIN")
            cursor.execute("REVOKE ALL ON DATABASE app FROM PUBLIC")
            cursor.execute("GRANT CONNECT,TEMP ON DATABASE app TO synod_app")
            cursor.execute("REVOKE CONNECT,TEMP ON DATABASE postgres FROM PUBLIC")
            cursor.execute("REVOKE CONNECT,TEMP ON DATABASE template1 FROM PUBLIC")
            cursor.execute("ALTER ROLE synod_app LOGIN")
            cursor.execute("""UPDATE synod_control.application SET completed=true,
                database_oid=(SELECT oid FROM pg_catalog.pg_database WHERE datname='app')""")
            cursor.execute("COMMIT")
        verify(connection, config, expected)
    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except Refused as error:
        print("Synod Patroni application guard: " + str(error), file=sys.stderr)
        sys.exit(1)
    except Exception:
        print("Synod Patroni application guard: setup or verification failed", file=sys.stderr)
        sys.exit(1)
