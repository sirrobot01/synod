package postgres

import (
	"errors"
	"strconv"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

const ConfigDirectory = "/run/synod-postgres"
const BootstrapCompletePath = ConfigDirectory + "/bootstrap-complete"
const InitializationDirectory = "/var/lib/synod-postgres"
const BootstrapFailurePath = InitializationDirectory + "/bootstrap-failed"

// ContainerCommand prepares application access before PostgreSQL accepts clients.
func ContainerCommand(config *agentv1.PostgresContainerConfig, installationID, clusterID, resourceID string) []string {
	return []string{"/bin/bash", "-ec", postgresStartup, "synod-postgres",
		strconv.FormatUint(uint64(config.Port), 10), installationID, clusterID, resourceID,
		config.SuperuserCredentialId, strconv.FormatInt(config.SuperuserCredentialVersion, 10),
		config.ApplicationCredentialId, strconv.FormatInt(config.ApplicationCredentialVersion, 10),
		config.ReplicationCredentialId, strconv.FormatInt(config.ReplicationCredentialVersion, 10), strconv.FormatBool(config.SynchronousReplication)}
}

// RecoveryCommand verifies or finishes an existing bootstrap receipt after confirmed failure.
func RecoveryCommand(config *agentv1.PostgresContainerConfig, installationID, clusterID, resourceID, commandID string) ([]string, error) {
	id, err := uuid.Parse(commandID)
	if err != nil || id == uuid.Nil || id.String() != commandID {
		return nil, errors.New("invalid recovery command ID")
	}
	return append(ContainerCommand(config, installationID, clusterID, resourceID), commandID), nil
}

// The program is fixed engine code. Its arguments contain only validated identities.
const postgresStartup = `
set -Ee -o pipefail
umask 077
source /usr/local/bin/docker-entrypoint.sh
export PGDATA=/var/lib/postgresql/data POSTGRES_USER=synod_admin POSTGRES_DB=postgres
export PGPORT="$1" SYNOD_INSTALLATION="$2" SYNOD_CLUSTER="$3" SYNOD_RESOURCE="$4"
export SYNOD_ADMIN_ID="$5" SYNOD_ADMIN_VERSION="$6" SYNOD_APP_ID="$7" SYNOD_APP_VERSION="$8"
export SYNOD_REPL_ID="$9" SYNOD_REPL_VERSION="${10}"
export SYNOD_SYNCHRONOUS="${11}" SYNOD_RECOVERY="${12:-}"
synchronous_standbys=''
case "$SYNOD_SYNCHRONOUS" in
    true) synchronous_standbys="ANY 1 (\"synod_$SYNOD_CLUSTER\")" ;;
    false) ;;
    *) exit 1 ;;
esac
export POSTGRES_INITDB_ARGS='--auth-local=trust --auth-host=scram-sha-256'
export POSTGRES_HOST_AUTH_METHOD=scram-sha-256
if [ "$(id -u)" = 0 ]; then
    mkdir -p /run/synod-postgres /var/lib/synod-postgres
    chmod 700 /run/synod-postgres /var/lib/synod-postgres
    chown postgres:postgres /run/synod-postgres /var/lib/synod-postgres
    docker_create_db_directories >/dev/null 2>&1
    exec gosu postgres /bin/bash -ec "$BASH_EXECUTION_STRING" "$0" "$@"
fi
if [ -n "$SYNOD_RECOVERY" ]; then
    [[ "$SYNOD_RECOVERY" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]
    [ ! -L /var/lib/synod-postgres/recovery.lock ]
    exec 9>/var/lib/synod-postgres/recovery.lock
    flock -n 9
    [ "$(cat /run/synod-postgres/bootstrap-complete)" = failed ]
    [ -f /var/lib/synod-postgres/bootstrap-failed ]
    [ ! -L /var/lib/synod-postgres/bootstrap-failed ]
    [ -s "$PGDATA/PG_VERSION" ]
    [ -f "$PGDATA/PG_VERSION" ]
    [ ! -L "$PGDATA/PG_VERSION" ]
    [ ! -e "$PGDATA/postmaster.pid" ]
    [ ! -L "$PGDATA/postmaster.pid" ]
    [ ! -e "/var/lib/synod-postgres/recovery-$SYNOD_RECOVERY" ]
    [ ! -L "/var/lib/synod-postgres/recovery-$SYNOD_RECOVERY" ]
fi
rm -f /run/synod-postgres/bootstrap-complete
sql_pid=''
temporary=0
cleanup() {
    local stopped=0
    if [ "$temporary" = 1 ] || [ -s "$PGDATA/postmaster.pid" ]; then
        pg_ctl -D "$PGDATA" -m fast -w -t 30 stop >/dev/null 2>&1 || stopped=1
    fi
    if [ -n "$sql_pid" ]; then
        kill -TERM "$sql_pid" >/dev/null 2>&1 || true
        wait "$sql_pid" >/dev/null 2>&1 || true
        sql_pid=''
    fi
    [ "$stopped" = 0 ]
}
record_recovery() {
    printf '%s:%s\n' "$1" "$SYNOD_RECOVERY" > "/var/lib/synod-postgres/recovery-$SYNOD_RECOVERY.tmp" &&
    sync -f "/var/lib/synod-postgres/recovery-$SYNOD_RECOVERY.tmp" &&
    mv "/var/lib/synod-postgres/recovery-$SYNOD_RECOVERY.tmp" "/var/lib/synod-postgres/recovery-$SYNOD_RECOVERY" &&
    sync -f /var/lib/synod-postgres
}
failed() {
    trap - ERR
    if cleanup; then
        if [ ! -e /var/lib/synod-postgres/bootstrap-failed ] && [ ! -L /var/lib/synod-postgres/bootstrap-failed ]; then
            if ! { printf 'failed\n' > /var/lib/synod-postgres/bootstrap-failed.tmp &&
                sync -f /var/lib/synod-postgres/bootstrap-failed.tmp &&
                mv /var/lib/synod-postgres/bootstrap-failed.tmp /var/lib/synod-postgres/bootstrap-failed; }; then
                exit 1
            fi
        fi
        if ! { [ -f /var/lib/synod-postgres/bootstrap-failed ] &&
            [ ! -L /var/lib/synod-postgres/bootstrap-failed ] &&
            sync -f /var/lib/synod-postgres/bootstrap-failed &&
            sync -f /var/lib/synod-postgres; }; then
            exit 1
        fi
        if [ -n "$SYNOD_RECOVERY" ]; then
            record_recovery refused || exit 1
        fi
        unset POSTGRES_PASSWORD SYNOD_APP_VERIFIER SYNOD_REPL_VERIFIER PGPASSWORD
        printf 'failed\n' > /run/synod-postgres/bootstrap-complete
        if [ -n "$SYNOD_RECOVERY" ]; then
            exit 1
        fi
        trap 'exit 0' TERM INT
        while :; do sleep 3600 & wait "$!"; done
    fi
    exit 1
}
trap failed ERR
trap 'trap - ERR; cleanup; exit 0' TERM INT
if [ -n "$SYNOD_RECOVERY" ]; then
    trap failed TERM INT
elif [ -e /var/lib/synod-postgres/bootstrap-failed ] || [ -L /var/lib/synod-postgres/bootstrap-failed ]; then
    failed
fi
[ -n "${POSTGRES_PASSWORD:-}" ]
[ -n "${SYNOD_APP_VERIFIER:-}" ]
[ -n "${SYNOD_REPL_VERIFIER:-}" ]
docker_setup_env >/dev/null 2>&1
if [ ! -s "$PGDATA/PG_VERSION" ]; then
    [ -z "$SYNOD_RECOVERY" ]
    [ ! -e /var/lib/synod-postgres/initialized-attempt ]
    [ -z "$(find "$PGDATA" -mindepth 1 -maxdepth 1 -print -quit)" ]
    printf 'attempted\n' > /var/lib/synod-postgres/initialized-attempt
    sync -f /var/lib/synod-postgres/initialized-attempt
    sync -f /var/lib/synod-postgres
    docker_init_database_dir >/dev/null 2>&1
else
    [ -f "$PGDATA/PG_VERSION" ]
    [ ! -L "$PGDATA/PG_VERSION" ]
    printf 'attempted\n' > /var/lib/synod-postgres/initialized-attempt
    sync -f /var/lib/synod-postgres/initialized-attempt
fi
export PGHOST=/run/synod-postgres PGUSER=synod_admin PGDATABASE=postgres
unset PGHOSTADDR PGSERVICE PGSERVICEFILE PGOPTIONS
printf 'local all all trust\nhost replication synod_repl all scram-sha-256\nhost all synod_repl all reject\nhost all all all scram-sha-256\n' > /run/synod-postgres/pg_hba.conf
docker_temp_server_start postgres -c hba_file=/run/synod-postgres/pg_hba.conf -c unix_socket_directories=/run/synod-postgres \
    -c unix_socket_permissions=0700 -c synchronous_standby_names= -c synchronous_commit=local -c logging_collector=off -c log_statement=none \
    -c log_min_duration_statement=-1 -c log_min_duration_sample=-1 -c log_transaction_sample_rate=0 \
    -c log_min_messages=panic -c log_min_error_statement=panic \
    -c log_parameter_max_length=0 -c log_parameter_max_length_on_error=0 >/dev/null 2>&1
temporary=1
psql --no-psqlrc --no-password --quiet --set ON_ERROR_STOP=1 >/dev/null 2>&1 <<'SYNOD_SQL' &
` + bootstrapSQL + `
SYNOD_SQL
sql_pid=$!
wait "$sql_pid"
sql_pid=''
cleanup
temporary=0
unset POSTGRES_PASSWORD SYNOD_APP_VERIFIER SYNOD_REPL_VERIFIER PGPASSWORD
if [ -n "$SYNOD_RECOVERY" ]; then
    record_recovery validated
    rm /var/lib/synod-postgres/bootstrap-failed
    sync -f /var/lib/synod-postgres
    printf 'recovered\n' > /run/synod-postgres/bootstrap-complete
    exit 0
fi
printf 'ready\n' > /run/synod-postgres/bootstrap-complete
trap - ERR TERM INT
exec postgres -p "$PGPORT" -c listen_addresses=0.0.0.0 -c hba_file=/run/synod-postgres/pg_hba.conf -c unix_socket_directories=/run/synod-postgres -c unix_socket_permissions=0700 \
    -c wal_level=replica -c max_wal_senders=10 -c max_replication_slots=10 -c wal_keep_size=256MB -c max_slot_wal_keep_size=1024MB \
    -c synchronous_commit=on -c "synchronous_standby_names=$synchronous_standbys"
`

// One session holds the advisory lock through database creation and both transactions.
const bootstrapSQL = `
\getenv installation SYNOD_INSTALLATION
\getenv cluster SYNOD_CLUSTER
\getenv resource SYNOD_RESOURCE
\getenv admin_id SYNOD_ADMIN_ID
\getenv admin_version SYNOD_ADMIN_VERSION
\getenv app_id SYNOD_APP_ID
\getenv app_version SYNOD_APP_VERSION
\getenv verifier SYNOD_APP_VERIFIER
\getenv replication_id SYNOD_REPL_ID
\getenv replication_version SYNOD_REPL_VERSION
\getenv replication_verifier SYNOD_REPL_VERIFIER
\getenv recovery SYNOD_RECOVERY
SET search_path=pg_catalog;
SET statement_timeout='30s';
SET lock_timeout='10s';
SELECT pg_catalog.set_config('synod.bootstrap', pg_catalog.jsonb_build_object(
    'installation', :'installation', 'cluster', :'cluster', 'resource', :'resource',
    'admin_id', :'admin_id', 'admin_version', :'admin_version',
    'app_id', :'app_id', 'app_version', :'app_version', 'verifier', :'verifier',
    'replication_id', :'replication_id', 'replication_version', :'replication_version',
    'replication_verifier', :'replication_verifier', 'recovery', :'recovery')::text, false);
SELECT pg_catalog.pg_advisory_lock(1937337956, 90001);
BEGIN;
DO $prepare$
DECLARE
    expected jsonb := pg_catalog.current_setting('synod.bootstrap')::jsonb;
    admin_oid oid;
    app_oid oid;
    replication_oid oid;
    database_oid oid;
    namespace_oid oid;
    receipt record;
BEGIN
    SELECT oid INTO admin_oid FROM pg_catalog.pg_authid WHERE rolname='synod_admin' AND rolsuper AND rolcanlogin;
    IF admin_oid IS NULL OR session_user<>'synod_admin' OR current_user<>'synod_admin' THEN
        RAISE EXCEPTION 'bootstrap_identity_conflict';
    END IF;
    IF (SELECT datdba FROM pg_catalog.pg_database WHERE datname='postgres')<>admin_oid THEN
        RAISE EXCEPTION 'bootstrap_identity_conflict';
    END IF;
    SELECT oid INTO namespace_oid FROM pg_catalog.pg_namespace WHERE nspname='synod_control';
    IF namespace_oid IS NULL THEN
        IF expected->>'recovery'<>'' THEN
            RAISE EXCEPTION 'bootstrap_receipt_missing';
        END IF;
        IF EXISTS (SELECT FROM pg_catalog.pg_authid WHERE rolname IN ('synod_app','synod_repl')) THEN
            RAISE EXCEPTION 'bootstrap_role_conflict';
        END IF;
        IF EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname='app') THEN
            RAISE EXCEPTION 'bootstrap_database_conflict';
        END IF;
        CREATE SCHEMA synod_control AUTHORIZATION synod_admin;
        REVOKE ALL ON SCHEMA synod_control FROM PUBLIC;
        CREATE TABLE synod_control.bootstrap (
            singleton boolean PRIMARY KEY CHECK (singleton),
            installation_id uuid NOT NULL, cluster_id uuid NOT NULL, resource_id uuid NOT NULL,
            admin_credential_id uuid NOT NULL, admin_version bigint NOT NULL,
            app_credential_id uuid NOT NULL, app_version bigint NOT NULL,
            replication_credential_id uuid NOT NULL, replication_version bigint NOT NULL,
            admin_oid oid NOT NULL, app_oid oid NOT NULL, replication_oid oid NOT NULL, database_oid oid,
            verifier_fingerprint bytea NOT NULL, replication_verifier_fingerprint bytea NOT NULL,
            phase text NOT NULL CHECK (phase IN ('prepared','completed'))
        );
        REVOKE ALL ON TABLE synod_control.bootstrap FROM PUBLIC;
        EXECUTE pg_catalog.format('CREATE ROLE synod_app NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L', expected->>'verifier');
        SELECT oid INTO app_oid FROM pg_catalog.pg_authid WHERE rolname='synod_app';
        EXECUTE pg_catalog.format('CREATE ROLE synod_repl NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT REPLICATION NOBYPASSRLS PASSWORD %L', expected->>'replication_verifier');
        SELECT oid INTO replication_oid FROM pg_catalog.pg_authid WHERE rolname='synod_repl';
        INSERT INTO synod_control.bootstrap VALUES (true,
            (expected->>'installation')::uuid, (expected->>'cluster')::uuid, (expected->>'resource')::uuid,
            (expected->>'admin_id')::uuid, (expected->>'admin_version')::bigint,
            (expected->>'app_id')::uuid, (expected->>'app_version')::bigint,
            (expected->>'replication_id')::uuid, (expected->>'replication_version')::bigint,
            admin_oid, app_oid, replication_oid, database_oid,
            pg_catalog.sha256(pg_catalog.convert_to(expected->>'verifier','UTF8')),
            pg_catalog.sha256(pg_catalog.convert_to(expected->>'replication_verifier','UTF8')), 'prepared');
    END IF;
    SELECT oid INTO namespace_oid FROM pg_catalog.pg_namespace WHERE nspname='synod_control' AND nspowner=admin_oid;
    IF namespace_oid IS NULL OR EXISTS (
        SELECT FROM pg_catalog.pg_namespace n, LATERAL pg_catalog.aclexplode(COALESCE(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) a
        WHERE n.oid=namespace_oid AND a.grantee<>admin_oid
    ) OR NOT EXISTS (
        SELECT FROM pg_catalog.pg_class WHERE relnamespace=namespace_oid AND relname='bootstrap' AND relkind='r'
        AND relowner=admin_oid AND NOT relrowsecurity
    ) OR EXISTS (
        SELECT FROM pg_catalog.pg_class c, LATERAL pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
        WHERE c.relnamespace=namespace_oid AND c.relname='bootstrap' AND a.grantee<>admin_oid
    ) THEN
        RAISE EXCEPTION 'bootstrap_receipt_conflict';
    END IF;
    SELECT * INTO STRICT receipt FROM synod_control.bootstrap;
    IF receipt.phase IS NULL OR receipt.phase NOT IN ('prepared','completed')
        OR receipt.installation_id IS DISTINCT FROM (expected->>'installation')::uuid OR receipt.cluster_id IS DISTINCT FROM (expected->>'cluster')::uuid
        OR receipt.resource_id IS DISTINCT FROM (expected->>'resource')::uuid OR receipt.admin_oid IS DISTINCT FROM admin_oid
        OR receipt.admin_credential_id IS DISTINCT FROM (expected->>'admin_id')::uuid OR receipt.admin_version IS DISTINCT FROM (expected->>'admin_version')::bigint
        OR receipt.app_credential_id IS DISTINCT FROM (expected->>'app_id')::uuid OR receipt.app_version IS DISTINCT FROM (expected->>'app_version')::bigint
        OR receipt.replication_credential_id IS DISTINCT FROM (expected->>'replication_id')::uuid OR receipt.replication_version IS DISTINCT FROM (expected->>'replication_version')::bigint
        OR receipt.replication_verifier_fingerprint IS DISTINCT FROM pg_catalog.sha256(pg_catalog.convert_to(expected->>'replication_verifier','UTF8'))
        OR receipt.verifier_fingerprint IS DISTINCT FROM pg_catalog.sha256(pg_catalog.convert_to(expected->>'verifier','UTF8')) THEN
        RAISE EXCEPTION 'bootstrap_receipt_conflict';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_authid WHERE oid=receipt.app_oid AND rolname='synod_app'
        AND rolpassword=expected->>'verifier' AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole
        AND NOT rolinherit AND NOT rolreplication AND NOT rolbypassrls
        AND rolcanlogin=(receipt.phase='completed'))
        OR EXISTS (SELECT FROM pg_catalog.pg_auth_members WHERE member=receipt.app_oid)
        OR EXISTS (SELECT FROM pg_catalog.pg_database WHERE datdba=receipt.app_oid AND datname<>'app') THEN
        RAISE EXCEPTION 'bootstrap_role_conflict';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_authid WHERE oid=receipt.replication_oid AND rolname='synod_repl'
        AND rolpassword=expected->>'replication_verifier' AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole
        AND NOT rolinherit AND rolreplication AND NOT rolbypassrls AND rolvaliduntil IS NULL
        AND rolcanlogin=(receipt.phase='completed'))
        OR EXISTS (SELECT FROM pg_catalog.pg_auth_members WHERE member=receipt.replication_oid OR roleid=receipt.replication_oid)
        OR EXISTS (SELECT FROM pg_catalog.pg_database WHERE datdba=receipt.replication_oid) THEN
        RAISE EXCEPTION 'bootstrap_replication_role_conflict';
    END IF;
    IF receipt.phase='completed' THEN
        IF receipt.database_oid IS NULL OR NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE oid=receipt.database_oid AND datname='app' AND datdba=receipt.app_oid)
            OR pg_catalog.has_database_privilege('synod_app','postgres','CONNECT')
            OR pg_catalog.has_database_privilege('synod_app','postgres','TEMP')
            OR pg_catalog.has_database_privilege('synod_app','template1','CONNECT')
            OR pg_catalog.has_database_privilege('synod_app','template1','TEMP')
            OR NOT pg_catalog.has_database_privilege('synod_app','app','CONNECT')
            OR NOT pg_catalog.has_database_privilege('synod_app','app','TEMP') THEN
            RAISE EXCEPTION 'bootstrap_database_conflict';
        END IF;
    ELSIF EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname='app' AND
        (datdba<>admin_oid OR (receipt.database_oid IS NOT NULL AND oid<>receipt.database_oid)))
        OR (receipt.database_oid IS NOT NULL AND NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE oid=receipt.database_oid AND datname='app')) THEN
        RAISE EXCEPTION 'bootstrap_database_conflict';
    END IF;
END
$prepare$;
COMMIT;
SELECT 'CREATE DATABASE app OWNER synod_admin TEMPLATE template0'
WHERE EXISTS (SELECT FROM synod_control.bootstrap WHERE phase='prepared')
AND NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname='app')
\gexec
BEGIN;
DO $complete$
DECLARE receipt record;
BEGIN
    SELECT * INTO STRICT receipt FROM synod_control.bootstrap;
    IF receipt.phase='prepared' THEN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname='app' AND datdba=receipt.admin_oid
            AND (receipt.database_oid IS NULL OR oid=receipt.database_oid)) THEN
            RAISE EXCEPTION 'bootstrap_database_conflict';
        END IF;
        ALTER DATABASE app OWNER TO synod_app;
        REVOKE ALL ON DATABASE app FROM PUBLIC;
        GRANT CONNECT,TEMP ON DATABASE app TO synod_app;
        REVOKE CONNECT,TEMP ON DATABASE postgres FROM PUBLIC;
        REVOKE CONNECT,TEMP ON DATABASE template1 FROM PUBLIC;
        ALTER ROLE synod_app LOGIN;
        ALTER ROLE synod_repl LOGIN;
        UPDATE synod_control.bootstrap SET phase='completed', database_oid=(SELECT oid FROM pg_catalog.pg_database WHERE datname='app');
    END IF;
END
$complete$;
COMMIT;
SELECT pg_catalog.pg_advisory_unlock(1937337956, 90001);
`
