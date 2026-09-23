package postgres

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/netip"
	"strconv"
	"strings"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

const ReplicaDataDirectory = DataDirectory + "/replica"
const ReplicaReceiptFile = ReplicaDataDirectory + "/.synod-replica-receipt"

// ReplicaSource identifies the primary to copy and follow.
type ReplicaSource struct {
	Address    string
	Port       uint16
	ResourceID string
}

// ReplicaCommand verifies owned replica data before starting a read-only standby.
func ReplicaCommand(config *agentv1.PostgresContainerConfig, source ReplicaSource, installationID, clusterID, resourceID string) ([]string, error) {
	if config == nil || config.Port == 0 || config.Port > 65535 || source.Port == 0 ||
		config.ApplicationCredentialVersion < 1 || config.SuperuserCredentialVersion < 1 || config.ReplicationCredentialVersion < 1 ||
		config.ApplicationCredentialId == config.SuperuserCredentialId || config.ApplicationCredentialId == config.ReplicationCredentialId ||
		config.SuperuserCredentialId == config.ReplicationCredentialId || resourceID == source.ResourceID {
		return nil, errors.New("invalid replica configuration")
	}
	address, err := netip.ParseAddr(source.Address)
	if err != nil || address.Zone() != "" || address.String() != source.Address || !address.IsGlobalUnicast() && !address.IsLoopback() {
		return nil, errors.New("invalid replica source address")
	}
	for _, value := range []string{installationID, clusterID, resourceID, source.ResourceID, config.ApplicationCredentialId, config.SuperuserCredentialId, config.ReplicationCredentialId} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return nil, errors.New("invalid replica identity")
		}
	}
	command := ContainerCommand(config, installationID, clusterID, resourceID)
	command[2] = postgresReplicaStartup
	command[3] = "synod-postgres-replica"
	command = append(command, source.Address, strconv.FormatUint(uint64(source.Port), 10), source.ResourceID)
	digest := sha256.Sum256([]byte(strings.Join(command[4:], "\x00")))
	return append(command, hex.EncodeToString(digest[:])), nil
}

// CloneReplicaCommand prepares a new replica through one command-bound physical copy.
func CloneReplicaCommand(config *agentv1.PostgresContainerConfig, source ReplicaSource, installationID, clusterID, resourceID, commandID string) ([]string, error) {
	id, err := uuid.Parse(commandID)
	if err != nil || id == uuid.Nil || id.String() != commandID {
		return nil, errors.New("invalid clone command ID")
	}
	command, err := ReplicaCommand(config, source, installationID, clusterID, resourceID)
	if err != nil {
		return nil, err
	}
	return append(command, commandID), nil
}

const postgresReplicaStartup = `
set -Ee -o pipefail
umask 077
export LC_ALL=C
export PGPORT="$1" SYNOD_INSTALLATION="$2" SYNOD_CLUSTER="$3" SYNOD_RESOURCE="$4"
export SYNOD_ADMIN_ID="$5" SYNOD_ADMIN_VERSION="$6" SYNOD_APP_ID="$7" SYNOD_APP_VERSION="$8"
export SYNOD_REPL_ID="$9" SYNOD_REPL_VERSION="${10}"
export SYNOD_SYNCHRONOUS="${11}"
export SYNOD_SOURCE_HOST="${12}" SYNOD_SOURCE_PORT="${13}" SYNOD_SOURCE_RESOURCE="${14}"
export SYNOD_CONFIG_DIGEST="${15}" SYNOD_CLONE_COMMAND="${16:-}"
synchronous_standbys=''
application_name="synod_$SYNOD_RESOURCE"
case "$SYNOD_SYNCHRONOUS" in
    true)
        synchronous_standbys="ANY 1 (\"synod_$SYNOD_CLUSTER\")"
        application_name="synod_$SYNOD_CLUSTER"
        ;;
    false) ;;
    *) exit 1 ;;
esac
root=/var/lib/postgresql/data
state=/var/lib/synod-postgres
private=/run/synod-postgres
target="$root/replica"
stage="$root/.synod-clone"
export PGDATA="$target"
if [ "$(id -u)" = 0 ]; then
    mkdir -p "$root" "$state" "$private"
    chmod 700 "$root" "$state" "$private"
    chown postgres:postgres "$root" "$state" "$private"
    exec gosu postgres /bin/bash -ec "$BASH_EXECUTION_STRING" "$0" "$@"
fi
inert() {
    unset SYNOD_APP_VERIFIER SYNOD_REPL_VERIFIER SYNOD_REPL_PASSWORD PGPASSWORD POSTGRES_PASSWORD
    trap - ERR TERM INT
    exec sleep infinity
}
work_pid=''
temporary=0
failure_stage=configuration
cleanup() {
    local stopped=0
    if [ -n "$work_pid" ]; then
        kill -TERM "$work_pid" >/dev/null 2>&1 || true
        wait "$work_pid" >/dev/null 2>&1 || true
        work_pid=''
    fi
    if [ "$temporary" = 1 ]; then
        pg_ctl -D "$PGDATA" -m fast -w -t 30 stop >/dev/null 2>&1 || stopped=1
    fi
    [ "$stopped" = 0 ]
}
failed() {
    trap - ERR TERM INT
    if ! cleanup; then
        exit 1
    fi
    printf 'failed:%s\n' "$failure_stage" > "$state/replica-failed"
    printf 'replica_%s_failed\n' "$failure_stage" >&2
    sync -f "$state/replica-failed"
    sync -f "$state"
    printf 'failed\n' > "$private/bootstrap-complete"
    if [ -n "$SYNOD_CLONE_COMMAND" ]; then
        exit 1
    fi
    inert
}
[ ! -L "$state/replica.lock" ]
exec 9>"$state/replica.lock"
flock -n 9
trap failed ERR TERM INT
[ ! -L "$root" ]
[ -d "$root" ]
if [ -e "$state/replica-failed" ] || [ -L "$state/replica-failed" ]; then
    failed
fi
if [ ! -e "$state/replica-config" ] && [ ! -L "$state/replica-config" ]; then
    [ -z "$SYNOD_CLONE_COMMAND" ]
    [ -z "$(find "$root" -mindepth 1 -maxdepth 1 -print -quit)" ]
    printf '%s\n' "$SYNOD_CONFIG_DIGEST" > "$state/replica-config"
    sync -f "$state/replica-config"
    sync -f "$state"
fi
[ -f "$state/replica-config" ]
[ ! -L "$state/replica-config" ]
cmp -s <(printf '%s\n' "$SYNOD_CONFIG_DIGEST") "$state/replica-config"
if [ -n "$SYNOD_CLONE_COMMAND" ]; then
    [ "$(cat "$private/bootstrap-complete")" = clone_pending ]
    [ ! -e "$state/replica-attempt" ]
    [ ! -L "$state/replica-attempt" ]
    [ -z "$(find "$root" -mindepth 1 -maxdepth 1 -print -quit)" ]
    printf 'synod-clone-v1\n%s\n%s\n' "$SYNOD_CONFIG_DIGEST" "$SYNOD_CLONE_COMMAND" > "$state/replica-attempt"
    sync -f "$state/replica-attempt"
    sync -f "$state"
    cp "$state/replica-attempt" "$root/.synod-clone-intent"
    sync -f "$root/.synod-clone-intent"
    sync -f "$root"
    mkdir "$stage"
elif [ ! -e "$target" ] && [ ! -L "$target" ]; then
    [ ! -e "$state/replica-attempt" ]
    [ ! -L "$state/replica-attempt" ]
    if [ -z "$(find "$root" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
        printf 'clone_pending\n' > "$private/bootstrap-complete"
        flock -u 9
        inert
    fi
    failed
fi
rm -f "$private/bootstrap-complete"
[ -n "${SYNOD_APP_VERIFIER:-}" ]
[ -n "${SYNOD_REPL_VERIFIER:-}" ]
[ -n "${SYNOD_REPL_PASSWORD:-}" ]
[[ ! "$SYNOD_REPL_PASSWORD" =~ [^[:print:]] ]]
[ "${#SYNOD_REPL_PASSWORD}" -le 1024 ]
# The server needs this file after startup secrets leave its environment.
password=$(printf '%s' "$SYNOD_REPL_PASSWORD" | sed 's/\\/\\\\/g; s/:/\\:/g')
printf '*:*:replication:synod_repl:%s\n' "$password" > "$private/replication.pass"
chmod 600 "$private/replication.pass"
unset password PGPASSWORD PGHOST PGHOSTADDR PGUSER PGDATABASE PGSERVICE PGSERVICEFILE PGOPTIONS
export PGPASSFILE="$private/replication.pass" PGCONNECT_TIMEOUT=5
printf 'local all all trust\nhost replication all all reject\nhost all synod_repl all reject\nhost all all all scram-sha-256\n' > "$private/pg_hba.conf"
cat > "$private/postgresql.conf" <<SYNOD_CONFIG
port=$PGPORT
listen_addresses=''
unix_socket_directories='$private'
unix_socket_permissions=0700
hba_file='$private/pg_hba.conf'
hot_standby=on
wal_level=replica
max_wal_senders=10
max_replication_slots=10
wal_keep_size=256MB
max_slot_wal_keep_size=1024MB
synchronous_commit=on
synchronous_standby_names='$synchronous_standbys'
primary_conninfo='host=$SYNOD_SOURCE_HOST hostaddr=$SYNOD_SOURCE_HOST port=$SYNOD_SOURCE_PORT user=synod_repl passfile=$private/replication.pass application_name=$application_name connect_timeout=5'
logging_collector=off
log_statement=none
log_min_duration_statement=-1
log_min_duration_sample=-1
log_transaction_sample_rate=0
log_min_messages=panic
log_min_error_statement=panic
log_parameter_max_length=0
log_parameter_max_length_on_error=0
SYNOD_CONFIG
if [ -n "$SYNOD_CLONE_COMMAND" ]; then
    failure_stage=copy
    pg_basebackup -h "$SYNOD_SOURCE_HOST" -p "$SYNOD_SOURCE_PORT" -U synod_repl -D "$stage/archives" \
        -F tar -X stream --checkpoint=fast --no-password --manifest-checksums=SHA256 --no-clean >/dev/null 2>&1 &
    work_pid=$!
    wait "$work_pid"
    work_pid=''
    failure_stage=archive
    for archive in "$stage/archives/"*; do
        case "$archive" in
            "$stage/archives/base.tar"|"$stage/archives/pg_wal.tar"|"$stage/archives/backup_manifest") ;;
            *) false ;;
        esac
        [ -f "$archive" ]
        [ ! -L "$archive" ]
    done
    mkdir "$stage/data"
    for archive in base.tar pg_wal.tar; do
        tar --list --file="$stage/archives/$archive" > "$stage/names"
        while IFS= read -r entry; do
            case "$entry" in /*|..|../*|*/../*|*/..) false ;; esac
        done < "$stage/names"
        tar --list --verbose --file="$stage/archives/$archive" > "$stage/types"
        while IFS= read -r entry; do
            case "$entry" in -*|d*) ;; *) false ;; esac
        done < "$stage/types"
        destination="$stage/data"
        if [ "$archive" = pg_wal.tar ]; then destination="$destination/pg_wal"; fi
        [ -d "$destination" ]
        [ ! -L "$destination" ]
        tar --extract --file="$stage/archives/$archive" --directory="$destination" --no-same-owner --no-same-permissions --keep-old-files
    done
    cp "$stage/archives/backup_manifest" "$stage/data/backup_manifest"
    failure_stage=backup_verification
    pg_verifybackup "$stage/data" >/dev/null 2>&1 &
    work_pid=$!
    wait "$work_pid"
    work_pid=''
    [ ! -e "$stage/data/.synod-replica-receipt" ]
    [ ! -L "$stage/data/.synod-replica-receipt" ]
    printf '# Synod replica configuration\n' > "$stage/data/postgresql.auto.conf"
    touch "$stage/data/standby.signal"
    export PGDATA="$stage/data"
else
    [ -d "$target" ]
    [ ! -L "$target" ]
    [ -f "$target/.synod-replica-receipt" ]
    [ ! -L "$target/.synod-replica-receipt" ]
    mapfile -t receipt < "$target/.synod-replica-receipt"
    [ "${#receipt[@]}" = 4 ]
    [ "${receipt[0]}" = synod-replica-v1 ]
    [ "${receipt[1]}" = "$SYNOD_CONFIG_DIGEST" ]
    [[ "${receipt[2]}" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]
    [[ "${receipt[3]}" =~ ^[1-9][0-9]{0,19}$ ]]
    cmp -s <(printf 'synod-replica-v1\n%s\n%s\n%s\n' "$SYNOD_CONFIG_DIGEST" "${receipt[2]}" "${receipt[3]}") "$target/.synod-replica-receipt"
    for record in "$state/replica-attempt" "$root/.synod-clone-intent"; do
        [ -f "$record" ]
        [ ! -L "$record" ]
        cmp -s <(printf 'synod-clone-v1\n%s\n%s\n' "$SYNOD_CONFIG_DIGEST" "${receipt[2]}") "$record"
    done
fi
[ -f "$PGDATA/PG_VERSION" ]
[ -s "$PGDATA/PG_VERSION" ]
[ ! -L "$PGDATA/PG_VERSION" ]
[ -f "$PGDATA/standby.signal" ]
[ ! -L "$PGDATA/standby.signal" ]
[ ! -e "$PGDATA/recovery.signal" ]
[ ! -L "$PGDATA/recovery.signal" ]
[ ! -L "$PGDATA/postmaster.pid" ]
if [ -n "$SYNOD_CLONE_COMMAND" ]; then [ ! -e "$PGDATA/postmaster.pid" ]; fi
[ -f "$PGDATA/postgresql.auto.conf" ]
[ ! -L "$PGDATA/postgresql.auto.conf" ]
cmp -s <(printf '# Synod replica configuration\n') "$PGDATA/postgresql.auto.conf"
control=$(pg_controldata "$PGDATA" 2>/dev/null)
system_id=''
while IFS= read -r line; do
    case "$line" in
        'Database system identifier:'*) system_id=$(printf '%s' "$line" | cut -d: -f2 | tr -d ' ') ;;
    esac
done <<< "$control"
[[ "$system_id" =~ ^[1-9][0-9]{0,19}$ ]]
if [ -z "$SYNOD_CLONE_COMMAND" ]; then
    [ "$system_id" = "${receipt[3]}" ]
fi
failure_stage=native_verification
temporary=1
pg_ctl -D "$PGDATA" -w -t 30 -o "-c config_file=$private/postgresql.conf" start >/dev/null 2>&1
export PGHOST="$private" PGUSER=synod_admin PGDATABASE=postgres
psql --no-psqlrc --no-password --quiet --set ON_ERROR_STOP=1 >/dev/null 2>&1 <<'SYNOD_SQL' &
` + replicaVerificationSQL + `
SYNOD_SQL
work_pid=$!
wait "$work_pid"
work_pid=''
cleanup
temporary=0
if [ -n "$SYNOD_CLONE_COMMAND" ]; then
    failure_stage=publication
    printf 'synod-replica-v1\n%s\n%s\n%s\n' "$SYNOD_CONFIG_DIGEST" "$SYNOD_CLONE_COMMAND" "$system_id" > "$PGDATA/.synod-replica-receipt"
    sync -f "$PGDATA"
    [ ! -e "$target" ]
    [ ! -L "$target" ]
    mv -T "$PGDATA" "$target"
    sync -f "$root"
    printf 'cloned\n' > "$private/bootstrap-complete"
    trap - ERR TERM INT
    exit 0
fi
unset SYNOD_APP_VERIFIER SYNOD_REPL_VERIFIER SYNOD_REPL_PASSWORD PGPASSWORD POSTGRES_PASSWORD
printf 'ready\n' > "$private/bootstrap-complete"
trap - ERR TERM INT
exec postgres -D "$PGDATA" -c config_file="$private/postgresql.conf" -c listen_addresses=0.0.0.0 -c log_min_messages=warning
`

const replicaVerificationSQL = `
\getenv installation SYNOD_INSTALLATION
\getenv cluster SYNOD_CLUSTER
\getenv source SYNOD_SOURCE_RESOURCE
\getenv admin_id SYNOD_ADMIN_ID
\getenv admin_version SYNOD_ADMIN_VERSION
\getenv app_id SYNOD_APP_ID
\getenv app_version SYNOD_APP_VERSION
\getenv verifier SYNOD_APP_VERIFIER
\getenv replication_id SYNOD_REPL_ID
\getenv replication_version SYNOD_REPL_VERSION
\getenv replication_verifier SYNOD_REPL_VERIFIER
SET search_path=pg_catalog;
SET statement_timeout='30s';
SET default_transaction_read_only=on;
SELECT pg_catalog.set_config('synod.replica', pg_catalog.jsonb_build_object(
    'installation', :'installation', 'cluster', :'cluster', 'source', :'source',
    'admin_id', :'admin_id', 'admin_version', :'admin_version',
    'app_id', :'app_id', 'app_version', :'app_version', 'verifier', :'verifier',
    'replication_id', :'replication_id', 'replication_version', :'replication_version',
    'replication_verifier', :'replication_verifier')::text, false);
DO $verify$
DECLARE
    expected jsonb := pg_catalog.current_setting('synod.replica')::jsonb;
    receipt record;
    admin_oid oid;
    namespace_oid oid;
BEGIN
    IF NOT pg_catalog.pg_is_in_recovery() OR NOT pg_catalog.current_setting('transaction_read_only')::boolean THEN
        RAISE EXCEPTION 'replica_role_conflict';
    END IF;
    SELECT oid INTO admin_oid FROM pg_catalog.pg_authid WHERE rolname='synod_admin' AND rolsuper;
    SELECT oid INTO namespace_oid FROM pg_catalog.pg_namespace WHERE nspname='synod_control' AND nspowner=admin_oid;
    IF admin_oid IS NULL OR namespace_oid IS NULL OR EXISTS (
        SELECT FROM pg_catalog.pg_namespace n, LATERAL pg_catalog.aclexplode(COALESCE(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) a
        WHERE n.oid=namespace_oid AND a.grantee<>admin_oid
    ) OR NOT EXISTS (
        SELECT FROM pg_catalog.pg_class WHERE relnamespace=namespace_oid AND relname='bootstrap' AND relkind='r'
        AND relowner=admin_oid AND NOT relrowsecurity
    ) OR EXISTS (
        SELECT FROM pg_catalog.pg_class c, LATERAL pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
        WHERE c.relnamespace=namespace_oid AND c.relname='bootstrap' AND a.grantee<>admin_oid
    ) THEN RAISE EXCEPTION 'replica_receipt_conflict'; END IF;
    SELECT * INTO STRICT receipt FROM synod_control.bootstrap;
    IF receipt.phase IS DISTINCT FROM 'completed'
        OR receipt.installation_id IS DISTINCT FROM (expected->>'installation')::uuid
        OR receipt.cluster_id IS DISTINCT FROM (expected->>'cluster')::uuid
        OR receipt.resource_id IS DISTINCT FROM (expected->>'source')::uuid
        OR receipt.admin_oid IS DISTINCT FROM admin_oid
        OR receipt.admin_credential_id IS DISTINCT FROM (expected->>'admin_id')::uuid OR receipt.admin_version IS DISTINCT FROM (expected->>'admin_version')::bigint
        OR receipt.app_credential_id IS DISTINCT FROM (expected->>'app_id')::uuid OR receipt.app_version IS DISTINCT FROM (expected->>'app_version')::bigint
        OR receipt.replication_credential_id IS DISTINCT FROM (expected->>'replication_id')::uuid OR receipt.replication_version IS DISTINCT FROM (expected->>'replication_version')::bigint
        OR receipt.verifier_fingerprint IS DISTINCT FROM pg_catalog.sha256(pg_catalog.convert_to(expected->>'verifier','UTF8'))
        OR receipt.replication_verifier_fingerprint IS DISTINCT FROM pg_catalog.sha256(pg_catalog.convert_to(expected->>'replication_verifier','UTF8'))
    THEN RAISE EXCEPTION 'replica_source_conflict'; END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_authid WHERE oid=receipt.app_oid AND rolname='synod_app'
        AND rolpassword=expected->>'verifier' AND rolcanlogin AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole
        AND NOT rolinherit AND NOT rolreplication AND NOT rolbypassrls)
        OR EXISTS (SELECT FROM pg_catalog.pg_auth_members WHERE member=receipt.app_oid)
        OR EXISTS (SELECT FROM pg_catalog.pg_database WHERE datdba=receipt.app_oid AND datname<>'app')
    THEN RAISE EXCEPTION 'replica_application_conflict'; END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_authid WHERE oid=receipt.replication_oid AND rolname='synod_repl'
        AND rolpassword=expected->>'replication_verifier' AND rolcanlogin AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole
        AND NOT rolinherit AND rolreplication AND NOT rolbypassrls AND rolvaliduntil IS NULL)
        OR EXISTS (SELECT FROM pg_catalog.pg_auth_members WHERE member=receipt.replication_oid OR roleid=receipt.replication_oid)
        OR EXISTS (SELECT FROM pg_catalog.pg_database WHERE datdba=receipt.replication_oid)
    THEN RAISE EXCEPTION 'replica_replication_conflict'; END IF;
    IF receipt.database_oid IS NULL OR NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE oid=receipt.database_oid AND datname='app' AND datdba=receipt.app_oid)
        OR pg_catalog.has_database_privilege('synod_app','postgres','CONNECT')
        OR pg_catalog.has_database_privilege('synod_app','postgres','TEMP')
        OR pg_catalog.has_database_privilege('synod_app','template1','CONNECT')
        OR pg_catalog.has_database_privilege('synod_app','template1','TEMP')
        OR NOT pg_catalog.has_database_privilege('synod_app','app','CONNECT')
        OR NOT pg_catalog.has_database_privilege('synod_app','app','TEMP')
    THEN RAISE EXCEPTION 'replica_database_conflict'; END IF;
END
$verify$;
`
