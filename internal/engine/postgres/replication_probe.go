package postgres

import (
	"context"
	"errors"
	"net/netip"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/engine"
)

const (
	reasonReplicationInvalid        = "postgres_replication_invalid"
	reasonIdentityUnavailable       = "postgres_identity_unavailable"
	reasonIdentityMismatch          = "postgres_identity_mismatch"
	reasonReplicationUnavailable    = "postgres_replication_unavailable"
	reasonReplicationDisconnected   = "postgres_replication_disconnected"
	reasonReplicationSourceMismatch = "postgres_replication_source_mismatch"
	reasonReplicationStale          = "postgres_replication_stale"
	reasonReplicationPaused         = "postgres_replication_paused"
	reasonReplicationLagExceeded    = "postgres_replication_lag_exceeded"
)

func validReplicationCheck(endpoint engine.Endpoint) bool {
	check := endpoint.PostgresReplication
	if check == nil {
		return true
	}
	if endpoint.Database != "postgres" || endpoint.Role != "synod_admin" || endpoint.RedisReplication != nil {
		return false
	}
	for _, value := range []string{check.InstallationID, check.ClusterID, check.ResourceID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return false
		}
	}
	if endpoint.PostgresRole == "primary" {
		return check.Source == nil && check.MaxReplayLagBytes == 0 && check.MaxMessageAge == 0
	}
	source := check.Source
	if source == nil || source.Port == 0 || source.Port > 65535 || source.ResourceID == check.ResourceID || check.MaxReplayLagBytes < 1 || check.MaxReplayLagBytes > 1<<30 || check.MaxMessageAge < time.Second || check.MaxMessageAge > 120*time.Second {
		return false
	}
	address, err := netip.ParseAddr(source.Address)
	id, idErr := uuid.Parse(source.ResourceID)
	return err == nil && address.String() == source.Address && address.Zone() == "" && (address.IsGlobalUnicast() || address.IsLoopback()) && idErr == nil && id != uuid.Nil && id.String() == source.ResourceID
}

func probeReplication(ctx context.Context, connection *pgx.Conn, endpoint engine.Endpoint, observed *engine.PostgresObservation) engine.Probe {
	check := endpoint.PostgresReplication
	resource := check.ResourceID
	if check.Source != nil {
		resource = check.Source.ResourceID
	}
	var identity bool
	var systemID string
	err := connection.QueryRow(ctx, `SELECT
 (SELECT system_identifier::text FROM pg_catalog.pg_control_system()),
 (SELECT count(*)=1 AND COALESCE(bool_and(b.phase='completed'
  AND b.installation_id=$1::uuid AND b.cluster_id=$2::uuid AND b.resource_id=$3::uuid
  AND b.admin_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname='synod_admin' AND rolsuper)),false)
  FROM synod_control.bootstrap b)
 AND EXISTS (SELECT FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_roles a ON a.oid=n.nspowner
  WHERE n.nspname='synod_control' AND a.rolname='synod_admin' AND a.rolsuper
  AND NOT EXISTS (SELECT FROM pg_catalog.aclexplode(COALESCE(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) acl WHERE acl.grantee<>a.oid))
 AND EXISTS (SELECT FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
  JOIN pg_catalog.pg_roles a ON a.oid=c.relowner
  WHERE n.nspname='synod_control' AND c.relname='bootstrap' AND c.relkind='r' AND NOT c.relrowsecurity
  AND a.rolname='synod_admin' AND a.rolsuper
  AND NOT EXISTS (SELECT FROM pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) acl WHERE acl.grantee<>a.oid))`, check.InstallationID, check.ClusterID, resource).Scan(&systemID, &identity)
	if err != nil {
		return engine.Probe{State: engine.ProbeUnavailable, Reason: reasonIdentityUnavailable}
	}
	nativeID, err := strconv.ParseUint(systemID, 10, 64)
	if err != nil || nativeID == 0 || strconv.FormatUint(nativeID, 10) != systemID {
		return engine.Probe{State: engine.ProbeUnavailable, Reason: reasonIdentityUnavailable}
	}
	observed.SystemIdentifier = systemID
	if !identity {
		return engine.Probe{State: engine.ProbeFailed, Reason: reasonIdentityMismatch}
	}
	if check.Source == nil {
		return engine.Probe{State: engine.ProbePassed}
	}
	receiver := &engine.PostgresReplicationObservation{}
	var host string
	var port int32
	err = connection.QueryRow(ctx, `SELECT status,sender_host,sender_port,received_tli,
 flushed_lsn::text,latest_end_lsn::text,pg_catalog.pg_last_wal_replay_lsn()::text,
 CASE WHEN flushed_lsn IS NOT NULL AND latest_end_lsn IS NOT NULL AND pg_catalog.pg_last_wal_replay_lsn() IS NOT NULL
 THEN GREATEST(0,pg_catalog.pg_wal_lsn_diff(GREATEST(flushed_lsn,latest_end_lsn),pg_catalog.pg_last_wal_replay_lsn()))::bigint END,
 FLOOR(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()-last_msg_receipt_time)*1000)::bigint,
 pg_catalog.pg_is_wal_replay_paused()
 FROM pg_catalog.pg_stat_wal_receiver`).Scan(&receiver.Status, &host, &port, &receiver.Timeline,
		&receiver.FlushedLSN, &receiver.ReportedLSN, &receiver.ReplayLSN, &receiver.ReplayLagBytes, &receiver.MessageAgeMillis, &receiver.ReplayPaused)
	if errors.Is(err, pgx.ErrNoRows) {
		observed.Replication = &engine.PostgresReplicationObservation{Status: "absent"}
		return engine.Probe{State: engine.ProbeFailed, Reason: reasonReplicationDisconnected}
	}
	if err != nil {
		return engine.Probe{State: engine.ProbeUnavailable, Reason: reasonReplicationUnavailable}
	}
	observed.Replication = receiver
	if receiver.Status != "streaming" {
		return engine.Probe{State: engine.ProbeFailed, Reason: reasonReplicationDisconnected}
	}
	actual, err := netip.ParseAddr(host)
	expected, _ := netip.ParseAddr(check.Source.Address)
	if err != nil || actual.Unmap() != expected.Unmap() || int64(port) != int64(check.Source.Port) {
		return engine.Probe{State: engine.ProbeFailed, Reason: reasonReplicationSourceMismatch}
	}
	if receiver.Timeline < 1 || receiver.FlushedLSN == nil || receiver.ReportedLSN == nil || receiver.ReplayLSN == nil || receiver.ReplayLagBytes == nil || receiver.MessageAgeMillis == nil {
		return engine.Probe{State: engine.ProbeUnavailable, Reason: reasonReplicationUnavailable}
	}
	if *receiver.MessageAgeMillis < 0 || *receiver.MessageAgeMillis > check.MaxMessageAge.Milliseconds() {
		return engine.Probe{State: engine.ProbeFailed, Reason: reasonReplicationStale}
	}
	if receiver.ReplayPaused {
		return engine.Probe{State: engine.ProbeFailed, Reason: reasonReplicationPaused}
	}
	if *receiver.ReplayLagBytes > check.MaxReplayLagBytes {
		return engine.Probe{State: engine.ProbeFailed, Reason: reasonReplicationLagExceeded}
	}
	return engine.Probe{State: engine.ProbePassed}
}
