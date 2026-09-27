package postgres

import (
	"context"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// PrepareAppWriterLease records the failover boundary before a grant can leave the controller.
// A nil grant means that evidence is not ready or the previous grant is still recent.
// Immediate grants put a lease before a start command in the same transport queue.
func (s *Store) PrepareAppWriterLease(ctx context.Context, installation, id, node, session uuid.UUID, epoch uint64, immediate bool) (*agentv1.WriterLease, error) {
	if installation == uuid.Nil || id == uuid.Nil || node == uuid.Nil || session == uuid.Nil || epoch == 0 || epoch > math.MaxInt64 {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, admissionError(err)
	}
	defer tx.Rollback(ctx)
	var active *uuid.UUID
	var currentEpoch uint64
	var lifecycle string
	var lastGrant *time.Time
	if err := tx.QueryRow(ctx, `SELECT writer_node_id,writer_epoch,desired_lifecycle,writer_last_grant_at FROM apps
 WHERE installation_id=$1 AND id=$2 FOR UPDATE`, installation, id).Scan(&active, &currentEpoch, &lifecycle, &lastGrant); err != nil {
		return nil, admissionError(err)
	}
	if active == nil || *active != node || currentEpoch != epoch || lifecycle != "running" {
		return nil, operation.ErrCommandConflict
	}
	var current uuid.UUID
	var heartbeat, hello []byte
	var lastSeen time.Time
	var recovery, revoked bool
	if err := tx.QueryRow(ctx, `SELECT p.session_id,p.heartbeat_bytes,p.last_seen_at,p.recovery_required,n.revoked_at IS NOT NULL,s.hello_bytes
 FROM agent_presence p JOIN nodes n ON n.installation_id=p.installation_id AND n.id=p.node_id
 JOIN agent_sessions s ON s.installation_id=p.installation_id AND s.node_id=p.node_id AND s.session_id=p.session_id
 WHERE p.installation_id=$1 AND p.node_id=$2 FOR UPDATE OF p`, installation, node).Scan(&current, &heartbeat, &lastSeen, &recovery, &revoked, &hello); err != nil {
		return nil, admissionError(err)
	}
	if current != session {
		return nil, ErrStaleSession
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, admissionError(err)
	}
	if revoked || recovery || !lastSeen.Add(20*time.Second).After(now) || (!immediate && lastGrant != nil && now.Before(lastGrant.Add(10*time.Second))) {
		return nil, nil
	}
	var h agentv1.Heartbeat
	var announced agentv1.Hello
	if proto.Unmarshal(heartbeat, &h) != nil || proto.Unmarshal(hello, &announced) != nil {
		return nil, &AdmissionError{Code: DependencyUnavailable}
	}
	const duration = uint64(app.LeaseSeconds * time.Second)
	if h.WriterClockNanos == 0 || h.WriterClockNanos > math.MaxInt64-duration || !slices.Contains(announced.Capabilities, replication.Capability) {
		return nil, nil
	}
	var promoting bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND app_id=$2 AND kind='app.failover' AND plan_version=0 AND status IN ('pending','running','waiting','cancelling'))`, installation, id).Scan(&promoting); err != nil {
		return nil, admissionError(err)
	}
	if promoting {
		return nil, nil
	}
	var unhealthy bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_copies WHERE installation_id=$1 AND app_id=$2 AND node_id=$3 AND desired AND role='active' AND (health='unhealthy' OR observed_state IN ('failed','stopped')) AND observed_session_id=$4)
 AND NOT EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND app_id=$2 AND kind IN ('app.deploy','app.start','app.failover','app.move') AND status IN ('pending','running','waiting'))`, installation, id, node, session).Scan(&unhealthy); err != nil {
		return nil, admissionError(err)
	}
	if unhealthy {
		return nil, nil
	}
	var encoded []byte
	err = tx.QueryRow(ctx, `SELECT table_bytes FROM agent_replication WHERE installation_id=$1 AND node_id=$2 AND session_id=$3 AND acknowledged_at IS NOT NULL AND failure_code=''`, installation, node, session).Scan(&encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, admissionError(err)
	}
	var table agentv1.ReplicationTable
	if proto.Unmarshal(encoded, &table) != nil {
		return nil, &AdmissionError{Code: DependencyUnavailable}
	}
	if !slices.ContainsFunc(table.Apps, func(a *agentv1.AppReplication) bool {
		return a.AppId == id.String() && a.Role == replication.RoleActive && a.Epoch == epoch
	}) {
		return nil, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE apps SET writer_last_grant_at=$3 WHERE installation_id=$1 AND id=$2`, installation, id, now); err != nil {
		return nil, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return nil, err
	}
	return &agentv1.WriterLease{SessionId: session.String(), AppId: id.String(), Epoch: epoch, Seconds: app.LeaseSeconds, ExpiresAtBootNanos: h.WriterClockNanos + duration}, nil
}

// AppWriter is the durable writer identity. Epoch zero means placement is pending.
type AppWriter struct {
	NodeID      uuid.UUID
	Epoch       uint64
	LastGrantAt *time.Time
}

func (s *Store) GetAppWriter(ctx context.Context, installation, id uuid.UUID) (AppWriter, error) {
	var writer AppWriter
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(writer_node_id,'00000000-0000-0000-0000-000000000000'::uuid),writer_epoch,writer_last_grant_at FROM apps WHERE installation_id=$1 AND id=$2`, installation, id).Scan(&writer.NodeID, &writer.Epoch, &writer.LastGrantAt)
	if err != nil {
		return AppWriter{}, admissionError(err)
	}
	return writer, nil
}
