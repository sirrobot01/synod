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
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// PrepareAppReplication keeps the revision until the assignment or session changes.
func (s *Store) PrepareAppReplication(ctx context.Context, installation, node uuid.UUID, desired *agentv1.ReplicationTable) (*agentv1.ReplicationTable, bool, error) {
	if installation == uuid.Nil || node == uuid.Nil || desired == nil || replication.Validate(desired.Apps) != nil {
		return nil, false, &AdmissionError{Code: InvalidRequest}
	}
	session, err := uuid.Parse(desired.SessionId)
	if err != nil || session == uuid.Nil || session.String() != desired.SessionId {
		return nil, false, &AdmissionError{Code: InvalidRequest}
	}
	for _, a := range desired.Apps {
		if a.Epoch > math.MaxInt64 {
			return nil, false, &AdmissionError{Code: InvalidRequest}
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, admissionError(err)
	}
	defer tx.Rollback(ctx)
	ids := make([]string, 0, len(desired.Apps))
	for _, a := range desired.Apps {
		ids = append(ids, a.AppId)
	}
	slices.Sort(ids)
	// App locks precede presence, as they do for routing and lifecycle admission.
	for _, id := range ids {
		var tier int
		var lifecycle string
		var epoch uint64
		var active *uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT tier,desired_lifecycle,writer_epoch,writer_node_id FROM apps WHERE installation_id=$1 AND id=$2 FOR SHARE`, installation, id).Scan(&tier, &lifecycle, &epoch, &active); err != nil {
			return nil, false, admissionError(err)
		}
		assignment := desired.Apps[slices.IndexFunc(desired.Apps, func(a *agentv1.AppReplication) bool { return a.AppId == id })]
		var role string
		err := tx.QueryRow(ctx, `SELECT role FROM app_copies WHERE installation_id=$1 AND app_id=$2 AND node_id=$3 AND desired`, installation, id, node).Scan(&role)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, false, admissionError(err)
		}
		if tier != 2 || lifecycle != "running" || active == nil || epoch != assignment.Epoch || role != assignment.Role || (role == replication.RoleActive) != (*active == node) {
			return nil, false, operation.ErrCommandConflict
		}
	}
	var current uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT session_id FROM agent_presence WHERE installation_id=$1 AND node_id=$2 FOR UPDATE`, installation, node).Scan(&current); err != nil {
		return nil, false, admissionError(err)
	}
	if current != session {
		return nil, false, ErrStaleSession
	}
	var stored []byte
	var acknowledged bool
	err = tx.QueryRow(ctx, `SELECT table_bytes,acknowledged_at IS NOT NULL AND failure_code='' FROM agent_replication WHERE installation_id=$1 AND node_id=$2 FOR UPDATE`, installation, node).Scan(&stored, &acknowledged)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, admissionError(err)
	}
	table := proto.Clone(desired).(*agentv1.ReplicationTable)
	table.Revision = 1
	if len(stored) > 0 {
		var previous agentv1.ReplicationTable
		if proto.Unmarshal(stored, &previous) != nil {
			return nil, false, &AdmissionError{Code: DependencyUnavailable}
		}
		table.Revision = previous.Revision
		if proto.Equal(table, &previous) {
			return table, acknowledged, nil
		}
		if previous.Revision >= math.MaxInt64 {
			return nil, false, operation.ErrCommandConflict
		}
		for _, a := range table.Apps {
			i := slices.IndexFunc(previous.Apps, func(old *agentv1.AppReplication) bool { return old.AppId == a.AppId })
			if i >= 0 && a.Epoch < previous.Apps[i].Epoch {
				return nil, false, operation.ErrCommandConflict
			}
		}
		table.Revision++
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(table)
	if err != nil {
		return nil, false, err
	}
	if len(encoded) > 1<<20 {
		return nil, false, &AdmissionError{Code: InvalidRequest}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO agent_replication(installation_id,node_id,session_id,revision,table_bytes) VALUES($1,$2,$3,$4,$5)
 ON CONFLICT(installation_id,node_id) DO UPDATE SET session_id=EXCLUDED.session_id,revision=EXCLUDED.revision,table_bytes=EXCLUDED.table_bytes,acknowledged_at=NULL,failure_code=''`, installation, node, session, table.Revision, encoded); err != nil {
		return nil, false, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return nil, false, err
	}
	return table, false, nil
}

// RecordAppReplicationAck accepts the current node session and published revision.
func (s *Store) RecordAppReplicationAck(ctx context.Context, installation, node, session uuid.UUID, ack *agentv1.ReplicationAck) error {
	if ack == nil || ack.SessionId != session.String() || ack.Revision > math.MaxInt64 || controlstream.CheckAgentMessage(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_ReplicationAck{ReplicationAck: ack}}, "", "") != nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	var current uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT session_id FROM agent_presence WHERE installation_id=$1 AND node_id=$2 FOR UPDATE`, installation, node).Scan(&current); err != nil {
		return admissionError(err)
	}
	if current != session {
		return ErrStaleSession
	}
	if _, err := tx.Exec(ctx, `UPDATE agent_replication SET acknowledged_at=clock_timestamp(),failure_code=$1 WHERE installation_id=$2 AND node_id=$3 AND session_id=$4 AND revision=$5`, ack.FailureCode, installation, node, session, ack.Revision); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}

// recordAppReplicas runs with the authenticated presence row locked.
func recordAppReplicas(ctx context.Context, tx pgx.Tx, installation, node, session uuid.UUID, reports []*agentv1.ReplicaStatus) error {
	if len(reports) == 0 {
		return nil
	}
	var encoded []byte
	err := tx.QueryRow(ctx, `SELECT table_bytes FROM agent_replication WHERE installation_id=$1 AND node_id=$2 AND session_id=$3 AND acknowledged_at IS NOT NULL AND failure_code=''`, installation, node, session).Scan(&encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return admissionError(err)
	}
	var table agentv1.ReplicationTable
	if proto.Unmarshal(encoded, &table) != nil {
		return &AdmissionError{Code: DependencyUnavailable}
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return admissionError(err)
	}
	for _, report := range reports {
		index := slices.IndexFunc(table.Apps, func(a *agentv1.AppReplication) bool { return a.AppId == report.AppId })
		if index < 0 {
			continue
		}
		assignment := table.Apps[index]
		if report.Role != assignment.Role || (report.Epoch != assignment.Epoch && report.Epoch != 0) {
			continue
		}
		var completed *time.Time
		if report.Sequence > 0 {
			if report.Epoch != assignment.Epoch || report.CompletedAt == nil || report.CompletedAt.CheckValid() != nil {
				continue
			}
			at := report.CompletedAt.AsTime()
			if at.After(now.Add(5 * time.Second)) {
				continue
			}
			if at.After(now) {
				at = now
			}
			completed = &at
		} else if report.FailureCode == "" {
			continue
		}
		if _, err := tx.Exec(ctx, `INSERT INTO app_replicas(installation_id,app_id,node_id,session_id,role,writer_epoch,sequence,completed_at,observed_at,failure_code,last_pause_nanos,max_pause_nanos,skipped_snapshots)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
 ON CONFLICT(installation_id,app_id,node_id) DO UPDATE SET session_id=EXCLUDED.session_id,role=EXCLUDED.role,
 writer_epoch=EXCLUDED.writer_epoch,
 sequence=CASE WHEN EXCLUDED.writer_epoch>app_replicas.writer_epoch OR EXCLUDED.sequence>app_replicas.sequence THEN EXCLUDED.sequence ELSE app_replicas.sequence END,
 completed_at=CASE WHEN EXCLUDED.writer_epoch>app_replicas.writer_epoch OR EXCLUDED.sequence>app_replicas.sequence THEN EXCLUDED.completed_at ELSE app_replicas.completed_at END,
 last_pause_nanos=EXCLUDED.last_pause_nanos,
 max_pause_nanos=GREATEST(app_replicas.max_pause_nanos,EXCLUDED.max_pause_nanos),
 skipped_snapshots=GREATEST(app_replicas.skipped_snapshots,EXCLUDED.skipped_snapshots),
 observed_at=EXCLUDED.observed_at,failure_code=EXCLUDED.failure_code
 WHERE EXCLUDED.writer_epoch>app_replicas.writer_epoch OR (EXCLUDED.writer_epoch=app_replicas.writer_epoch AND (EXCLUDED.sequence>=app_replicas.sequence OR EXCLUDED.sequence=0))`, installation, report.AppId, node, session, report.Role, assignment.Epoch, report.Sequence, completed, now, report.FailureCode, report.LastPauseNanos, report.MaxPauseNanos, report.SkippedSnapshots); err != nil {
			return admissionError(err)
		}
	}
	return nil
}
