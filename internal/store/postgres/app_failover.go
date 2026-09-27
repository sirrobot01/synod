package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// AppFailoverCandidate holds complete evidence from a current standby session.
type AppFailoverCandidate struct {
	CopyID   uuid.UUID
	NodeID   uuid.UUID
	Sequence uint64
}

func (s *Store) AppFailoverCandidates(ctx context.Context, installation, id uuid.UUID) ([]AppFailoverCandidate, error) {
	rows, err := s.pool.Query(ctx, `SELECT c.id,c.node_id,r.sequence FROM app_copies c
 JOIN apps a ON a.installation_id=c.installation_id AND a.id=c.app_id
 JOIN app_replicas r ON r.installation_id=c.installation_id AND r.app_id=c.app_id AND r.node_id=c.node_id
 JOIN agent_presence p ON p.installation_id=c.installation_id AND p.node_id=c.node_id
 JOIN nodes n ON n.installation_id=c.installation_id AND n.id=c.node_id
 WHERE c.installation_id=$1 AND c.app_id=$2 AND c.desired AND c.role='standby'
 AND r.role='standby' AND r.failure_code='' AND r.writer_epoch=a.writer_epoch AND r.sequence>0 AND r.completed_at IS NOT NULL
 AND r.session_id=p.session_id AND r.observed_at>clock_timestamp()-interval '20 seconds'
 AND p.last_seen_at>clock_timestamp()-interval '20 seconds' AND NOT p.recovery_required AND n.revoked_at IS NULL
 ORDER BY r.sequence DESC,c.node_id`, installation, id)
	if err != nil {
		return nil, admissionError(err)
	}
	candidates, err := pgx.CollectRows(rows, pgx.RowToStructByPos[AppFailoverCandidate])
	if err != nil || len(candidates) == 0 {
		return candidates, err
	}
	var encoded []byte
	err = s.pool.QueryRow(ctx, `SELECT p.request_bytes FROM operation_placements p JOIN operations o ON o.installation_id=p.installation_id AND o.id=p.operation_id WHERE o.installation_id=$1 AND o.app_id=$2 AND o.kind IN ('app.deploy','app.start') ORDER BY o.desired_revision LIMIT 1`, installation, id).Scan(&encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, admissionError(err)
	}
	var request placement.Request
	if err := json.Unmarshal(encoded, &request); err != nil {
		return nil, err
	}
	index := slices.IndexFunc(request.Resources, func(r placement.Resource) bool { return r.Role == "active" })
	if index < 0 {
		return nil, operation.ErrCommandConflict
	}
	original := request.Resources[index]
	request.Groups = nil
	snapshot, err := s.ReadPlacementSnapshot(ctx, installation)
	if err != nil {
		return nil, err
	}
	eligible := make([]AppFailoverCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		index := slices.IndexFunc(snapshot.Allocations, func(a placement.Allocation) bool { return a.ResourceID == candidate.CopyID && !a.Released })
		if index < 0 {
			continue
		}
		allocation := snapshot.Allocations[index]
		resource := original
		resource.ID, resource.ReservationID, resource.OwnedResourceIDs, resource.NodeAffinity, resource.Ports = allocation.ResourceID, allocation.ID, allocation.OwnedResourceIDs, candidate.NodeID, allocation.Ports
		request.Resources = []placement.Resource{resource}
		if _, err := placement.Plan(request, snapshot); err == nil {
			eligible = append(eligible, candidate)
		}
	}
	return eligible, nil
}

// AdmitAppFailover waits for the last possible writer grant before replacing work.
func (s *Store) AdmitAppFailover(ctx context.Context, installation, id uuid.UUID) (uuid.UUID, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, admissionError(err)
	}
	defer tx.Rollback(ctx)
	var epoch, revision int64
	var due bool
	err = tx.QueryRow(ctx, `SELECT writer_epoch,desired_revision,
 tier=2 AND desired_lifecycle='running' AND writer_last_grant_at IS NOT NULL
 AND writer_last_grant_at+make_interval(secs=>(spec->'replication'->>'failover_after_seconds')::double precision)<=clock_timestamp()
 FROM apps WHERE installation_id=$1 AND id=$2 FOR UPDATE`, installation, id).Scan(&epoch, &revision, &due)
	if err != nil {
		return uuid.Nil, admissionError(err)
	}
	if !due || epoch == math.MaxInt64 || revision == math.MaxInt64 {
		return uuid.Nil, nil
	}
	var pending uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM operations WHERE installation_id=$1 AND app_id=$2 AND kind='app.failover' AND status IN ('pending','running','waiting','cancelling')`, installation, id).Scan(&pending)
	if err == nil {
		return pending, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, admissionError(err)
	}
	// Do not supersede an operation until at least one complete replica exists.
	var complete bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_replicas r JOIN app_copies c USING(installation_id,app_id,node_id)
 WHERE r.installation_id=$1 AND r.app_id=$2 AND r.writer_epoch=$3 AND r.role='standby' AND r.failure_code='' AND r.sequence>0 AND c.desired AND c.role='standby')`, installation, id, epoch).Scan(&complete); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if !complete {
		return uuid.Nil, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE operations SET status='cancelled',failure_code='writer_lease_expired',claim_active=false,
 claim_expires_at=CASE WHEN owner_id IS NOT NULL THEN clock_timestamp() END,finished_at=clock_timestamp(),updated_at=clock_timestamp()
 WHERE installation_id=$1 AND app_id=$2 AND status IN ('pending','running','waiting','cancelling')`, installation, id); err != nil {
		return uuid.Nil, admissionError(err)
	}
	pending = uuid.New()
	hash := sha256.Sum256([]byte(pending.String()))
	if _, err := tx.Exec(ctx, `UPDATE apps SET desired_revision=desired_revision+1,observed_lifecycle='unknown',updated_at=clock_timestamp() WHERE installation_id=$1 AND id=$2`, installation, id); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE app_copies SET desired_revision=$3,health='unknown',observed_at=NULL,observed_session_id=NULL WHERE installation_id=$1 AND app_id=$2 AND desired`, installation, id, revision+1); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO operations(id,installation_id,cluster_id,actor_id,api_operation,idempotency_key,request_hash,hash_version,kind,desired_revision,status,plan_version,execution_epoch,attempts,created_at,updated_at)
 VALUES($1,$2,$3,'controller','app.failover',$4,$5,1,'app.failover',$6,'pending',0,0,0,clock_timestamp(),clock_timestamp())`, pending, installation, id, pending.String(), hash[:], revision+1); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox(event_id,installation_id,cluster_id,operation_id,kind,schema_version,created_at) VALUES($1,$2,$3,$4,'operation.ready',1,clock_timestamp())`, uuid.New(), installation, id, pending); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return uuid.Nil, err
	}
	return pending, nil
}

// recordAppPromotion commits the new writer with its compute reservation and commands.
func recordAppPromotion(ctx context.Context, tx pgx.Tx, claim operation.Claim, request placement.Request, decision placement.Decision, plan *agentv1.ExecutionPlan) error {
	if (len(request.Resources) < 1 || len(request.Resources) > 2) || len(decision.Assignments) != len(request.Resources) || len(plan.Steps) != 2 {
		return operation.ErrCommandConflict
	}
	if err := operation.ValidatePlan(plan, operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID, DesiredRevision: request.DesiredRevision, Kind: "app.failover"}); err != nil {
		return err
	}
	resource := request.Resources[0]
	index := slices.IndexFunc(decision.Assignments, func(a placement.Assignment) bool { return a.ResourceID == resource.ID })
	if index < 0 {
		return operation.ErrCommandConflict
	}
	a := decision.Assignments[index]
	ensure, start := plan.Steps[0].Command, plan.Steps[1].Command
	if resource.Role != "active" || a.ResourceID != resource.ID || ensure.Input.ResourceId != resource.ID.String() || ensure.Epoch.NodeId != a.NodeID.String() || ensure.Input.GetEnsureContainer().GetGeneric() == nil {
		return operation.ErrCommandConflict
	}
	input := ensure.Input.GetEnsureContainer()
	if start.Input.GetStartContainer() == nil || start.Input.ResourceId != ensure.Input.ResourceId || start.Epoch.NodeId != ensure.Epoch.NodeId || !slices.Contains(plan.Steps[1].DependsOn, ensure.StepId) || len(input.Ports) != 1 || len(a.Ports) != 1 {
		return operation.ErrCommandConflict
	}
	binding, reserved := input.Ports[0], a.Ports[0]
	if reserved.Name != "backend" || binding.HostPort != uint32(reserved.Number) || binding.HostAddress != reserved.Address || binding.Transport != reserved.Transport {
		return operation.ErrCommandConflict
	}
	var name string
	var epoch uint64
	var due bool
	if err := tx.QueryRow(ctx, `SELECT name,writer_epoch,writer_last_grant_at IS NOT NULL AND writer_last_grant_at+make_interval(secs=>(spec->'replication'->>'failover_after_seconds')::double precision)<=clock_timestamp() AND desired_lifecycle='running'
 FROM apps WHERE installation_id=$1 AND id=$2 AND desired_revision=$3`, claim.InstallationID, claim.ClusterID, request.DesiredRevision).Scan(&name, &epoch, &due); err != nil {
		return admissionError(err)
	}
	if !due || epoch == math.MaxInt64 {
		return operation.ErrCommandConflict
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM agent_presence WHERE installation_id=$1 AND node_id=$2 FOR SHARE`, claim.InstallationID, a.NodeID); err != nil {
		return admissionError(err)
	}
	var sequence uint64
	var completed time.Time
	var copyIndex uint32
	var hostPort uint16
	err := tx.QueryRow(ctx, `SELECT r.sequence,r.completed_at,c.copy_index,c.host_port FROM app_replicas r JOIN app_copies c USING(installation_id,app_id,node_id)
 JOIN agent_presence p USING(installation_id,node_id)
 WHERE c.installation_id=$1 AND c.app_id=$2 AND c.id=$3 AND c.node_id=$4 AND c.desired AND c.role='standby'
 AND r.role='standby' AND r.failure_code='' AND r.writer_epoch=$5 AND r.sequence>0 AND r.session_id=p.session_id
 AND r.observed_at>clock_timestamp()-interval '20 seconds' AND p.last_seen_at>clock_timestamp()-interval '20 seconds' AND NOT p.recovery_required
 FOR SHARE OF r`, claim.InstallationID, claim.ClusterID, resource.ID, a.NodeID, epoch).Scan(&sequence, &completed, &copyIndex, &hostPort)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return operation.ErrCommandConflict
		}
		return admissionError(err)
	}
	if input.Generic.AppName != name || input.Generic.CopyIndex != copyIndex || binding.HostPort != uint32(hostPort) {
		return operation.ErrCommandConflict
	}
	rows, err := tx.Query(ctx, `SELECT allocation_bytes FROM resource_allocations WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL AND resource_id IN (SELECT id FROM app_copies WHERE installation_id=$1 AND app_id=$2 AND desired AND role='active')`, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return admissionError(err)
	}
	allocations, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		return admissionError(err)
	}
	for _, encoded := range allocations {
		var allocation placement.Allocation
		if err := json.Unmarshal(encoded, &allocation); err != nil {
			return err
		}
		allocation.Budget.CPUMillis, allocation.Budget.MemoryBytes = 0, 0
		encoded, err := json.Marshal(allocation)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE resource_allocations SET allocation_bytes=$2 WHERE id=$1`, allocation.ID, encoded); err != nil {
			return admissionError(err)
		}
	}
	if len(request.Resources) == 2 {
		standby := request.Resources[1]
		index := slices.IndexFunc(decision.Assignments, func(a placement.Assignment) bool { return a.ResourceID == standby.ID })
		if standby.Role != "standby" || index < 0 {
			return operation.ErrCommandConflict
		}
		assigned := decision.Assignments[index]
		if len(assigned.Ports) != 1 || assigned.Ports[0].Name != "backend" || assigned.NodeID == a.NodeID {
			return operation.ErrCommandConflict
		}
		// Keep the old allocation until data cleanup. It no longer receives assignments.
		if _, err := tx.Exec(ctx, `UPDATE app_copies SET desired=false,role='standby',health='unknown',observed_at=NULL,observed_session_id=NULL WHERE installation_id=$1 AND app_id=$2 AND desired AND role='active'`, claim.InstallationID, claim.ClusterID); err != nil {
			return admissionError(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO app_copies(id,installation_id,app_id,node_id,copy_index,host_port,desired_revision,role,observed_state,created_at,updated_at)
 SELECT $1,$2,$3,$4,COALESCE(MAX(copy_index),0)+1,$5,$6,'standby','pending',clock_timestamp(),clock_timestamp() FROM app_copies WHERE installation_id=$2 AND app_id=$3`, standby.ID, claim.InstallationID, claim.ClusterID, assigned.NodeID, assigned.Ports[0].Number, request.DesiredRevision); err != nil {
			return admissionError(err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE app_copies SET role='standby',health='unknown',observed_at=NULL,observed_session_id=NULL WHERE installation_id=$1 AND app_id=$2 AND desired AND role='active'`, claim.InstallationID, claim.ClusterID); err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE app_copies SET role='active',observed_state='pending',health='unknown',observed_at=NULL,observed_session_id=NULL WHERE id=$1`, resource.ID); err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE apps SET writer_epoch=writer_epoch+1,writer_node_id=$3,writer_last_grant_at=NULL,updated_at=clock_timestamp() WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.ClusterID, a.NodeID); err != nil {
		return admissionError(err)
	}
	return nil
}
