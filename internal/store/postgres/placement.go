package postgres

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// ErrSnapshotTooLarge reports an installation larger than one planning snapshot.
var ErrSnapshotTooLarge = errors.New("placement_snapshot_too_large")

// ReadPlacementSnapshot observes nodes and held allocations at one database time.
// It reports state and never judges it. The planner applies every freshness and
// compatibility rule against the returned observation time.
//
// The read is repeatable so that nodes and allocations come from one snapshot,
// and it takes the observation time last. A time read first can precede a heartbeat
// that the node read then sees, and the planner rejects any node seen in the future.
//
// Port and owned-resource lists come from the stored typed allocation.
// allocation_ports and allocation_resources exist to enforce uniqueness in the
// database, and one transaction writes both from the same value.
func (s *Store) ReadPlacementSnapshot(ctx context.Context, installationID uuid.UUID) (placement.Snapshot, error) {
	if installationID == uuid.Nil {
		return placement.Snapshot{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return placement.Snapshot{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	snapshot := placement.Snapshot{InstallationID: installationID}
	if snapshot.Nodes, err = readPlacementNodes(ctx, tx, installationID); err != nil {
		return placement.Snapshot{}, err
	}
	if snapshot.Allocations, err = readAllocations(ctx, tx, installationID); err != nil {
		return placement.Snapshot{}, err
	}
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&snapshot.ObservedAt); err != nil {
		return placement.Snapshot{}, admissionError(err)
	}
	return snapshot, nil
}

func readPlacementNodes(ctx context.Context, tx pgx.Tx, installationID uuid.UUID) ([]placement.Node, error) {
	rows, err := tx.Query(ctx, `SELECT n.id, n.revoked_at IS NOT NULL, p.session_id, s.hello_bytes, p.last_seen_at, p.recovery_required,
 policy.policy_bytes, capacity.probe_bytes, capacity.report_bytes
 FROM nodes n
 LEFT JOIN agent_presence p ON p.installation_id=n.installation_id AND p.node_id=n.id
 LEFT JOIN agent_sessions s ON s.installation_id=p.installation_id AND s.node_id=p.node_id AND s.session_id=p.session_id
 LEFT JOIN node_placement_policies policy ON policy.installation_id=n.installation_id AND policy.node_id=n.id
 LEFT JOIN node_capacity capacity ON capacity.installation_id=n.installation_id AND capacity.node_id=n.id
 WHERE n.installation_id=$1 ORDER BY n.id LIMIT $2`, installationID, placement.MaxNodes+1)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	var nodes []placement.Node
	for rows.Next() {
		node := placement.Node{InstallationID: installationID}
		var session *uuid.UUID
		var lastSeen *time.Time
		var recovery *bool
		var helloBytes, policyBytes, probeBytes, reportBytes []byte
		if err := rows.Scan(&node.ID, &node.Revoked, &session, &helloBytes, &lastSeen, &recovery, &policyBytes, &probeBytes, &reportBytes); err != nil {
			return nil, admissionError(err)
		}
		if session != nil {
			node.SessionID = *session
		}
		if lastSeen != nil {
			node.LastSeenAt = *lastSeen
		}
		if recovery != nil {
			node.RecoveryRequired = *recovery
		}
		if helloBytes != nil {
			var hello agentv1.Hello
			if proto.Unmarshal(helloBytes, &hello) != nil {
				return nil, &AdmissionError{Code: DependencyUnavailable}
			}
			// An unreadable identity leaves the zero value, which the planner rejects.
			node.BootID, _ = uuid.Parse(hello.BootId)
			node.JournalGeneration, _ = uuid.Parse(hello.JournalGeneration)
		}
		if policyBytes != nil {
			node.Policy = new(placement.NodePolicy)
			if json.Unmarshal(policyBytes, node.Policy) != nil {
				return nil, &AdmissionError{Code: DependencyUnavailable}
			}
		}
		if probeBytes != nil {
			node.Probe = new(placement.CapacityProbe)
			if json.Unmarshal(probeBytes, node.Probe) != nil {
				return nil, &AdmissionError{Code: DependencyUnavailable}
			}
		}
		if reportBytes != nil {
			node.Report = new(placement.CapacityReport)
			if json.Unmarshal(reportBytes, node.Report) != nil {
				return nil, &AdmissionError{Code: DependencyUnavailable}
			}
		}
		nodes = append(nodes, node)
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	if len(nodes) > placement.MaxNodes {
		return nil, ErrSnapshotTooLarge
	}
	return nodes, nil
}

// readAllocations returns held reservations only. A tombstone holds no capacity and
// no port, and counting one toward the planning bound would eventually stop all
// placement for an installation with normal churn, with no way to recover.
// The unique key on installation and resource still prevents a reused resource identity.
func readAllocations(ctx context.Context, tx pgx.Tx, installationID uuid.UUID) ([]placement.Allocation, error) {
	rows, err := tx.Query(ctx, `SELECT allocation_bytes, released_at IS NOT NULL FROM resource_allocations
 WHERE installation_id=$1 AND released_at IS NULL ORDER BY id LIMIT $2`, installationID, placement.MaxAllocations+1)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	var allocations []placement.Allocation
	for rows.Next() {
		var encoded []byte
		var released bool
		if err := rows.Scan(&encoded, &released); err != nil {
			return nil, admissionError(err)
		}
		var allocation placement.Allocation
		if json.Unmarshal(encoded, &allocation) != nil {
			return nil, &AdmissionError{Code: DependencyUnavailable}
		}
		// The row decides release. The stored value records the reservation itself.
		allocation.Released = released
		allocations = append(allocations, allocation)
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	if len(allocations) > placement.MaxAllocations {
		return nil, ErrSnapshotTooLarge
	}
	return allocations, nil
}

// ErrPlacementConflict reports a placement that no longer fits its locked inputs.
// The caller can read a new snapshot and plan again.
var ErrPlacementConflict = errors.New("placement_conflict")

// ErrPlacementHeld reports a release request for an allocation that work still needs.
var ErrPlacementHeld = errors.New("placement_still_held")

const maxRequestBytes = 262144
const maxDecisionBytes = 65536
const maxAllocationBytes = 65536

// PlacementRecord is the committed placement of one operation.
type PlacementRecord struct {
	Request              placement.Request
	Decision             placement.Decision
	Allocations          []placement.Allocation
	RequestHash          [32]byte
	PlanHash             [32]byte
	CredentialIntentHash []byte
	CommittedAt          time.Time
}

// InstallPlacementPlan commits reservations, an optional sealed intent, and the immutable plan together.
// A committed placement replays without fresh capacity evidence.
func (s *Store) InstallPlacementPlan(ctx context.Context, claim operation.Claim, request placement.Request, decision placement.Decision, plan *agentv1.ExecutionPlan, intent *ClusterIntent) (PlacementRecord, error) {
	if err := request.Validate(); err != nil {
		return PlacementRecord{}, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	if request.InstallationID != claim.InstallationID || request.ClusterID != claim.ClusterID || request.OperationID != claim.OperationID {
		return PlacementRecord{}, operation.ErrStaleOwner
	}
	encodedPlan, err := validateInstalledPlan(claim, plan)
	if err != nil {
		return PlacementRecord{}, err
	}
	var intentHash []byte
	if intent != nil {
		if intent.InstallationID != claim.InstallationID || intent.ClusterID != claim.ClusterID {
			return PlacementRecord{}, ErrCredentialConflict
		}
		if err := intent.validate(); err != nil {
			return PlacementRecord{}, err
		}
		for _, credential := range intent.Credentials {
			referenced := false
			for _, step := range plan.Steps {
				if slices.ContainsFunc(step.Command.Input.Secrets, func(reference *agentv1.SecretReference) bool {
					return reference.CredentialId == credential.CredentialID.String() && reference.Version == credential.Version && reference.Purpose == credential.Purpose
				}) {
					referenced = true
					break
				}
			}
			if !referenced {
				return PlacementRecord{}, ErrCredentialConflict
			}
		}
		encoded, err := json.Marshal(intent)
		if err != nil {
			return PlacementRecord{}, &AdmissionError{Code: InvalidRequest, Cause: err}
		}
		hash := sha256.Sum256(encoded)
		intentHash = hash[:]
	}
	requestBytes, err := json.Marshal(request)
	if err != nil || len(requestBytes) > maxRequestBytes {
		return PlacementRecord{}, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	decisionBytes, err := json.Marshal(decision)
	if err != nil || len(decisionBytes) > maxDecisionBytes {
		return PlacementRecord{}, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	requestHash, planHash := sha256.Sum256(requestBytes), sha256.Sum256(encodedPlan)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return PlacementRecord{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	var operationKind string
	if err := tx.QueryRow(ctx, `SELECT kind FROM operations WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.OperationID).Scan(&operationKind); err != nil {
		return PlacementRecord{}, admissionError(err)
	}
	if strings.HasPrefix(operationKind, "app.") {
		if _, err := tx.Exec(ctx, `SELECT 1 FROM apps WHERE installation_id=$1 AND id=$2 FOR UPDATE`, claim.InstallationID, claim.ClusterID); err != nil {
			return PlacementRecord{}, admissionError(err)
		}
	}
	if intent != nil {
		if _, err := tx.Exec(ctx, `SELECT 1 FROM clusters WHERE installation_id=$1 AND id=$2 FOR UPDATE`, claim.InstallationID, claim.ClusterID); err != nil {
			return PlacementRecord{}, admissionError(err)
		}
	}
	active, status, existing, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return PlacementRecord{}, err
	}
	// A terminal transition leaves the claim row intact for the rest of its lease,
	// so ownership alone does not prove that this operation still needs capacity.
	if status == operation.Succeeded || status == operation.Failed || status == operation.Cancelled {
		return PlacementRecord{}, operation.ErrStaleOwner
	}
	// A replay resolves by stored identity before any freshness check.
	committed, found, err := readPlacementTx(ctx, tx, claim.InstallationID, claim.OperationID)
	if err != nil {
		return PlacementRecord{}, err
	}
	if found {
		if committed.RequestHash != requestHash || committed.PlanHash != planHash || !bytes.Equal(committed.CredentialIntentHash, intentHash) {
			return PlacementRecord{}, operation.ErrCommandConflict
		}
		if intent != nil {
			if err := recordClusterIntentTx(ctx, tx, *intent, false); err != nil {
				return PlacementRecord{}, err
			}
			if err := checkPlanCredentials(ctx, tx, claim, plan); err != nil {
				return PlacementRecord{}, err
			}
		}
		return committed, nil
	}
	if intent != nil && (active || existing != nil) {
		return PlacementRecord{}, operation.ErrCommandConflict
	}
	if err := lockSelectedNodes(ctx, tx, claim.InstallationID, decision); err != nil {
		return PlacementRecord{}, err
	}
	snapshot := placement.Snapshot{InstallationID: claim.InstallationID}
	if snapshot.Nodes, err = readPlacementNodes(ctx, tx, claim.InstallationID); err != nil {
		return PlacementRecord{}, err
	}
	if snapshot.Allocations, err = readAllocations(ctx, tx, claim.InstallationID); err != nil {
		return PlacementRecord{}, err
	}
	// Read the time last so that no observed record can sit in the snapshot future.
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&snapshot.ObservedAt); err != nil {
		return PlacementRecord{}, admissionError(err)
	}
	if err := placement.CheckDecision(request, snapshot, decision); err != nil {
		return PlacementRecord{}, fmt.Errorf("%w: %s", ErrPlacementConflict, err)
	}
	var revision int64
	var kind string
	if err := tx.QueryRow(ctx, `SELECT desired_revision,kind FROM operations WHERE id=$1`, claim.OperationID).Scan(&revision, &kind); err != nil {
		return PlacementRecord{}, admissionError(err)
	}
	if revision != request.DesiredRevision {
		return PlacementRecord{}, operation.ErrCommandConflict
	}
	if intent != nil {
		if err := operation.ValidatePlan(plan, operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID, DesiredRevision: revision, Kind: kind}); err != nil {
			return PlacementRecord{}, err
		}
	}
	allocations, err := reserve(ctx, tx, claim, request, decision, snapshot)
	if err != nil {
		return PlacementRecord{}, err
	}
	if strings.HasPrefix(kind, "app.") {
		if err := recordAppCopies(ctx, tx, claim, request, decision, plan); err != nil {
			return PlacementRecord{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE operations SET failure_code=NULL WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.OperationID); err != nil {
			return PlacementRecord{}, admissionError(err)
		}
	}
	if err := recordMembers(ctx, tx, claim, request); err != nil {
		return PlacementRecord{}, err
	}
	if intent != nil {
		if err := recordClusterIntentTx(ctx, tx, *intent, true); err != nil {
			return PlacementRecord{}, err
		}
		if err := checkPlanCredentials(ctx, tx, claim, plan); err != nil {
			return PlacementRecord{}, err
		}
	}
	if _, err := installPlanTx(ctx, tx, claim, plan, encodedPlan); err != nil {
		return PlacementRecord{}, err
	}
	// Lock waits happened above, so ownership is checked once more before the commit.
	if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
		return PlacementRecord{}, err
	}
	var committedAt time.Time
	if err := tx.QueryRow(ctx, `INSERT INTO operation_placements (installation_id,cluster_id,operation_id,request_hash,request_bytes,decision_bytes,plan_hash,credential_intent_hash)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING committed_at`,
		claim.InstallationID, claim.ClusterID, claim.OperationID, requestHash[:], requestBytes, decisionBytes, planHash[:], intentHash).Scan(&committedAt); err != nil {
		return PlacementRecord{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return PlacementRecord{}, err
	}
	return PlacementRecord{Request: request, Decision: decision, Allocations: allocations, RequestHash: requestHash, PlanHash: planHash, CredentialIntentHash: intentHash, CommittedAt: committedAt}, nil
}

// lockSelectedNodes locks every target row in UUID order so writers cannot interleave.
// The node lock protects aggregate capacity and wildcard port overlap.
func lockSelectedNodes(ctx context.Context, tx pgx.Tx, installationID uuid.UUID, decision placement.Decision) error {
	targets := make([]uuid.UUID, 0, len(decision.Assignments))
	for _, assignment := range decision.Assignments {
		if !slices.Contains(targets, assignment.NodeID) {
			targets = append(targets, assignment.NodeID)
		}
	}
	slices.SortFunc(targets, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	rows, err := tx.Query(ctx, `SELECT id FROM nodes WHERE installation_id=$1 AND id = ANY($2) ORDER BY id FOR UPDATE`, installationID, targets)
	if err != nil {
		return admissionError(err)
	}
	locked := 0
	for rows.Next() {
		locked++
	}
	rows.Close()
	if rows.Err() != nil {
		return admissionError(rows.Err())
	}
	if locked != len(targets) {
		return fmt.Errorf("%w: a selected node is not enrolled", ErrPlacementConflict)
	}
	return nil
}

// reserve inserts a new allocation for each assignment and retains any prior one.
// A prior allocation keeps its identity, node, ports, and the larger held budget.
func reserve(ctx context.Context, tx pgx.Tx, claim operation.Claim, request placement.Request, decision placement.Decision, snapshot placement.Snapshot) ([]placement.Allocation, error) {
	held := make(map[uuid.UUID]placement.Allocation, len(snapshot.Allocations))
	for _, allocation := range snapshot.Allocations {
		if !allocation.Released {
			held[allocation.ResourceID] = allocation
		}
	}
	allocations := make([]placement.Allocation, 0, len(decision.Assignments))
	for _, assignment := range decision.Assignments {
		index := slices.IndexFunc(request.Resources, func(r placement.Resource) bool { return r.ID == assignment.ResourceID })
		resource := request.Resources[index]
		budget, err := resource.Budgets.Total()
		if err != nil {
			return nil, &AdmissionError{Code: InvalidRequest, Cause: err}
		}
		allocation := placement.Allocation{
			ID: resource.ReservationID, InstallationID: claim.InstallationID, NodeID: assignment.NodeID,
			ClusterID: claim.ClusterID, ResourceID: resource.ID, CreatedByOperation: claim.OperationID,
			CreatedRevision: request.DesiredRevision, OwnedResourceIDs: resource.OwnedResourceIDs,
			Budget: budget, Ports: assignment.Ports,
		}
		if prior, exists := held[resource.ID]; exists {
			// The caller must name the reservation it is reusing. The decision carries
			// the ports, because a request may ask for a range rather than a number.
			if prior.ID != resource.ReservationID || prior.NodeID != assignment.NodeID || !samePorts(prior.Ports, assignment.Ports) {
				return nil, fmt.Errorf("%w: resource %s is already reserved", ErrPlacementConflict, resource.ID)
			}
			// Keep the larger held budget until explicit release evidence permits a cut.
			// The planner charges a growth, so the stored budget must record it too.
			allocation = prior
			allocation.Budget = larger(prior.Budget, budget)
			if allocation.Budget != prior.Budget {
				encoded, err := json.Marshal(allocation)
				if err != nil || len(encoded) > maxAllocationBytes {
					return nil, &AdmissionError{Code: InvalidRequest, Cause: err}
				}
				if _, err := tx.Exec(ctx, `UPDATE resource_allocations SET allocation_bytes=$1 WHERE installation_id=$2 AND id=$3`,
					encoded, allocation.InstallationID, allocation.ID); err != nil {
					return nil, admissionError(err)
				}
			}
			if err := bindOperation(ctx, tx, claim, prior.ID); err != nil {
				return nil, err
			}
			allocations = append(allocations, allocation)
			continue
		}
		encoded, err := json.Marshal(allocation)
		if err != nil || len(encoded) > maxAllocationBytes {
			return nil, &AdmissionError{Code: InvalidRequest, Cause: err}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO resource_allocations (id,installation_id,node_id,cluster_id,resource_id,created_operation_id,created_revision,allocation_bytes)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, allocation.ID, allocation.InstallationID, allocation.NodeID, allocation.ClusterID,
			allocation.ResourceID, allocation.CreatedByOperation, allocation.CreatedRevision, encoded); err != nil {
			return nil, admissionError(err)
		}
		for _, owned := range allocation.OwnedResourceIDs {
			if _, err := tx.Exec(ctx, `INSERT INTO allocation_resources (installation_id,cluster_id,node_id,allocation_id,resource_id) VALUES ($1,$2,$3,$4,$5)`,
				allocation.InstallationID, allocation.ClusterID, allocation.NodeID, allocation.ID, owned); err != nil {
				return nil, admissionError(err)
			}
		}
		for _, port := range allocation.Ports {
			if err := port.Validate(); err != nil {
				return nil, &AdmissionError{Code: InvalidRequest, Cause: err}
			}
			if _, err := tx.Exec(ctx, `INSERT INTO allocation_ports (installation_id,cluster_id,node_id,allocation_id,address,transport,port,name) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				allocation.InstallationID, allocation.ClusterID, allocation.NodeID, allocation.ID, port.Address, port.Transport, port.Number, port.Name); err != nil {
				return nil, admissionError(err)
			}
		}
		if err := bindOperation(ctx, tx, claim, allocation.ID); err != nil {
			return nil, err
		}
		allocations = append(allocations, allocation)
	}
	return allocations, nil
}

// larger keeps the greater amount in each dimension.
// A reduction needs explicit release evidence, so a held budget never shrinks here.
func larger(held, requested placement.Vector) placement.Vector {
	return placement.Vector{
		CPUMillis:   max(held.CPUMillis, requested.CPUMillis),
		MemoryBytes: max(held.MemoryBytes, requested.MemoryBytes),
		DiskBytes:   max(held.DiskBytes, requested.DiskBytes),
	}
}

// samePorts compares two bindings as sets. Order carries no meaning on a host.
func samePorts(held, requested []placement.Port) bool {
	if len(held) != len(requested) {
		return false
	}
	order := func(a, b placement.Port) int {
		return cmp.Or(cmp.Compare(a.Transport, b.Transport), cmp.Compare(a.Address, b.Address), cmp.Compare(a.Number, b.Number))
	}
	first, second := slices.Clone(held), slices.Clone(requested)
	slices.SortFunc(first, order)
	slices.SortFunc(second, order)
	return slices.Equal(first, second)
}

func bindOperation(ctx context.Context, tx pgx.Tx, claim operation.Claim, allocationID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `INSERT INTO operation_allocation_bindings (installation_id,cluster_id,operation_id,allocation_id)
 VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, claim.InstallationID, claim.ClusterID, claim.OperationID, allocationID); err != nil {
		return admissionError(err)
	}
	return nil
}

// ReadPlacement resolves an uncertain commit by the original operation identity.
// Its absence is not proof that a prior transaction stopped.
func (s *Store) ReadPlacement(ctx context.Context, installationID, operationID uuid.UUID) (PlacementRecord, error) {
	if installationID == uuid.Nil || operationID == uuid.Nil {
		return PlacementRecord{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return PlacementRecord{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	record, found, err := readPlacementTx(ctx, tx, installationID, operationID)
	if err != nil {
		return PlacementRecord{}, err
	}
	if !found {
		return PlacementRecord{}, &AdmissionError{Code: NotFound}
	}
	return record, nil
}

func readPlacementTx(ctx context.Context, tx pgx.Tx, installationID, operationID uuid.UUID) (PlacementRecord, bool, error) {
	var record PlacementRecord
	var requestHash, planHash, requestBytes, decisionBytes []byte
	err := tx.QueryRow(ctx, `SELECT request_hash,request_bytes,decision_bytes,plan_hash,credential_intent_hash,committed_at FROM operation_placements
 WHERE installation_id=$1 AND operation_id=$2`, installationID, operationID).Scan(&requestHash, &requestBytes, &decisionBytes, &planHash, &record.CredentialIntentHash, &record.CommittedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return PlacementRecord{}, false, nil
	}
	if err != nil {
		return PlacementRecord{}, false, admissionError(err)
	}
	copy(record.RequestHash[:], requestHash)
	copy(record.PlanHash[:], planHash)
	if json.Unmarshal(requestBytes, &record.Request) != nil || json.Unmarshal(decisionBytes, &record.Decision) != nil {
		return PlacementRecord{}, false, &AdmissionError{Code: DependencyUnavailable}
	}
	rows, err := tx.Query(ctx, `SELECT a.allocation_bytes, a.released_at IS NOT NULL FROM resource_allocations a
 JOIN operation_allocation_bindings b ON b.installation_id=a.installation_id AND b.cluster_id=a.cluster_id AND b.allocation_id=a.id
 WHERE b.installation_id=$1 AND b.operation_id=$2 ORDER BY a.id`, installationID, operationID)
	if err != nil {
		return PlacementRecord{}, false, admissionError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var encoded []byte
		var released bool
		if err := rows.Scan(&encoded, &released); err != nil {
			return PlacementRecord{}, false, admissionError(err)
		}
		var allocation placement.Allocation
		if json.Unmarshal(encoded, &allocation) != nil {
			return PlacementRecord{}, false, &AdmissionError{Code: DependencyUnavailable}
		}
		allocation.Released = released
		record.Allocations = append(record.Allocations, allocation)
	}
	if rows.Err() != nil {
		return PlacementRecord{}, false, admissionError(rows.Err())
	}
	return record, true, nil
}

// ReleaseUnusedPlacement writes a tombstone for an allocation that no work can need.
// It never deletes a host resource and it never proves a native removal.
//
// The rule is deliberately conservative. Every operation bound to the allocation must
// have failed or been cancelled, the cluster must hold no unfinished operation, and the
// cluster must have dispatched no command at all. InstallPlan can install a plan with no
// placement binding, so a binding-only check would miss that older dispatch history.
// Repeat release is idempotent.
func (s *Store) ReleaseUnusedPlacement(ctx context.Context, installationID, operationID, reservationID uuid.UUID) error {
	if installationID == uuid.Nil || operationID == uuid.Nil || reservationID == uuid.Nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	var clusterID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT cluster_id FROM resource_allocations
 WHERE installation_id=$1 AND id=$2`, installationID, reservationID).Scan(&clusterID); err != nil {
		return admissionError(err)
	}
	// Lock the cluster row before any absence check. An EXISTS that finds nothing locks
	// nothing, so the cluster row is the only object that can serialize this check
	// against a writer that adds an operation. Every future operation writer for an
	// existing cluster must take this same lock. Allocation ownership is immutable;
	// read its release state only after the cluster lock to match lifecycle completion.
	if _, err := tx.Exec(ctx, `SELECT 1 FROM clusters WHERE installation_id=$1 AND id=$2 FOR UPDATE`, installationID, clusterID); err != nil {
		return admissionError(err)
	}
	var released *time.Time
	if err := tx.QueryRow(ctx, `SELECT released_at FROM resource_allocations
 WHERE installation_id=$1 AND cluster_id=$2 AND id=$3 FOR UPDATE`, installationID, clusterID, reservationID).Scan(&released); err != nil {
		return admissionError(err)
	}
	if released != nil {
		return nil
	}
	var unfinished, foreign, dispatched bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM operations
 WHERE installation_id=$1 AND cluster_id=$2 AND status IN ('pending','running','waiting','cancelling'))`, installationID, clusterID).Scan(&unfinished); err != nil {
		return admissionError(err)
	}
	if unfinished {
		return ErrPlacementHeld
	}
	// Every operation that reserved this allocation must have stopped without success.
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM operation_allocation_bindings b
 JOIN operations o ON o.installation_id=b.installation_id AND o.id=b.operation_id
 WHERE b.installation_id=$1 AND b.allocation_id=$2 AND o.status NOT IN ('failed','cancelled'))`, installationID, reservationID).Scan(&foreign); err != nil {
		return admissionError(err)
	}
	if foreign {
		return ErrPlacementHeld
	}
	var bound bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM operation_allocation_bindings
 WHERE installation_id=$1 AND operation_id=$2 AND allocation_id=$3)`, installationID, operationID, reservationID).Scan(&bound); err != nil {
		return admissionError(err)
	}
	if !bound {
		return operation.ErrCommandConflict
	}
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM command_dispatches
 WHERE installation_id=$1 AND cluster_id=$2)`, installationID, clusterID).Scan(&dispatched); err != nil {
		return admissionError(err)
	}
	if dispatched {
		return ErrPlacementHeld
	}
	if _, err := tx.Exec(ctx, `UPDATE resource_allocations SET released_at=clock_timestamp() WHERE installation_id=$1 AND id=$2`, installationID, reservationID); err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE allocation_ports SET released_at=clock_timestamp() WHERE installation_id=$1 AND allocation_id=$2 AND released_at IS NULL`, installationID, reservationID); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}
