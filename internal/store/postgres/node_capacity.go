package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// ErrProbeExpired reports a capacity challenge that can no longer publish evidence.
var ErrProbeExpired = errors.New("capacity_probe_expired")

// ErrPolicyRevision reports a policy write that raced another administrator.
var ErrPolicyRevision = errors.New("policy_revision_conflict")

const maxPolicyBytes = 65536
const maxReportBytes = 262144

// PutNodePlacementPolicy stores operator policy for one enrolled node.
// expected is the revision the caller read. Zero writes the first policy.
// A policy change never moves an allocation and never releases a reservation.
func (s *Store) PutNodePlacementPolicy(ctx context.Context, policy placement.NodePolicy, expected int64) (int64, error) {
	if err := policy.Validate(); err != nil {
		return 0, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	if expected < 0 {
		return 0, &AdmissionError{Code: InvalidRequest}
	}
	stored := policy
	stored.Revision = expected + 1
	encoded, err := json.Marshal(stored)
	if err != nil || len(encoded) > maxPolicyBytes {
		return 0, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, admissionError(err)
	}
	defer tx.Rollback(ctx)
	if err := checkEnrolledNode(ctx, tx, policy.InstallationID, policy.NodeID); err != nil {
		return 0, err
	}
	var current int64
	err = tx.QueryRow(ctx, `SELECT revision FROM node_placement_policies WHERE installation_id=$1 AND node_id=$2`, policy.InstallationID, policy.NodeID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		current = 0
	} else if err != nil {
		return 0, admissionError(err)
	}
	if current != expected {
		return 0, ErrPolicyRevision
	}
	if _, err := tx.Exec(ctx, `INSERT INTO node_placement_policies (installation_id,node_id,revision,policy_bytes) VALUES ($1,$2,$3,$4)
 ON CONFLICT (installation_id,node_id) DO UPDATE SET revision=EXCLUDED.revision,policy_bytes=EXCLUDED.policy_bytes`,
		policy.InstallationID, policy.NodeID, stored.Revision, encoded); err != nil {
		return 0, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return 0, err
	}
	return stored.Revision, nil
}

// BeginCapacityProbe issues one challenge that binds the current agent session.
// The Store chooses the challenge identity and the monotonic probe revision.
// It records issue and expiry with database time, never with caller time.
// The challenge replaces any earlier one and discards its report.
func (s *Store) BeginCapacityProbe(ctx context.Context, installationID, nodeID, sessionID uuid.UUID) (placement.CapacityProbe, error) {
	var probe placement.CapacityProbe
	if installationID == uuid.Nil || nodeID == uuid.Nil || sessionID == uuid.Nil {
		return probe, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return probe, admissionError(err)
	}
	defer tx.Rollback(ctx)
	if err := checkEnrolledNode(ctx, tx, installationID, nodeID); err != nil {
		return probe, err
	}
	boot, generation, err := currentSession(ctx, tx, installationID, nodeID, sessionID)
	if err != nil {
		return probe, err
	}
	var policyRevision int64
	if err := tx.QueryRow(ctx, `SELECT revision FROM node_placement_policies WHERE installation_id=$1 AND node_id=$2 FOR SHARE`, installationID, nodeID).Scan(&policyRevision); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Placement excludes a node with no policy, so a probe would prove nothing.
			return probe, &AdmissionError{Code: NotFound}
		}
		return probe, admissionError(err)
	}
	var previous int64
	err = tx.QueryRow(ctx, `SELECT probe_revision FROM node_capacity WHERE installation_id=$1 AND node_id=$2`, installationID, nodeID).Scan(&previous)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return probe, admissionError(err)
	}
	var issued time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&issued); err != nil {
		return probe, admissionError(err)
	}
	probe = placement.CapacityProbe{
		ID: uuid.New(), InstallationID: installationID, NodeID: nodeID, SessionID: sessionID,
		BootID: boot, JournalGeneration: generation,
		Revision: previous + 1, PolicyRevision: policyRevision, IssuedAt: issued,
		ExpiresAt: issued.Add(placement.ProbeWindow), ValidUntil: issued.Add(placement.CapacityAge),
	}
	encoded, err := json.Marshal(probe)
	if err != nil || len(encoded) > 4096 {
		return placement.CapacityProbe{}, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO node_capacity (installation_id,node_id,probe_revision,probe_id,probe_bytes,report_bytes)
 VALUES ($1,$2,$3,$4,$5,NULL) ON CONFLICT (installation_id,node_id)
 DO UPDATE SET probe_revision=EXCLUDED.probe_revision,probe_id=EXCLUDED.probe_id,probe_bytes=EXCLUDED.probe_bytes,report_bytes=NULL`,
		installationID, nodeID, probe.Revision, probe.ID, encoded); err != nil {
		return placement.CapacityProbe{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return placement.CapacityProbe{}, err
	}
	return probe, nil
}

// RecordNodeCapacity stores synthetic or measured capacity for the exact open challenge.
// The caller must already have verified the peer installation and node identity.
// Presence, Hello capabilities, and agent timestamps cannot substitute for this evidence.
// An exact replay succeeds without refreshing the challenge. A changed answer conflicts.
func (s *Store) RecordNodeCapacity(ctx context.Context, installationID, nodeID uuid.UUID, report placement.CapacityReport) error {
	if err := report.Validate(); err != nil {
		return &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	if report.InstallationID != installationID || report.NodeID != nodeID {
		return operation.ErrStaleOwner
	}
	encoded, err := json.Marshal(report)
	if err != nil || len(encoded) > maxReportBytes {
		return &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	if err := checkEnrolledNode(ctx, tx, installationID, nodeID); err != nil {
		return err
	}
	var probeBytes, storedReport []byte
	if err := tx.QueryRow(ctx, `SELECT probe_bytes,report_bytes FROM node_capacity WHERE installation_id=$1 AND node_id=$2`, installationID, nodeID).Scan(&probeBytes, &storedReport); err != nil {
		return admissionError(err)
	}
	var probe placement.CapacityProbe
	if json.Unmarshal(probeBytes, &probe) != nil {
		return &AdmissionError{Code: DependencyUnavailable}
	}
	if probe.ID != report.ProbeID || probe.Revision != report.ProbeRevision || probe.SessionID != report.SessionID || probe.BootID != report.BootID || probe.JournalGeneration != report.JournalGeneration {
		return ErrProbeExpired
	}
	if storedReport != nil {
		// A replay is idempotent. It never extends the freshness of its challenge.
		if !bytes.Equal(storedReport, encoded) {
			return operation.ErrCommandConflict
		}
		return nil
	}
	// The session must still be the one that the challenge bound.
	if _, _, err := currentSession(ctx, tx, installationID, nodeID, probe.SessionID); err != nil {
		return err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return admissionError(err)
	}
	if !now.Before(probe.ExpiresAt) {
		return ErrProbeExpired
	}
	if _, err := tx.Exec(ctx, `UPDATE node_capacity SET report_bytes=$1 WHERE installation_id=$2 AND node_id=$3`, encoded, installationID, nodeID); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}

// checkEnrolledNode locks one node row and rejects an unknown or revoked identity.
// The lock is exclusive because a policy or probe row may not exist yet, and
// SELECT FOR UPDATE on a missing row locks nothing. The node row is the only
// object that every writer for this node shares, so it serializes the first write.
func checkEnrolledNode(ctx context.Context, tx pgx.Tx, installationID, nodeID uuid.UUID) error {
	var revoked *time.Time
	if err := tx.QueryRow(ctx, `SELECT revoked_at FROM nodes WHERE installation_id=$1 AND id=$2 FOR UPDATE`, installationID, nodeID).Scan(&revoked); err != nil {
		return admissionError(err)
	}
	if revoked != nil {
		return ErrStaleSession
	}
	return nil
}

// currentSession requires the exact live session and returns its boot and journal identity.
// The caller already holds the node row lock, so a shared presence lock is enough here.
// It does not judge presence freshness. A reservation checks that separately.
func currentSession(ctx context.Context, tx pgx.Tx, installationID, nodeID, sessionID uuid.UUID) (uuid.UUID, uuid.UUID, error) {
	var current uuid.UUID
	var helloBytes []byte
	if err := tx.QueryRow(ctx, `SELECT p.session_id,s.hello_bytes FROM agent_presence p
 JOIN agent_sessions s USING (installation_id,node_id,session_id)
 WHERE p.installation_id=$1 AND p.node_id=$2 FOR SHARE OF p`, installationID, nodeID).Scan(&current, &helloBytes); err != nil {
		return uuid.Nil, uuid.Nil, admissionError(err)
	}
	if current != sessionID {
		return uuid.Nil, uuid.Nil, ErrStaleSession
	}
	var hello agentv1.Hello
	if proto.Unmarshal(helloBytes, &hello) != nil {
		return uuid.Nil, uuid.Nil, &AdmissionError{Code: DependencyUnavailable}
	}
	boot, bootErr := uuid.Parse(hello.BootId)
	generation, generationErr := uuid.Parse(hello.JournalGeneration)
	if bootErr != nil || generationErr != nil || boot == uuid.Nil || generation == uuid.Nil {
		return uuid.Nil, uuid.Nil, &AdmissionError{Code: DependencyUnavailable}
	}
	return boot, generation, nil
}

// ReadNodePlacementPolicy answers the policy an operator approved for one node.
func (s *Store) ReadNodePlacementPolicy(ctx context.Context, installationID, nodeID uuid.UUID) (placement.NodePolicy, error) {
	if installationID == uuid.Nil || nodeID == uuid.Nil {
		return placement.NodePolicy{}, &AdmissionError{Code: InvalidRequest}
	}
	var encoded []byte
	if err := s.pool.QueryRow(ctx, `SELECT policy_bytes FROM node_placement_policies
 WHERE installation_id=$1 AND node_id=$2`, installationID, nodeID).Scan(&encoded); err != nil {
		return placement.NodePolicy{}, admissionError(err)
	}
	var policy placement.NodePolicy
	if err := json.Unmarshal(encoded, &policy); err != nil {
		return placement.NodePolicy{}, admissionError(err)
	}
	if policy.InstallationID != installationID || policy.NodeID != nodeID {
		return placement.NodePolicy{}, &AdmissionError{Code: DependencyUnavailable}
	}
	return policy, nil
}
