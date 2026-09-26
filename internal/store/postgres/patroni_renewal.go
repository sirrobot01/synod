package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// ReadLatestCredentialVersion returns the highest stored version of one credential. A renewal
// uses the next version, because an earlier failed renewal can hold one that no member uses.
func (s *Store) ReadLatestCredentialVersion(ctx context.Context, installationID, clusterID, credentialID uuid.UUID) (int64, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || credentialID == uuid.Nil {
		return 0, &AdmissionError{Code: InvalidRequest}
	}
	var version int64
	if err := s.pool.QueryRow(ctx, `SELECT max(version) FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$2 AND credential_id=$3 HAVING count(*)>0`, installationID, clusterID, credentialID).Scan(&version); err != nil {
		return 0, admissionError(err)
	}
	return version, nil
}

// InstallRenewalPlan records the renewed certificate versions and installs the renewal plan
// in one transaction. Every credential in the intent must be a new version of an existing
// credential, and the plan must reference each one.
func (s *Store) InstallRenewalPlan(ctx context.Context, claim operation.Claim, plan *agentv1.ExecutionPlan, intent ClusterIntent) error {
	if intent.InstallationID != claim.InstallationID || intent.ClusterID != claim.ClusterID {
		return ErrCredentialConflict
	}
	if err := intent.validate(); err != nil {
		return err
	}
	encoded, err := validateInstalledPlan(claim, plan)
	if err != nil {
		return err
	}
	for _, credential := range intent.Credentials {
		if credential.Purpose != PatroniMemberTLSCredential && credential.Purpose != PatroniDCSTLSCredential || credential.Version < 2 {
			return ErrCredentialConflict
		}
		if !planReferences(plan, credential) {
			return ErrCredentialConflict
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT 1 FROM clusters WHERE installation_id=$1 AND id=$2 FOR UPDATE`, claim.InstallationID, claim.ClusterID); err != nil {
		return admissionError(err)
	}
	op := operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID}
	if err := tx.QueryRow(ctx, `SELECT kind,desired_revision FROM operations WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.OperationID).Scan(&op.Kind, &op.DesiredRevision); err != nil {
		return admissionError(err)
	}
	if op.Kind != "cluster.renew_certificates" {
		return operation.ErrCommandConflict
	}
	if err := operation.ValidatePlan(plan, op); err != nil {
		return err
	}
	for _, credential := range intent.Credentials {
		if err := priorVersion(ctx, tx, claim, credential); err != nil {
			return err
		}
	}
	if err := recordClusterIntentTx(ctx, tx, intent, true); err != nil {
		return err
	}
	if err := checkPlanCredentials(ctx, tx, claim, plan); err != nil {
		return err
	}
	changed, err := installPlanTx(ctx, tx, claim, plan, encoded)
	if err != nil || !changed {
		return err
	}
	return s.commitMetadata(ctx, tx)
}

// planReferences reports whether some step names the exact credential version.
func planReferences(plan *agentv1.ExecutionPlan, credential Credential) bool {
	for _, step := range plan.Steps {
		for _, reference := range step.Command.Input.Secrets {
			if reference.CredentialId == credential.CredentialID.String() && reference.Version == credential.Version && reference.Purpose == credential.Purpose {
				return true
			}
		}
	}
	return false
}

// priorVersion requires that a renewed credential version continues a credential this cluster
// already holds for the same purpose and member.
func priorVersion(ctx context.Context, tx pgx.Tx, claim operation.Claim, credential Credential) error {
	var prior bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$2 AND credential_id=$3 AND version=$4 AND purpose=$5
 AND member_id IS NOT DISTINCT FROM NULLIF($6::uuid,'00000000-0000-0000-0000-000000000000'))`,
		claim.InstallationID, claim.ClusterID, credential.CredentialID, credential.Version-1, credential.Purpose, credential.MemberID).Scan(&prior); err != nil {
		return admissionError(err)
	}
	if !prior {
		return ErrCredentialConflict
	}
	return nil
}

// recordRecreated points each member whose recreate step completed at this operation's plan,
// which holds the member's new configuration, even when a later step failed. A member without
// that proof keeps its prior configuration. Success needs the proof for every recreate step.
func recordRecreated(ctx context.Context, tx pgx.Tx, claim operation.Claim, op operation.Operation, plan *agentv1.ExecutionPlan, succeeded bool) error {
	for _, step := range plan.Steps {
		if step.Command.Input.GetRecreatePatroniMember() == nil {
			continue
		}
		_, _, err := checkPlanCompletions(ctx, tx, op, &agentv1.ExecutionPlan{Version: plan.Version, Steps: []*agentv1.PlanStep{step}})
		if errors.Is(err, operation.ErrBarrierPending) && !succeeded {
			continue
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE cluster_members SET container_operation_id=$1,updated_at=clock_timestamp() WHERE installation_id=$2 AND cluster_id=$3 AND resource_id=$4 AND removed_at IS NULL`,
			claim.OperationID, claim.InstallationID, claim.ClusterID, step.Command.Input.ResourceId); err != nil {
			return admissionError(err)
		}
	}
	return nil
}

// settleRenewal records which members run their renewed containers at a terminal boundary.
//
// A member whose recreate step completed runs the configuration of this renewal, even when a
// later step failed. A member without that proof keeps its prior configuration. Success also
// needs every step and, for a running cluster, exactly one writer among the final checks.
func settleRenewal(ctx context.Context, tx pgx.Tx, claim operation.Claim, encoded []byte, succeeded bool) error {
	op := operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID}
	var lifecycle string
	if err := tx.QueryRow(ctx, `SELECT o.kind,o.desired_revision,o.plan_version,c.desired_lifecycle FROM operations o JOIN clusters c ON c.installation_id=o.installation_id AND c.id=o.cluster_id
 WHERE o.installation_id=$1 AND o.id=$2`, claim.InstallationID, claim.OperationID).Scan(&op.Kind, &op.DesiredRevision, &op.PlanVersion, &lifecycle); err != nil {
		return admissionError(err)
	}
	if op.Kind != "cluster.renew_certificates" {
		return nil
	}
	if len(encoded) == 0 {
		if succeeded {
			return operation.ErrBarrierPending
		}
		return nil
	}
	if len(encoded) > maxExecutionStateBytes {
		return ErrExecutionStateTooLarge
	}
	var plan agentv1.ExecutionPlan
	if proto.Unmarshal(encoded, &plan) != nil {
		return operation.ErrCommandConflict
	}
	if err := operation.ValidatePlan(&plan, op); err != nil {
		return err
	}
	if int(plan.Version) != op.PlanVersion {
		return operation.ErrCommandConflict
	}
	if succeeded {
		if _, _, err := checkPlanCompletions(ctx, tx, op, &plan); err != nil {
			return err
		}
		// A running renewal ends its member steps with a final check of every member. Route steps
		// of a routed cluster follow them. A stopped renewal only recreates.
		if prefix, members := operation.RenewalPrefix(&plan), operation.LeadingChecks(plan.Steps)+1; members > 1 {
			elected, primaries, err := checkPlanCompletions(ctx, tx, op, &agentv1.ExecutionPlan{Version: plan.Version, Steps: plan.Steps[prefix-members : prefix]})
			if err != nil {
				return err
			}
			if elected != members || primaries != 1 {
				return operation.ErrPostconditionUnproved
			}
		}
	}
	if err := recordRecreated(ctx, tx, claim, op, &plan, succeeded); err != nil {
		return err
	}
	if succeeded {
		if _, err := tx.Exec(ctx, `UPDATE clusters SET observed_revision=$1,observed_lifecycle=$2,updated_at=clock_timestamp() WHERE installation_id=$3 AND id=$4`, op.DesiredRevision, lifecycle, claim.InstallationID, claim.ClusterID); err != nil {
			return admissionError(err)
		}
	}
	return nil
}

// CertificateExpiry is the earliest certificate expiry that a Patroni cluster's members use.
type CertificateExpiry struct {
	ClusterID       uuid.UUID
	DesiredRevision int64
	ExpiresAt       time.Time
}

// renewalRetryDelay keeps a failed renewal from being admitted again at every scan.
const renewalRetryDelay = time.Hour

// ListCertificateExpiries returns each Patroni cluster that a renewal could act on now, with the
// earliest expiry among the certificates its members' current configurations reference.
//
// A cluster qualifies with three active members, no active operation, and no renewal that
// failed within the retry delay. The caller decides which expiries are close enough.
func (s *Store) ListCertificateExpiries(ctx context.Context, installationID uuid.UUID) ([]CertificateExpiry, error) {
	if installationID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	rows, err := s.pool.Query(ctx, `SELECT c.id,c.desired_revision FROM clusters c
 WHERE c.installation_id=$1 AND c.deleted_at IS NULL AND c.desired_lifecycle IN ('running','stopped')
 AND c.spec->'postgresql'->>'topology'='patroni'
 AND NOT EXISTS (SELECT 1 FROM operations o WHERE o.installation_id=c.installation_id AND o.cluster_id=c.id AND o.status IN ('pending','running','waiting','cancelling'))
 AND NOT EXISTS (SELECT 1 FROM operations o WHERE o.installation_id=c.installation_id AND o.cluster_id=c.id AND o.kind='cluster.renew_certificates'
  AND o.status IN ('failed','cancelled') AND o.updated_at>clock_timestamp()-$2::interval)
 AND (SELECT count(*) FILTER (WHERE m.role='patroni' AND m.state='active') FROM cluster_members m WHERE m.installation_id=c.installation_id AND m.cluster_id=c.id AND m.removed_at IS NULL)=3
 AND NOT EXISTS (SELECT 1 FROM cluster_members m WHERE m.installation_id=c.installation_id AND m.cluster_id=c.id AND m.removed_at IS NULL AND (m.role<>'patroni' OR m.state<>'active'))
 ORDER BY c.id`, installationID, renewalRetryDelay.String())
	if err != nil {
		return nil, admissionError(err)
	}
	var found []CertificateExpiry
	for rows.Next() {
		var each CertificateExpiry
		if err := rows.Scan(&each.ClusterID, &each.DesiredRevision); err != nil {
			rows.Close()
			return nil, admissionError(err)
		}
		found = append(found, each)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, admissionError(err)
	}
	for index := range found {
		each := &found[index]
		members, err := s.ReadClusterMembers(ctx, installationID, each.ClusterID)
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			container, err := s.ReadMemberContainer(ctx, installationID, each.ClusterID, member.ResourceID)
			if err != nil {
				return nil, err
			}
			for _, reference := range container.GetPostgres().GetPatroni().GetCredentials() {
				if reference.Purpose != PatroniMemberTLSCredential && reference.Purpose != PatroniDCSTLSCredential {
					continue
				}
				var expires time.Time
				if err := s.pool.QueryRow(ctx, `SELECT expires_at FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$2 AND credential_id=$3 AND version=$4 AND expires_at IS NOT NULL`,
					installationID, each.ClusterID, reference.CredentialId, reference.Version).Scan(&expires); err != nil {
					return nil, admissionError(err)
				}
				if each.ExpiresAt.IsZero() || expires.Before(each.ExpiresAt) {
					each.ExpiresAt = expires
				}
			}
		}
	}
	return found, nil
}
