package postgres

import (
	"context"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/placement"
)

func recoveryPreviewFixture(t *testing.T) (*placementHost, cluster.ActionPlan) {
	t.Helper()
	h := placementFixture(t)
	claim := h.claimCluster(t, "recover-db")
	request, decision, plan := h.place(t, claim, smallBudget, nil)
	volume := declared(h, claim.ClusterID)
	request.Resources[0].OwnedResourceIDs = append(request.Resources[0].OwnedResourceIDs, volume.ResourceID)
	// Placement accounts for the volume under the same reservation.
	snapshot, err := h.store.ReadPlacementSnapshot(t.Context(), h.id)
	if err != nil {
		t.Fatal(err)
	}
	decision, err = placement.Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.RecordVolumeIdentity(t.Context(), volume); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, nil); err != nil {
		t.Fatal(err)
	}
	h.finish(t, claim.OperationID)
	preview, err := h.store.PreviewSetupRecovery(t.Context(), h.id, claim.ClusterID, "actor-1")
	if err != nil {
		t.Fatal(err)
	}
	if preview.MemberID == uuid.Nil || preview.ResourceID != request.Resources[0].ID || !slices.Equal(preview.RetainedVolumes, []uuid.UUID{volume.ResourceID}) || preview.Action != "recover_setup" || preview.ExpectedRevision != 1 || preview.DataDisposition != "retain" {
		t.Fatalf("wrong recovery target: %+v", preview)
	}
	return h, preview
}

func TestSetupRecoveryAdmissionReplaysLostCommit(t *testing.T) {
	h, plan := recoveryPreviewFixture(t)
	request := ClusterActionRequest{Identity: RequestIdentity{InstallationID: h.id, ActorID: "actor-1", APIOperation: RecoverSetupAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(), ClusterID: plan.ClusterID, ExpectedRevision: plan.ExpectedRevision, Confirmation: &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain"}}
	before, err := h.store.GetCluster(t.Context(), h.id, plan.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if before.DesiredRevision != 1 || h.count(t, `SELECT count(*) FROM operations`) != 1 {
		t.Fatal("preview admitted work")
	}
	owner := New(h.store.pool)
	owner.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	_, err = owner.AdmitClusterAction(t.Context(), request)
	requireCode(t, err, OutcomeUnknown)
	first, err := h.store.AdmitClusterAction(t.Context(), request)
	if err != nil || !first.Replayed || first.DesiredRevision != 2 {
		t.Fatalf("lost reply: %+v %v", first, err)
	}
	if _, err := h.store.pool.Exec(t.Context(), `UPDATE cluster_action_plans SET created_at=clock_timestamp()-interval '10 minutes',expires_at=clock_timestamp()-interval '1 minute' WHERE plan_token=$1`, plan.PlanToken); err != nil {
		t.Fatal(err)
	}
	again, err := h.store.AdmitClusterAction(t.Context(), request)
	if err != nil || !again.Replayed || again.OperationID != first.OperationID {
		t.Fatalf("expired replay: %+v %v", again, err)
	}
	var confirmed uuid.UUID
	if err := h.store.pool.QueryRow(t.Context(), `SELECT action_plan_token FROM operations WHERE id=$1`, again.OperationID).Scan(&confirmed); err != nil || confirmed != plan.PlanToken {
		t.Fatalf("recovery replay changed confirmed plan: %s %v", confirmed, err)
	}
	for _, query := range []string{`SELECT count(*) FROM operations WHERE kind='cluster.recover_setup'`, `SELECT count(*) FROM outbox WHERE operation_id=$1`, `SELECT count(*) FROM audit_events WHERE operation_id=$1 AND action='cluster.recover_setup.accepted'`} {
		args := []any{}
		if query != `SELECT count(*) FROM operations WHERE kind='cluster.recover_setup'` {
			args = append(args, first.OperationID)
		}
		if h.count(t, query, args...) != 1 {
			t.Fatalf("not exactly one admission record: %s", query)
		}
	}
	request.Confirmation.PlanToken = uuid.New()
	_, err = h.store.AdmitClusterAction(t.Context(), request)
	requireCode(t, err, IdempotencyConflict)
}

func TestSetupRecoveryRejectsChangedConfirmationAndTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		query  string
		change func(*ClusterActionRequest)
		code   ErrorCode
	}{
		{name: "missing confirmation", change: func(r *ClusterActionRequest) { r.Confirmation = nil }, code: InvalidRequest},
		{name: "not confirmed", change: func(r *ClusterActionRequest) { r.Confirmation.Confirm = false }, code: InvalidRequest},
		{name: "other actor", change: func(r *ClusterActionRequest) { r.Identity.ActorID = "other" }, code: InvalidRequest},
		{name: "stale revision", change: func(r *ClusterActionRequest) { r.ExpectedRevision++ }, code: RevisionConflict},
		{name: "other plan", change: func(r *ClusterActionRequest) { r.Confirmation.PlanToken = uuid.New() }, code: InvalidRequest},
		{name: "stopped", query: `UPDATE clusters SET desired_lifecycle='stopped'`, code: OperationConflict},
		{name: "missing reservation", query: `UPDATE resource_allocations SET released_at=clock_timestamp()`, code: OperationConflict},
		{name: "removed member", query: `UPDATE cluster_members SET removed_at=clock_timestamp()`, code: OperationConflict},
		{name: "changed member", query: `UPDATE cluster_members SET member_id=gen_random_uuid()`, code: OperationConflict},
		{name: "lost volume binding", query: `DELETE FROM allocation_resources WHERE resource_id IN (SELECT resource_id FROM volume_identities)`, code: OperationConflict},
		{name: "unsupported topology", query: `UPDATE clusters SET spec=jsonb_set(spec,'{postgresql,topology}','"replication"')`, code: UnsupportedSpec},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, plan := recoveryPreviewFixture(t)
			r := ClusterActionRequest{Identity: RequestIdentity{InstallationID: h.id, ActorID: "actor-1", APIOperation: RecoverSetupAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(), ClusterID: plan.ClusterID, ExpectedRevision: 1, Confirmation: &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain"}}
			if tc.change != nil {
				tc.change(&r)
			}
			if tc.query != "" {
				if _, err := h.store.pool.Exec(t.Context(), tc.query); err != nil {
					t.Fatal(err)
				}
			}
			_, err := h.store.AdmitClusterAction(t.Context(), r)
			requireCode(t, err, tc.code)
			if h.count(t, `SELECT count(*) FROM operations`) != 1 || h.count(t, `SELECT count(*) FROM outbox`) != 1 || h.count(t, `SELECT count(*) FROM idempotency_results`) != 1 {
				t.Fatal("refused recovery changed admission records")
			}
		})
	}
}

func TestSetupRecoveryCompletionNeedsAllFourReceipts(t *testing.T) {
	h, preview := recoveryPreviewFixture(t)
	admission, err := h.store.AdmitClusterAction(t.Context(), ClusterActionRequest{Identity: RequestIdentity{InstallationID: h.id, ActorID: "actor-1", APIOperation: RecoverSetupAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(), ClusterID: preview.ClusterID, ExpectedRevision: 1, Confirmation: &cluster.ActionConfirmation{PlanToken: preview.PlanToken, Confirm: true, DataDisposition: "retain"}})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.AcquireClaim(t.Context(), ClaimRequest{ClaimID: uuid.New(), InstallationID: h.id, OperationID: admission.OperationID, RecoveryGeneration: h.generation, OwnerID: "recovery-owner", TTL: time.Minute, LeaseID: 1, LeaseRevision: 1})
	if err != nil {
		t.Fatal(err)
	}
	plan := completionPlan(t, claim, h.node, preview.ResourceID, admission.DesiredRevision, "recover_setup")
	if err := h.store.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	reconcilePlan(t, h.store, claim, plan)
	if err := h.store.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	update := ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "All recovery postconditions are proved."}
	before, err := h.store.GetCluster(t.Context(), h.id, preview.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range plan.Steps {
		if err := h.store.UpdateProgress(t.Context(), claim, update); err == nil {
			t.Fatal("completion accepted missing receipt")
		}
		current, err := h.store.GetCluster(t.Context(), h.id, preview.ClusterID)
		if err != nil || current.ObservedRevision != before.ObservedRevision {
			t.Fatal("incomplete recovery published observed revision")
		}
		completeSteps(t, h.store, claim, &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{step}})
	}
	if err := h.store.UpdateProgress(t.Context(), claim, update); err != nil {
		t.Fatal(err)
	}
	after, err := h.store.GetCluster(t.Context(), h.id, preview.ClusterID)
	if err != nil || after.ObservedRevision != admission.DesiredRevision || after.ObservedLifecycle != cluster.ObservedRunning {
		t.Fatalf("completion did not commit: %+v %v", after, err)
	}
	if h.count(t, `SELECT count(*) FROM resource_allocations WHERE released_at IS NULL`) != 1 {
		t.Fatal("recovery released reservation")
	}
}
