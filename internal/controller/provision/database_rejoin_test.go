package provision

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func completeDatabaseStep(t *testing.T, f *fixture, claim operation.Claim, step *agentv1.PlanStep) {
	t.Helper()
	command := step.Command
	if err := f.store.RecordDispatch(t.Context(), claim, command); err != nil {
		t.Fatal(err)
	}
	state := map[agentv1.Postcondition]string{agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED: "exited", agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT: "absent", agentv1.Postcondition_POSTCONDITION_APP_DATA_ABSENT: "app_data_absent", agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS: "created", agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING: "running"}[step.Postcondition]
	native := "docker-command:" + command.CommandId
	if command.Input.GetRemoveAppData() != nil {
		native = "app-data:" + command.CommandId
	}
	result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: native, State: state}}}
	if err := f.store.RecordAgentResult(t.Context(), claim.InstallationID, uuid.MustParse(command.Epoch.NodeId), result); err != nil {
		t.Fatal(err)
	}
	encoded, _ := proto.Marshal(result)
	if err := f.store.RecordStepCompletion(t.Context(), claim, uuid.MustParse(command.StepId), result.Epoch, sha256.Sum256(encoded)); err != nil {
		t.Fatal(err)
	}
}

func exerciseDatabaseRejoin(t *testing.T, f *fixture, current operation.Claim, currentPlan *agentv1.ExecutionPlan, first int, writer, returning uuid.UUID, retry bool) {
	t.Helper()
	if first == 0 {
		for _, node := range []uuid.UUID{writer} {
			barrier := &agentv1.BarrierResult{Epoch: epochFor(current, node), AdmissionsClosed: true, Drained: true}
			if err := f.store.RecordBarrier(t.Context(), current, barrier); err != nil {
				t.Fatal(err)
			}
			encoded, _ := proto.Marshal(barrier)
			if err := f.store.ReconcileBarrier(t.Context(), current, node, sha256.Sum256(encoded), "No active effects."); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := f.store.ActivateClaim(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	for _, step := range currentPlan.Steps[first:] {
		completeDatabaseStep(t, f, current, step)
	}
	if err := f.store.UpdateProgress(t.Context(), current, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "The new primary is running."}); err != nil {
		t.Fatal(err)
	}
	installation, id := current.InstallationID, current.AppID
	if _, err := f.pool.Exec(t.Context(), `UPDATE apps SET writer_last_grant_at=clock_timestamp() WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(t.Context(), `UPDATE app_copies SET health='healthy',observed_state='running' WHERE app_id=$1 AND role='active'`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(t.Context(), `UPDATE agent_presence SET last_seen_at=clock_timestamp()-interval '21 seconds' WHERE node_id=$1`, returning); err != nil {
		t.Fatal(err)
	}
	if admitted, err := f.store.AdmitDatabaseRejoin(t.Context(), installation, id); err != nil || admitted != uuid.Nil {
		t.Fatalf("offline rejoin: %v %v", admitted, err)
	}
	if _, err := f.pool.Exec(t.Context(), `UPDATE agent_presence SET last_seen_at=clock_timestamp() WHERE node_id=$1`, returning); err != nil {
		t.Fatal(err)
	}
	admitted, err := f.store.AdmitDatabaseRejoin(t.Context(), installation, id)
	if err != nil || admitted == uuid.Nil {
		t.Fatalf("rejoin admission: %v %v", admitted, err)
	}
	if another, err := f.store.AdmitDatabaseRejoin(t.Context(), installation, id); err != nil || another != uuid.Nil {
		t.Fatalf("duplicate rejoin: %v %v", another, err)
	}
	claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: admitted, RecoveryGeneration: current.RecoveryGeneration, OwnerID: "rejoin-test", TTL: time.Minute, LeaseID: 8, LeaseRevision: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	plan, err := f.store.ReadPlan(t.Context(), claim)
	if err != nil || len(plan.Steps) != 5 {
		t.Fatalf("rebuild plan: %v %v", plan, err)
	}
	var otherBarriers int
	if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM claim_barriers WHERE claim_id=$1 AND node_id<>$2`, claim.ID, returning).Scan(&otherBarriers); err != nil || otherBarriers != 0 {
		t.Fatalf("rejoin touches another device: %d %v", otherBarriers, err)
	}
	barrier := &agentv1.BarrierResult{Epoch: epochFor(claim, returning), AdmissionsClosed: true, Drained: true}
	if err := f.store.RecordBarrier(t.Context(), claim, barrier); err != nil {
		t.Fatal(err)
	}
	encoded, _ := proto.Marshal(barrier)
	if err := f.store.ReconcileBarrier(t.Context(), claim, returning, sha256.Sum256(encoded), "No active effects."); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PrepareDatabaseRejoinStep(t.Context(), claim, uuid.MustParse(plan.Steps[3].Command.StepId)); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("standby assigned before deletion: %v", err)
	}
	tables, err := f.store.AppReplicationTargets(t.Context(), installation)
	if err != nil || len(tables[returning].Apps) != 0 {
		t.Fatalf("old primary assigned: %v %v", tables, err)
	}
	table, acknowledged, err := f.store.PrepareAppReplication(t.Context(), installation, returning, tables[returning])
	if err != nil {
		t.Fatal(err)
	}
	session := uuid.MustParse(table.SessionId)
	if ready, err := f.store.PrepareDatabaseRejoinStep(t.Context(), claim, uuid.MustParse(plan.Steps[0].Command.StepId)); err != nil || ready != acknowledged {
		t.Fatalf("cleanup without withdrawal ack: %v %v", ready, err)
	}
	if err := f.store.RecordAppReplicationAck(t.Context(), installation, returning, session, &agentv1.ReplicationAck{SessionId: table.SessionId, Revision: table.Revision}); err != nil {
		t.Fatal(err)
	}
	for _, step := range plan.Steps[:3] {
		if ready, err := f.store.PrepareDatabaseRejoinStep(t.Context(), claim, uuid.MustParse(step.Command.StepId)); err != nil || !ready {
			t.Fatalf("cleanup: %v %v", ready, err)
		}
		completeDatabaseStep(t, f, claim, step)
	}
	if ready, err := f.store.PrepareDatabaseRejoinStep(t.Context(), claim, uuid.MustParse(plan.Steps[3].Command.StepId)); err != nil || !ready {
		t.Fatalf("removed data: %v %v", ready, err)
	}
	tables, err = f.store.AppReplicationTargets(t.Context(), installation)
	if err != nil || len(tables[returning].Apps) != 1 || tables[returning].Apps[0].Role != "standby" || tables[returning].Apps[0].Epoch != 2 {
		t.Fatalf("fresh standby assignment: %v %v", tables, err)
	}
	copyID := uuid.MustParse(plan.Steps[3].Command.Input.ResourceId)
	if ready, err := f.store.DatabaseStandbyReady(t.Context(), installation, id, returning, session, copyID, 2, plan.Steps[3].Command.DesiredRevision); err != nil || ready {
		t.Fatalf("start before fresh ack: %v %v", ready, err)
	}
	table, _, err = f.store.PrepareAppReplication(t.Context(), installation, returning, tables[returning])
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordAppReplicationAck(t.Context(), installation, returning, session, &agentv1.ReplicationAck{SessionId: table.SessionId, Revision: table.Revision}); err != nil {
		t.Fatal(err)
	}
	if retry {
		if err := f.store.UpdateProgress(t.Context(), claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Failed, Reason: "The start reply was lost."}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET finished_at=clock_timestamp()-interval '11 seconds' WHERE id=$1`, claim.OperationID); err != nil {
			t.Fatal(err)
		}
		admitted, err = f.store.AdmitDatabaseRejoin(t.Context(), installation, id)
		if err != nil || admitted == uuid.Nil {
			t.Fatalf("retry: %v %v", admitted, err)
		}
		claim, err = f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: admitted, RecoveryGeneration: current.RecoveryGeneration, OwnerID: "rejoin-test", TTL: time.Minute, LeaseID: 9, LeaseRevision: 9})
		if err != nil {
			t.Fatal(err)
		}
		if err := f.planner.Plan(t.Context(), claim); err != nil {
			t.Fatal(err)
		}
		plan, err = f.store.ReadPlan(t.Context(), claim)
		if err != nil || len(plan.Steps) != 2 {
			t.Fatalf("retry deletes fresh data: %v %v", plan, err)
		}
		barrier = &agentv1.BarrierResult{Epoch: epochFor(claim, returning), AdmissionsClosed: true, Drained: true}
		if err := f.store.RecordBarrier(t.Context(), claim, barrier); err != nil {
			t.Fatal(err)
		}
		encoded, _ = proto.Marshal(barrier)
		if err := f.store.ReconcileBarrier(t.Context(), claim, returning, sha256.Sum256(encoded), "No active effects."); err != nil {
			t.Fatal(err)
		}
		if err := f.store.ActivateClaim(t.Context(), claim); err != nil {
			t.Fatal(err)
		}
	} else {
		plan.Steps = plan.Steps[3:]
	}
	for _, step := range plan.Steps {
		if ready, err := f.store.PrepareDatabaseRejoinStep(t.Context(), claim, uuid.MustParse(step.Command.StepId)); err != nil || !ready {
			t.Fatalf("restart: %v %v", ready, err)
		}
		completeDatabaseStep(t, f, claim, step)
	}
	if err := f.store.UpdateProgress(t.Context(), claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "The rebuilt standby is running."}); err != nil {
		t.Fatal(err)
	}
	var required bool
	if err := f.pool.QueryRow(t.Context(), `SELECT rebuild_required FROM app_copies WHERE id=$1`, copyID).Scan(&required); err != nil || required {
		t.Fatalf("rebuild state: %v %v", required, err)
	}
	var health string
	if err := f.pool.QueryRow(t.Context(), `SELECT health FROM app_copies WHERE app_id=$1 AND role='active'`, id).Scan(&health); err != nil || health != "healthy" {
		t.Fatalf("rejoin changed writer health: %q %v", health, err)
	}
}
