package provision

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDatabaseMoveRequiresHeldTargetAndStoppedSource(t *testing.T) {
	for _, outcome := range []string{"postgres", "redis", "rejoin_retry", "rollback", "target_lost", "timeout"} {
		t.Run(outcome, func(t *testing.T) {
			spec := app.DatabaseSpec{Engine: "postgres", Version: "17", Databases: []string{"photos"}}
			if outcome == "redis" {
				spec = app.DatabaseSpec{Engine: "redis", Version: "8"}
			}
			f, target, request := databaseFixture(t, spec)
			installation := f.claim.InstallationID
			created, err := f.store.AdmitApp(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: created.OperationID, RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "database-move", TTL: time.Minute, LeaseID: 2, LeaseRevision: 2})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			deployed, err := f.store.ReadPlan(t.Context(), claim)
			if err != nil {
				t.Fatal(err)
			}
			for _, node := range []uuid.UUID{f.node, target} {
				barrier := &agentv1.BarrierResult{Epoch: epochFor(claim, node), AdmissionsClosed: true, Drained: true}
				if err := f.store.RecordBarrier(t.Context(), claim, barrier); err != nil {
					t.Fatal(err)
				}
				encoded, _ := proto.Marshal(barrier)
				if err := f.store.ReconcileBarrier(t.Context(), claim, node, sha256.Sum256(encoded), "No active effects."); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.store.ActivateClaim(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			for _, step := range deployed.Steps {
				if step.Command.Input.GetEnsureContainer() != nil {
					if err := f.store.RecordDispatch(t.Context(), claim, step.Command); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='succeeded',claim_active=false,finished_at=clock_timestamp() WHERE id=$1`, claim.OperationID); err != nil {
				t.Fatal(err)
			}
			tables, err := f.store.AppReplicationTargets(t.Context(), installation)
			if err != nil {
				t.Fatal(err)
			}
			normal := proto.Clone(tables[target]).(*agentv1.ReplicationTable)
			table, _, err := f.store.PrepareAppReplication(t.Context(), installation, target, normal)
			if err != nil {
				t.Fatal(err)
			}
			session := uuid.MustParse(table.SessionId)
			if err := f.store.RecordAppReplicationAck(t.Context(), installation, target, session, &agentv1.ReplicationAck{SessionId: table.SessionId, Revision: table.Revision}); err != nil {
				t.Fatal(err)
			}
			action := postgres.AppActionRequest{Identity: postgres.RequestIdentity{InstallationID: installation, ActorID: "database-move", APIOperation: postgres.MoveAppAPIOperation, IdempotencyKey: "move"}, RequestID: "move", AppID: created.AppID, ExpectedRevision: 1, TargetNodeID: target}
			if _, err := f.store.AdmitAppAction(t.Context(), action); err == nil {
				t.Fatal("incomplete standby admitted")
			}
			heartbeat := &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.Now(), Replicas: []*agentv1.ReplicaStatus{{AppId: created.AppID.String(), Role: "standby", Epoch: 1, Sequence: 1, CompletedAt: timestamppb.Now(), State: "streaming"}}}
			if err := f.store.RecordAgentHeartbeat(t.Context(), installation, target, session, heartbeat); err != nil {
				t.Fatal(err)
			}
			moved, err := f.store.AdmitAppAction(t.Context(), action)
			if err != nil {
				t.Fatal(err)
			}
			claim, err = f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: moved.OperationID, RecoveryGeneration: claim.RecoveryGeneration, OwnerID: "database-move", TTL: time.Minute, LeaseID: 3, LeaseRevision: 3})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatalf("plan: %v (%v)", err, errors.Unwrap(err))
			}
			plan, err := f.store.ReadPlan(t.Context(), claim)
			if err != nil || len(plan.Steps) != 3 {
				t.Fatalf("plan: %v %v", plan, err)
			}
			if plan.Steps[0].Command.Input.GetSwitchDatabase() == nil || plan.Steps[0].Command.Input.Timeout.AsDuration() > time.Minute || len(plan.Steps[1].Command.Input.Secrets) == 0 {
				t.Fatalf("database plan: %v", plan)
			}
			for _, node := range []uuid.UUID{f.node, target} {
				barrier := &agentv1.BarrierResult{Epoch: epochFor(claim, node), AdmissionsClosed: true, Drained: true}
				if err := f.store.RecordBarrier(t.Context(), claim, barrier); err != nil {
					t.Fatal(err)
				}
				encoded, _ := proto.Marshal(barrier)
				if err := f.store.ReconcileBarrier(t.Context(), claim, node, sha256.Sum256(encoded), "No active effects."); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.store.ActivateClaim(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.store.PrepareAppReplication(t.Context(), installation, target, normal); !errors.Is(err, operation.ErrCommandConflict) {
				t.Fatalf("stale normal assignment: %v", err)
			}
			tables, err = f.store.AppReplicationTargets(t.Context(), installation)
			if err != nil {
				t.Fatal(err)
			}
			held := tables[target]
			if held.Apps[0].Database.SwitchoverOperationId != claim.OperationID.String() {
				t.Fatalf("missing hold: %v", held)
			}
			table, _, err = f.store.PrepareAppReplication(t.Context(), installation, target, held)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.store.RecordAppReplicationAck(t.Context(), installation, target, session, &agentv1.ReplicationAck{SessionId: table.SessionId, Revision: table.Revision}); err != nil {
				t.Fatal(err)
			}
			sourceStep := uuid.MustParse(plan.Steps[0].Command.StepId)
			targetStep := uuid.MustParse(plan.Steps[1].Command.StepId)
			if ready, err := f.store.PrepareAppMoveStep(t.Context(), claim, sourceStep); ready || err != nil {
				t.Fatalf("ack without hold report: %v %v", ready, err)
			}
			// A report from the old supervisor cannot prove the new hold.
			heartbeat.Sequence++
			heartbeat.Replicas[0].Sequence++
			if err := f.store.RecordAgentHeartbeat(t.Context(), installation, target, session, heartbeat); err != nil {
				t.Fatal(err)
			}
			if ready, err := f.store.PrepareAppMoveStep(t.Context(), claim, sourceStep); ready || err != nil {
				t.Fatalf("old supervisor report: %v %v", ready, err)
			}
			heartbeat.Sequence++
			heartbeat.Replicas[0].Sequence++
			heartbeat.Replicas[0].DatabaseHoldId = claim.OperationID.String()
			heartbeat.Replicas[0].CompletedAt = timestamppb.New(time.Now().Add(-time.Minute))
			if err := f.store.RecordAgentHeartbeat(t.Context(), installation, target, session, heartbeat); err != nil {
				t.Fatal(err)
			}
			if ready, err := f.store.PrepareAppMoveStep(t.Context(), claim, sourceStep); ready || err != nil {
				t.Fatalf("cached hold report: %v %v", ready, err)
			}
			heartbeat.Sequence++
			heartbeat.Replicas[0].Sequence++
			heartbeat.Replicas[0].CompletedAt = timestamppb.Now()
			if err := f.store.RecordAgentHeartbeat(t.Context(), installation, target, session, heartbeat); err != nil {
				t.Fatal(err)
			}
			if ready, err := f.store.PrepareAppMoveStep(t.Context(), claim, sourceStep); !ready || err != nil {
				t.Fatalf("held target: %v %v", ready, err)
			}
			if _, err := f.store.PrepareAppMoveStep(t.Context(), claim, targetStep); !errors.Is(err, operation.ErrBarrierPending) {
				t.Fatalf("promotion without source proof: %v", err)
			}
			if outcome != "rollback" {
				command := plan.Steps[0].Command
				if err := f.store.RecordDispatch(t.Context(), claim, command); err != nil {
					t.Fatal(err)
				}
				result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "database-switch:" + command.CommandId, State: "database_switchover_ready", DatabaseSwitchover: &agentv1.DatabaseSwitchoverReceipt{WriterEpoch: 1, TargetNodeId: target.String(), PositionHash: make([]byte, 32)}}}}
				if err := f.store.RecordAgentResult(t.Context(), installation, f.node, result); err != nil {
					t.Fatal(err)
				}
				encoded, _ := proto.Marshal(result)
				if err := f.store.RecordStepCompletion(t.Context(), claim, sourceStep, result.Epoch, sha256.Sum256(encoded)); err != nil {
					t.Fatal(err)
				}
			}
			if outcome == "target_lost" || outcome == "timeout" {
				if outcome == "target_lost" {
					_, err = f.pool.Exec(t.Context(), `UPDATE agent_presence SET last_seen_at=clock_timestamp()-interval '21 seconds' WHERE node_id=$1`, target)
				} else {
					_, err = f.pool.Exec(t.Context(), `UPDATE operation_step_completions SET created_at=clock_timestamp()-interval '1 hour' WHERE operation_id=$1`, claim.OperationID)
				}
				if err != nil {
					t.Fatal(err)
				}
				ready, e := f.store.PrepareAppMoveStep(t.Context(), claim, targetStep)
				if ready || (outcome == "timeout" && !errors.Is(e, postgres.ErrAppMoveTargetLost)) || (outcome == "target_lost" && e != nil) {
					t.Fatalf("lost target: %v %v", ready, e)
				}
			}
			if outcome == "rollback" || outcome == "target_lost" || outcome == "timeout" {
				if err := f.store.UpdateProgress(t.Context(), claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Failed, Reason: "Catch-up failed."}); err != nil {
					t.Fatal(err)
				}
				if err := f.store.ReconcileAppMove(t.Context(), installation, created.AppID); err != nil {
					t.Fatal(err)
				}
				var restart uuid.UUID
				if err := f.pool.QueryRow(t.Context(), `SELECT id FROM operations WHERE app_id=$1 AND kind='app.start' AND status='pending'`, created.AppID).Scan(&restart); err != nil {
					t.Fatal(err)
				}
				rollback, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: restart, RecoveryGeneration: claim.RecoveryGeneration, OwnerID: "database-move", TTL: time.Minute, LeaseID: 4, LeaseRevision: 4})
				if err != nil {
					t.Fatal(err)
				}
				if err := f.planner.Plan(t.Context(), rollback); err != nil {
					t.Fatal(err)
				}
				var lostBarriers int
				if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM claim_barriers WHERE claim_id=$1 AND node_id<>$2`, rollback.ID, f.node).Scan(&lostBarriers); err != nil || lostBarriers != 0 {
					t.Fatalf("rollback requires lost target barrier: %d %v", lostBarriers, err)
				}
				restored, err := f.store.ReadPlan(t.Context(), rollback)
				if err != nil || len(restored.Steps) != 3 {
					t.Fatalf("rollback: %v %v", restored, err)
				}
				for _, step := range restored.Steps {
					if step.Command.Epoch.NodeId != f.node.String() {
						t.Fatal("rollback waits for lost target")
					}
				}
				writer, err := f.store.GetAppWriter(t.Context(), installation, created.AppID)
				if err != nil || writer.Epoch != 1 || writer.NodeID != f.node {
					t.Fatalf("rollback writer: %v %v", writer, err)
				}
			} else {
				// Source loss is expected after its completed stop.
				heartbeat.Sequence++
				heartbeat.Replicas[0].Sequence++
				heartbeat.Replicas[0].State = "disconnected"
				heartbeat.Replicas[0].CompletedAt = timestamppb.Now()
				if err := f.store.RecordAgentHeartbeat(t.Context(), installation, target, session, heartbeat); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					if ready, err := f.store.PrepareAppMoveStep(t.Context(), claim, targetStep); err != nil || !ready {
						t.Fatalf("promotion: %v %v", ready, err)
					}
				}
				writer, err := f.store.GetAppWriter(t.Context(), installation, created.AppID)
				if err != nil || writer.Epoch != 2 || writer.NodeID != target {
					t.Fatalf("writer: %v %v", writer, err)
				}
				status, err := f.store.GetAppStatus(t.Context(), installation, created.AppID)
				if err != nil || status.Status.LossWindowSeconds == nil || *status.Status.LossWindowSeconds != 0 {
					t.Fatalf("loss window: %v %v", status, err)
				}
				exerciseDatabaseRejoin(t, f, claim, plan, 1, target, f.node, outcome == "rejoin_retry")
			}
			if _, _, err := f.store.PrepareAppReplication(t.Context(), installation, target, held); !errors.Is(err, operation.ErrCommandConflict) {
				t.Fatalf("stale hold: %v", err)
			}
			snapshot, err := f.store.ReadPlacementSnapshot(t.Context(), installation)
			if err != nil {
				t.Fatal(err)
			}
			copies, err := f.store.GetAppCopies(t.Context(), installation, created.AppID)
			if err != nil {
				t.Fatal(err)
			}
			for _, copy := range copies {
				found := false
				for _, allocation := range snapshot.Allocations {
					if allocation.ResourceID == copy.ID && !allocation.Released {
						found = true
						if allocation.Budget.MemoryBytes <= 0 || allocation.Budget.CPUMillis <= 0 {
							t.Fatalf("lost engine budget: %v", allocation)
						}
					}
				}
				if !found {
					t.Fatal("copy has no allocation")
				}
			}

		})
	}
}
