package provision

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	imageinspect "github.com/sirrobot01/synod/internal/runtime/image"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAppMoveRequiresFinalProofAndKeepsRollbackBoundary(t *testing.T) {
	for _, outcome := range []string{"success", "rollback", "stop_failure", "promoted_failure", "new_target", "retired_target", "target_lost", "report_timeout"} {
		t.Run(outcome, func(t *testing.T) {
			f := newFixture(t, postgresSpec())
			installation := f.claim.InstallationID
			target := uuid.New()
			if _, err := f.pool.Exec(t.Context(), `INSERT INTO nodes(id,installation_id,public_key_hash,enrolled_at) VALUES($1,$2,$3,clock_timestamp())`, target, installation, make([]byte, 32)); err != nil {
				t.Fatal(err)
			}
			readyNodeWith(t, f.store, installation, target, func(p *placement.NodePolicy) {
				p.Architecture = "arm64"
				p.Engines = nil
				p.ClientAddress = "192.0.2.22"
			})
			r := app.DefaultReplication()
			r.FailbackStableSeconds = 60
			created, err := f.store.AdmitApp(t.Context(), postgres.CreateAppRequest{Identity: postgres.RequestIdentity{InstallationID: installation, ActorID: "move-test", APIOperation: postgres.CreateAppAPIOperation, IdempotencyKey: "create"}, RequestID: "create", Spec: app.AppSpec{SchemaVersion: 1, Name: "writer", Tier: app.TierSingleWriter, Copies: 1, Replication: &r, DevicePreference: app.DevicePreference{Preferred: []string{f.node.String()}}, Service: app.ServiceSpec{Image: "example", Ports: []app.PortSpec{{ContainerPort: 80}}, Mounts: []app.MountSpec{{Type: "replicated", Source: "config", Target: "/config"}}}, Router: app.RouterSpec{Protocol: "tcp", Port: 8080}}})
			if err != nil {
				t.Fatal(err)
			}
			f.planner.config.ResolveImage = func(_ context.Context, s app.ServiceSpec) (imageinspect.Resolution, error) {
				s.Image = "example@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
				s.SupportedArchitectures = []string{"amd64", "arm64"}
				return imageinspect.Resolution{Service: s}, nil
			}
			claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: created.OperationID, RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "move-test", TTL: time.Minute, LeaseID: 2, LeaseRevision: 2})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='succeeded',claim_active=false,finished_at=clock_timestamp() WHERE id=$1`, claim.OperationID); err != nil {
				t.Fatal(err)
			}
			if outcome == "new_target" {
				target = uuid.New()
				if _, err := f.pool.Exec(t.Context(), `INSERT INTO nodes(id,installation_id,public_key_hash,enrolled_at) VALUES($1,$2,$3,clock_timestamp())`, target, installation, make([]byte, 32)); err != nil {
					t.Fatal(err)
				}
				readyNodeWith(t, f.store, installation, target, func(p *placement.NodePolicy) {
					p.Architecture = "arm64"
					p.Engines = nil
					p.ClientAddress = "192.0.2.23"
				})
			}
			if outcome == "retired_target" {
				if _, err := f.pool.Exec(t.Context(), `UPDATE app_copies SET desired=false WHERE app_id=$1 AND node_id=$2`, created.AppID, target); err != nil {
					t.Fatal(err)
				}
			}
			request := postgres.AppActionRequest{Identity: postgres.RequestIdentity{InstallationID: installation, ActorID: "move-test", APIOperation: postgres.MoveAppAPIOperation, IdempotencyKey: "move"}, RequestID: "move", AppID: created.AppID, ExpectedRevision: 1, TargetNodeID: target}
			moved, err := f.store.AdmitAppAction(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := f.store.AdmitAppAction(t.Context(), request)
			if err != nil || !replay.Replayed || replay.OperationID != moved.OperationID {
				t.Fatalf("admission replay: %+v %v", replay, err)
			}
			changed := request
			changed.TargetNodeID = f.node
			if _, err := f.store.AdmitAppAction(t.Context(), changed); err == nil {
				t.Fatal("changed target replayed")
			}
			claim, err = f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: moved.OperationID, RecoveryGeneration: claim.RecoveryGeneration, OwnerID: "move-test", TTL: time.Minute, LeaseID: 3, LeaseRevision: 3})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatalf("move plan: %v (%v)", err, errors.Unwrap(err))
			}
			plan, err := f.store.ReadPlan(t.Context(), claim)
			if err != nil || len(plan.Steps) != 4 {
				t.Fatalf("move plan: %v %v", plan, err)
			}
			if plan.Steps[0].Command.Epoch.NodeId != f.node.String() || plan.Steps[2].Command.Epoch.NodeId != target.String() {
				t.Fatal("wrong move nodes")
			}
			for _, node := range []uuid.UUID{f.node, target} {
				report := &agentv1.BarrierResult{Epoch: epochFor(claim, node), AdmissionsClosed: true, Drained: true}
				if err := f.store.RecordBarrier(t.Context(), claim, report); err != nil {
					t.Fatal(err)
				}
				encoded, _ := proto.Marshal(report)
				if err := f.store.ReconcileBarrier(t.Context(), claim, node, sha256.Sum256(encoded), "No active effects."); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.store.ActivateClaim(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			heartbeats := map[uuid.UUID]uint64{}
			publish := func(sequence uint64, epoch uint64) {
				t.Helper()
				tables, err := f.store.AppReplicationTargets(t.Context(), installation)
				if err != nil {
					t.Fatal(err)
				}
				for node, desired := range tables {
					table, _, err := f.store.PrepareAppReplication(t.Context(), installation, node, desired)
					if err != nil {
						t.Fatal(err)
					}
					session := uuid.MustParse(table.SessionId)
					if err := f.store.RecordAppReplicationAck(t.Context(), installation, node, session, &agentv1.ReplicationAck{SessionId: table.SessionId, Revision: table.Revision}); err != nil {
						t.Fatal(err)
					}
					heartbeats[node]++
					h := &agentv1.Heartbeat{Sequence: heartbeats[node], ObservedAt: timestamppb.Now(), WriterClockNanos: uint64(time.Hour)}
					if node == target && sequence > 0 {
						h.Replicas = []*agentv1.ReplicaStatus{{AppId: created.AppID.String(), Role: "standby", Epoch: epoch, Sequence: sequence, CompletedAt: timestamppb.Now()}}
					}
					if err := f.store.RecordAgentHeartbeat(t.Context(), installation, node, session, h); err != nil {
						t.Fatal(err)
					}
				}
			}
			publish(0, 1)
			if ready, err := f.store.PrepareAppMoveStep(t.Context(), claim, uuid.MustParse(plan.Steps[0].Command.StepId)); err != nil || ready {
				t.Fatalf("stopped without initial copy: %v %v", ready, err)
			}
			publish(4, 1)
			if ready, err := f.store.PrepareAppMoveStep(t.Context(), claim, uuid.MustParse(plan.Steps[0].Command.StepId)); err != nil || !ready {
				t.Fatalf("initial copy rejected: %v %v", ready, err)
			}
			if _, err := f.store.PrepareAppMoveStep(t.Context(), claim, uuid.MustParse(plan.Steps[2].Command.StepId)); !errors.Is(err, operation.ErrBarrierPending) {
				t.Fatalf("promoted without stop/final receipts: %v", err)
			}
			complete := func(index int) {
				t.Helper()
				step := plan.Steps[index]
				command := step.Command
				if err := f.store.RecordDispatch(t.Context(), claim, command); err != nil {
					t.Fatal(err)
				}
				effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: []string{"exited", "snapshot_transferred", "created", "running"}[index]}
				if index == 1 {
					effect.NativeOperationId = "app-data:" + command.CommandId
					effect.AppSnapshot = &agentv1.AppSnapshotReceipt{WriterEpoch: 1, Sequence: 5, TargetNodeId: target.String(), ManifestHash: make([]byte, 32)}
				}
				result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: []*agentv1.Effect{effect}}
				if err := f.store.RecordAgentResult(t.Context(), installation, uuid.MustParse(command.Epoch.NodeId), result); err != nil {
					t.Fatal(err)
				}
				encoded, _ := proto.Marshal(result)
				if err := f.store.RecordStepCompletion(t.Context(), claim, uuid.MustParse(command.StepId), result.Epoch, sha256.Sum256(encoded)); err != nil {
					t.Fatal(err)
				}
			}
			if outcome != "stop_failure" {
				complete(0)
			}
			if outcome == "target_lost" || outcome == "report_timeout" {
				complete(1)
				if outcome == "target_lost" {
					_, err = f.pool.Exec(t.Context(), `UPDATE agent_presence SET last_seen_at=clock_timestamp()-interval '21 seconds' WHERE installation_id=$1 AND node_id=$2`, installation, target)
				} else {
					_, err = f.pool.Exec(t.Context(), `UPDATE operation_step_completions SET created_at=clock_timestamp()-($2 * interval '1 second') WHERE operation_id=$1`, claim.OperationID, plan.Steps[2].Command.Input.Timeout.AsDuration().Seconds()+1)
				}
				if err != nil {
					t.Fatal(err)
				}
				if ready, err := f.store.PrepareAppMoveStep(t.Context(), claim, uuid.MustParse(plan.Steps[2].Command.StepId)); ready || !errors.Is(err, postgres.ErrAppMoveTargetLost) {
					t.Fatalf("lost target did not fail: %v %v", ready, err)
				}
			}
			if outcome == "rollback" || outcome == "stop_failure" || outcome == "target_lost" || outcome == "report_timeout" {
				if err := f.store.UpdateProgress(t.Context(), claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Failed, Reason: "Final transfer failed."}); err != nil {
					t.Fatal(err)
				}
				if err := f.store.ReconcileAppMove(t.Context(), installation, created.AppID); err != nil {
					t.Fatal(err)
				}
				if err := f.store.ReconcileAppMove(t.Context(), installation, created.AppID); err != nil {
					t.Fatal(err)
				}
				var restart uuid.UUID
				if err := f.pool.QueryRow(t.Context(), `SELECT id FROM operations WHERE app_id=$1 AND kind='app.start' AND status='pending'`, created.AppID).Scan(&restart); err != nil {
					t.Fatal(err)
				}
				rollback, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: restart, RecoveryGeneration: claim.RecoveryGeneration, OwnerID: "move-test", TTL: time.Minute, LeaseID: 4, LeaseRevision: 4})
				if err != nil {
					t.Fatal(err)
				}
				if err := f.planner.Plan(t.Context(), rollback); err != nil {
					t.Fatal(err)
				}
				restored, err := f.store.ReadPlan(t.Context(), rollback)
				if err != nil || len(restored.Steps) != 3 || restored.Steps[0].Command.Input.GetStopContainer() == nil || restored.Steps[2].Command.Epoch.NodeId != f.node.String() {
					t.Fatalf("rollback plan: %v %v", restored, err)
				}
				writer, err := f.store.GetAppWriter(t.Context(), installation, created.AppID)
				if err != nil || writer.NodeID != f.node || writer.Epoch != 1 {
					t.Fatalf("rollback changed writer: %+v %v", writer, err)
				}
				return
			}
			complete(1)
			if ready, err := f.store.PrepareAppMoveStep(t.Context(), claim, uuid.MustParse(plan.Steps[2].Command.StepId)); err != nil || ready {
				t.Fatalf("promoted before final heartbeat: %v %v", ready, err)
			}
			publish(5, 1)
			for range 2 {
				if ready, err := f.store.PrepareAppMoveStep(t.Context(), claim, uuid.MustParse(plan.Steps[2].Command.StepId)); err != nil || !ready {
					t.Fatalf("promotion: %v %v", ready, err)
				}
			}
			writer, err := f.store.GetAppWriter(t.Context(), installation, created.AppID)
			if err != nil || writer.NodeID != target || writer.Epoch != 2 {
				t.Fatalf("writer: %+v %v", writer, err)
			}
			if outcome == "promoted_failure" {
				if err := f.store.UpdateProgress(t.Context(), claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Failed, Reason: "Target start failed."}); err != nil {
					t.Fatal(err)
				}
				if err := f.store.ReconcileAppMove(t.Context(), installation, created.AppID); err != nil {
					t.Fatal(err)
				}
				var restarts int
				if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM operations WHERE app_id=$1 AND kind='app.start'`, created.AppID).Scan(&restarts); err != nil || restarts != 0 {
					t.Fatalf("old writer restarted after promotion: %d %v", restarts, err)
				}
				return
			}
			complete(2)
			complete(3)
			if err := f.store.UpdateProgress(t.Context(), claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Final copy and target start are proved."}); err != nil {
				t.Fatal(err)
			}
			copies, err := f.store.GetAppCopies(t.Context(), installation, created.AppID)
			if err != nil || len(copies) != 2 {
				t.Fatalf("copies: %+v %v", copies, err)
			}
			for _, copy := range copies {
				if copy.NodeID == f.node && copy.Role != "standby" {
					t.Fatal("source is not standby")
				}
			}
			if outcome == "success" {
				// A heartbeat gap resets the stable period even when the session survives.
				if _, err := f.pool.Exec(t.Context(), `UPDATE agent_presence SET online_since=clock_timestamp()-interval '61 seconds',last_seen_at=clock_timestamp()-interval '21 seconds' WHERE installation_id=$1 AND node_id=$2`, installation, f.node); err != nil {
					t.Fatal(err)
				}
				publish(0, 2)
				if err := f.store.ReconcileAppMove(t.Context(), installation, created.AppID); err != nil {
					t.Fatal(err)
				}
				var count int
				if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM operations WHERE app_id=$1 AND kind='app.move'`, created.AppID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("failback ignored heartbeat gap: %d %v", count, err)
				}
				if _, err := f.pool.Exec(t.Context(), `UPDATE agent_presence SET online_since=clock_timestamp()-interval '61 seconds' WHERE installation_id=$1 AND node_id=$2`, installation, f.node); err != nil {
					t.Fatal(err)
				}
				if err := f.store.ReconcileAppMove(t.Context(), installation, created.AppID); err != nil {
					t.Fatal(err)
				}
				var auto uuid.UUID
				if err := f.pool.QueryRow(t.Context(), `SELECT id FROM operations WHERE app_id=$1 AND kind='app.move' AND status='pending'`, created.AppID).Scan(&auto); err != nil {
					t.Fatal(err)
				}
				back, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: auto, RecoveryGeneration: claim.RecoveryGeneration, OwnerID: "move-test", TTL: time.Minute, LeaseID: 4, LeaseRevision: 4})
				if err != nil {
					t.Fatal(err)
				}
				if err := f.planner.Plan(t.Context(), back); err != nil {
					t.Fatal(err)
				}
				backPlan, err := f.store.ReadPlan(t.Context(), back)
				if err != nil {
					t.Fatal(err)
				}
				publish(0, 2)
				if ready, err := f.store.PrepareAppMoveStep(t.Context(), back, uuid.MustParse(backPlan.Steps[0].Command.StepId)); err != nil || ready {
					t.Fatalf("failback stopped before current replica: %v %v", ready, err)
				}
				presence, err := f.store.GetAgentPresence(t.Context(), installation, f.node)
				if err != nil {
					t.Fatal(err)
				}
				heartbeats[f.node]++
				if err := f.store.RecordAgentHeartbeat(t.Context(), installation, f.node, presence.SessionID, &agentv1.Heartbeat{Sequence: heartbeats[f.node], ObservedAt: timestamppb.Now(), Replicas: []*agentv1.ReplicaStatus{{AppId: created.AppID.String(), Role: "standby", Epoch: 2, Sequence: 8, CompletedAt: timestamppb.Now()}}}); err != nil {
					t.Fatal(err)
				}
				if ready, err := f.store.PrepareAppMoveStep(t.Context(), back, uuid.MustParse(backPlan.Steps[0].Command.StepId)); err != nil || !ready {
					t.Fatalf("stable complete failback blocked: %v %v", ready, err)
				}
				return
			}
			// Deletion includes retired data holders and waits for withdrawal acknowledgement.
			deleted, err := f.store.AdmitAppAction(t.Context(), postgres.AppActionRequest{Identity: postgres.RequestIdentity{InstallationID: installation, ActorID: "move-test", APIOperation: postgres.DeleteAppAPIOperation, IdempotencyKey: "delete"}, RequestID: "delete", AppID: created.AppID, ExpectedRevision: 2})
			if err != nil {
				t.Fatal(err)
			}
			removal, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: deleted.OperationID, RecoveryGeneration: claim.RecoveryGeneration, OwnerID: "move-test", TTL: time.Minute, LeaseID: 4, LeaseRevision: 4})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.planner.Plan(t.Context(), removal); err != nil {
				t.Fatal(err)
			}
			cleanup, err := f.store.ReadPlan(t.Context(), removal)
			if err != nil {
				t.Fatal(err)
			}
			all, err := f.store.GetAllAppCopies(t.Context(), installation, created.AppID)
			if err != nil {
				t.Fatal(err)
			}
			if len(cleanup.Steps) != 3*len(all) {
				t.Fatalf("cleanup missed copies: %d commands for %d copies", len(cleanup.Steps), len(all))
			}
			if ready, err := f.store.AppDataRemovalReady(t.Context(), installation, created.AppID, target); err != nil || ready {
				t.Fatalf("cleanup before withdrawal: %v %v", ready, err)
			}
			publish(0, 2)
			if ready, err := f.store.AppDataRemovalReady(t.Context(), installation, created.AppID, target); err != nil || !ready {
				t.Fatalf("withdrawal not accepted: %v %v", ready, err)
			}
			for _, copy := range all {
				report := &agentv1.BarrierResult{Epoch: epochFor(removal, copy.NodeID), AdmissionsClosed: true, Drained: true}
				if err := f.store.RecordBarrier(t.Context(), removal, report); err != nil {
					t.Fatal(err)
				}
				encoded, _ := proto.Marshal(report)
				if err := f.store.ReconcileBarrier(t.Context(), removal, copy.NodeID, sha256.Sum256(encoded), "No active effects."); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.store.ActivateClaim(t.Context(), removal); err != nil {
				t.Fatal(err)
			}
			for _, step := range cleanup.Steps {
				command := step.Command
				if err := f.store.RecordDispatch(t.Context(), removal, command); err != nil {
					t.Fatal(err)
				}
				effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: "absent"}
				if command.Input.GetRemoveAppData() != nil {
					effect.NativeOperationId = "app-data:" + command.CommandId
					effect.State = "app_data_absent"
				}
				result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: []*agentv1.Effect{effect}}
				if err := f.store.RecordAgentResult(t.Context(), installation, uuid.MustParse(command.Epoch.NodeId), result); err != nil {
					t.Fatal(err)
				}
				encoded, _ := proto.Marshal(result)
				if err := f.store.RecordStepCompletion(t.Context(), removal, uuid.MustParse(command.StepId), result.Epoch, sha256.Sum256(encoded)); err != nil {
					t.Fatal(err)
				}
			}
			targets, err := f.store.AppRouteTargets(t.Context(), installation)
			if err != nil {
				t.Fatal(err)
			}
			for node, target := range targets {
				table, _, err := f.store.PrepareAppRoutes(t.Context(), installation, node, target.Table)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.store.RecordAppRouteAck(t.Context(), installation, node, uuid.MustParse(table.SessionId), &agentv1.RouteAck{SessionId: table.SessionId, Revision: table.Revision}); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.store.UpdateProgress(t.Context(), removal, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "All containers and app data are absent."}); err != nil {
				t.Fatal(err)
			}
			var held int
			if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM resource_allocations WHERE cluster_id=$1 AND released_at IS NULL`, created.AppID).Scan(&held); err != nil || held != 0 {
				t.Fatalf("cleanup kept reservations: %d %v", held, err)
			}

		})
	}
}
