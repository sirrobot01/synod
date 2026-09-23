package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// completionPlan orders native actions before the final lifecycle proof.
func completionPlan(t *testing.T, claim operation.Claim, node, resource uuid.UUID, revision int64, kind string) *agentv1.ExecutionPlan {
	t.Helper()
	conditions := []agentv1.Postcondition{agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, agentv1.Postcondition_POSTCONDITION_ENGINE_READY}
	if kind == "stop" {
		conditions = []agentv1.Postcondition{agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED}
	}
	if kind == "delete" {
		conditions = []agentv1.Postcondition{agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT}
	}
	app, admin, replication := uuid.NewString(), uuid.NewString(), uuid.NewString()
	if kind == "recover_setup" {
		conditions = []agentv1.Postcondition{agentv1.Postcondition_POSTCONDITION_POSTGRES_SETUP_RECOVERED, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, agentv1.Postcondition_POSTCONDITION_ENGINE_READY}
	}

	plan := &agentv1.ExecutionPlan{Version: 1}
	for _, condition := range conditions {
		input := &agentv1.CommandInput{ResourceId: resource.String(), PayloadVersion: 1, Timeout: durationpb.New(time.Minute)}
		switch condition {
		case agentv1.Postcondition_POSTCONDITION_POSTGRES_SETUP_RECOVERED:
			input.Payload = &agentv1.CommandInput_RecoverPostgresSetup{RecoverPostgresSetup: &agentv1.RecoverPostgresSetup{Postgres: &agentv1.PostgresContainerConfig{Port: 15432, SuperuserCredentialId: admin, SuperuserCredentialVersion: 1, ApplicationCredentialId: app, ApplicationCredentialVersion: 1, ReplicationCredentialId: replication, ReplicationCredentialVersion: 1}}}
			input.Secrets = []*agentv1.SecretReference{{CredentialId: admin, Version: 1, Purpose: "superuser"}, {CredentialId: app, Version: 1, Purpose: "application"}, {CredentialId: replication, Version: 1, Purpose: "replication"}}

		case agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING:
			input.Payload = &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}
		case agentv1.Postcondition_POSTCONDITION_ENGINE_READY:
			input.Payload = &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{Engine: "postgres", PostgresRole: "primary", Address: "127.0.0.1", Port: 15432, Database: "app", Role: "synod_app"}}
			input.Secrets = []*agentv1.SecretReference{{CredentialId: app, Version: 1, Purpose: "application"}}
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED:
			input.Payload = &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(time.Second)}}
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT:
			input.Payload = &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}
		}
		command := &agentv1.Command{Epoch: &agentv1.Epoch{InstallationId: claim.InstallationID.String(), ClusterId: claim.ClusterID.String(), NodeId: node.String(), RecoveryGeneration: claim.RecoveryGeneration.String(), ExecutionEpoch: claim.ExecutionEpoch}, OperationId: claim.OperationID.String(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: revision, PlanVersion: 1, Input: input}
		hash, err := operation.CommandHash(command)
		if err != nil {
			t.Fatal(err)
		}
		command.InputHash = hash[:]
		step := &agentv1.PlanStep{Command: command, MaxAttempts: 3, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: condition}
		if len(plan.Steps) > 0 {
			step.DependsOn = []string{plan.Steps[len(plan.Steps)-1].Command.StepId}
		}
		plan.Steps = append(plan.Steps, step)
	}
	return plan
}

// completionFixture reserves a real instance and installs its checked command plan.
func completionFixture(t *testing.T) (*placementHost, operation.Claim, *agentv1.ExecutionPlan) {
	t.Helper()
	host := placementFixture(t)
	claim := host.claimCluster(t, "completed-db")
	request, decision, _ := host.place(t, claim, smallBudget, []placement.Port{{Address: "127.0.0.1", Transport: "tcp", Number: 15432}})
	plan := completionPlan(t, claim, host.node, request.Resources[0].ID, 1, "create")
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, nil); err != nil {
		t.Fatal(err)
	}
	reconcilePlan(t, host.store, claim, plan)
	if err := host.store.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	return host, claim, plan
}

// completeSteps delivers native receipts through dispatch and checked completion APIs.
func completeSteps(t *testing.T, s *Store, claim operation.Claim, plan *agentv1.ExecutionPlan) {
	t.Helper()
	for _, step := range plan.Steps {
		command := proto.Clone(step.Command).(*agentv1.Command)
		command.Epoch.ExecutionEpoch = claim.ExecutionEpoch
		command.Epoch.RecoveryGeneration = claim.RecoveryGeneration.String()
		if err := s.RecordDispatch(t.Context(), claim, command); err != nil {
			t.Fatal(err)
		}
		state, prefix := "running", "docker-command:"
		switch step.Postcondition {
		case agentv1.Postcondition_POSTCONDITION_POSTGRES_SETUP_RECOVERED:
			state = "setup_recovered"

		case agentv1.Postcondition_POSTCONDITION_ENGINE_READY:
			state, prefix = "ready", "readiness:"
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, agentv1.Postcondition_POSTCONDITION_CONTAINER_INSPECTED:
			state = "exited"
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT:
			state = "absent"
		}
		result := &agentv1.TerminalResult{Epoch: proto.Clone(command.Epoch).(*agentv1.Epoch), CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: prefix + command.CommandId, State: state}}}
		if err := s.RecordAgentResult(t.Context(), claim.InstallationID, uuid.MustParse(command.Epoch.NodeId), result); err != nil {
			t.Fatal(err)
		}
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.RecordStepCompletion(t.Context(), claim, uuid.MustParse(command.StepId), result.Epoch, sha256.Sum256(encoded)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLifecycleCompletionCommitsObservedStateAndDeleteTogether(t *testing.T) {
	host, claim, plan := completionFixture(t)
	s := host.store
	resource := uuid.MustParse(plan.Steps[0].Command.Input.ResourceId)
	volume := declared(host, claim.ClusterID)
	if err := s.RecordVolumeIdentity(t.Context(), volume); err != nil {
		t.Fatal(err)
	}
	update := ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Every native postcondition is proved."}
	for index, action := range []string{"create", "stop", "start", "delete"} {
		t.Run(action, func(t *testing.T) {
			revision := int64(index + 1)
			if action != "create" {
				apiOperation := map[string]string{"stop": StopClusterAPIOperation, "start": StartClusterAPIOperation, "delete": DeleteClusterAPIOperation}[action]
				admission, err := host.action(t, claim.ClusterID, apiOperation, revision-1)
				if err != nil {
					t.Fatal(err)
				}
				claim, err = s.AcquireClaim(t.Context(), ClaimRequest{ClaimID: uuid.New(), InstallationID: host.id, OperationID: admission.OperationID, RecoveryGeneration: host.generation, OwnerID: "complete-" + action, TTL: time.Minute, CoordinationKey: "/completion/" + action, CoordinationLease: 1, CoordinationRevision: 1})
				if err != nil {
					t.Fatal(err)
				}
				plan = completionPlan(t, claim, host.node, resource, revision, action)
				if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
					t.Fatal(err)
				}
				reconcilePlan(t, s, claim, plan)
				if err := s.ActivateClaim(t.Context(), claim); err != nil {
					t.Fatal(err)
				}
			}
			before, err := s.GetCluster(t.Context(), host.id, claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			completeSteps(t, s, claim, plan)
			if err := s.UpdateProgress(t.Context(), claim, update); err != nil {
				t.Fatal(err)
			}
			after, err := s.GetCluster(t.Context(), host.id, claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			expected := cluster.ObservedRunning
			if action == "stop" {
				expected = cluster.ObservedStopped
			}
			if action == "delete" {
				expected = cluster.ObservedDeleted
			}
			if after.ObservedRevision != revision || after.ObservedLifecycle != expected || !after.UpdatedAt.After(before.UpdatedAt) || (after.DeletedAt != nil) != (action == "delete") {
				t.Fatalf("completed cluster = %+v", after)
			}
			op, err := s.GetOperation(t.Context(), host.id, claim.OperationID)
			if err != nil || op.Status != operation.Succeeded {
				t.Fatalf("operation = %+v, %v", op, err)
			}
			// Replay must precede allocation checks: a completed delete has no live allocation.
			if err := s.UpdateProgress(t.Context(), claim, update); err != nil {
				t.Fatalf("exact replay: %v", err)
			}
			replay, err := s.GetCluster(t.Context(), host.id, claim.ClusterID)
			if err != nil || !replay.UpdatedAt.Equal(after.UpdatedAt) {
				t.Fatalf("replay changed completion metadata: %+v, %v", replay, err)
			}
			if action != "delete" && host.count(t, `SELECT count(*) FROM allocation_ports WHERE released_at IS NULL`) != 1 {
				t.Fatal("non-delete released the port")
			}
		})
	}
	tombstone, err := s.ReadClusterTombstone(t.Context(), host.id, claim.ClusterID)
	if err != nil || !tombstone.Cleared || len(tombstone.Pending) != 0 || tombstone.DeletedByOperation != claim.OperationID || tombstone.RetainedVolumes != 1 || tombstone.BackupDisposition != BackupsNoPolicy {
		t.Fatalf("tombstone=%+v, %v", tombstone, err)
	}
	if host.count(t, `SELECT count(*) FROM cluster_tombstone_nodes WHERE acknowledged_at IS NOT NULL`) != 1 {
		t.Fatal("native absence did not acknowledge the node")
	}
	for _, table := range []string{"resource_allocations", "allocation_ports"} {
		if host.count(t, `SELECT count(*) FROM `+table+` WHERE released_at IS NULL`) != 0 {
			t.Fatalf("%s still holds deleted cluster", table)
		}
	}
	retained, err := s.ReadVolumeIdentity(t.Context(), host.id, volume.ResourceID)
	if err != nil || retained != volume {
		t.Fatalf("retained volume changed: %+v, %v", retained, err)
	}
	page, err := s.ListClusters(t.Context(), host.id, cluster.ListOptions{Limit: 50})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("default list retained deleted cluster: %+v, %v", page, err)
	}
	page, err = s.ListClusters(t.Context(), host.id, cluster.ListOptions{Limit: 50, IncludeDeleted: true})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != claim.ClusterID || page.Items[0].DeletedAt == nil {
		t.Fatalf("deleted list lost completed cluster: %+v, %v", page, err)
	}
	replacement := host.claimCluster(t, "completed-db")
	if replacement.ClusterID == claim.ClusterID {
		t.Fatal("name reuse revived a deleted identity")
	}
}

func TestLifecycleCompletionRequiresStoredProof(t *testing.T) {
	for _, name := range []string{"missing plan", "empty plan", "malformed plan", "missing completion", "missing receipt", "receipt hash", "malformed receipt", "unknown effect", "active effect", "wrong resource", "wrong node", "wrong state", "no allocation", "stale revision", "wrong lifecycle"} {
		t.Run(name, func(t *testing.T) {
			host, claim, plan := completionFixture(t)
			s := host.store
			completeSteps(t, s, claim, plan)
			command := plan.Steps[len(plan.Steps)-1].Command
			var query string
			var args []any
			switch name {
			case "missing plan":
				query = `UPDATE operations SET plan_bytes=NULL,plan_version=0`
			case "empty plan":
				encoded, err := proto.Marshal(&agentv1.ExecutionPlan{Version: 1})
				if err != nil {
					t.Fatal(err)
				}
				query, args = `UPDATE operations SET plan_bytes=$1`, []any{encoded}
			case "malformed plan":
				query, args = `UPDATE operations SET plan_bytes=$1`, []any{[]byte{255}}
			case "missing completion":
				query, args = `DELETE FROM operation_step_completions WHERE step_id=$1`, []any{command.StepId}
			case "missing receipt":
				if _, err := s.pool.Exec(t.Context(), `DELETE FROM operation_step_completions WHERE step_id=$1`, command.StepId); err != nil {
					t.Fatal(err)
				}
				query, args = `DELETE FROM command_results WHERE command_id=$1`, []any{command.CommandId}
			case "receipt hash":
				query, args = `UPDATE operation_step_completions SET receipt_hash=$1 WHERE step_id=$2`, []any{make([]byte, 32), command.StepId}
			case "no allocation":
				query = `UPDATE resource_allocations SET released_at=clock_timestamp()`
			case "stale revision":
				query = `UPDATE clusters SET desired_revision=desired_revision+1`
			case "wrong lifecycle":
				query = `UPDATE clusters SET desired_lifecycle='stopped'`
			default:
				var encoded []byte
				if err := s.pool.QueryRow(t.Context(), `SELECT result_bytes FROM command_results WHERE command_id=$1`, command.CommandId).Scan(&encoded); err != nil {
					t.Fatal(err)
				}
				var result agentv1.TerminalResult
				if err := proto.Unmarshal(encoded, &result); err != nil {
					t.Fatal(err)
				}
				switch name {
				case "unknown effect":
					result.Effects[0].OutcomeUnknown = true
				case "active effect":
					result.Effects[0].Active = true
				case "wrong resource":
					result.Effects[0].ResourceId = uuid.NewString()
				case "wrong node":
					result.Epoch.NodeId = uuid.NewString()
				case "wrong state":
					result.Effects[0].State = "running"
				}
				var err error
				encoded, err = (proto.MarshalOptions{Deterministic: true}).Marshal(&result)
				if err != nil {
					t.Fatal(err)
				}
				if name == "malformed receipt" {
					encoded = []byte{255}
				}
				hash := sha256.Sum256(encoded)
				if _, err := s.pool.Exec(t.Context(), `UPDATE operation_step_completions SET receipt_hash=$1 WHERE step_id=$2`, hash[:], command.StepId); err != nil {
					t.Fatal(err)
				}
				query, args = `UPDATE command_results SET result_bytes=$1 WHERE command_id=$2`, []any{encoded, command.CommandId}
			}
			if _, err := s.pool.Exec(t.Context(), query, args...); err != nil {
				t.Fatal(err)
			}
			before, err := s.GetCluster(t.Context(), host.id, claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			err = s.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "All steps appear complete."})
			if err == nil {
				t.Fatal("unproved lifecycle succeeded")
			}
			after, readErr := s.GetCluster(t.Context(), host.id, claim.ClusterID)
			if readErr != nil || after.ObservedRevision != before.ObservedRevision || after.ObservedLifecycle != before.ObservedLifecycle || !after.UpdatedAt.Equal(before.UpdatedAt) || after.DeletedAt != nil {
				t.Fatalf("refusal changed observed state: %+v, %v", after, readErr)
			}
			if host.count(t, `SELECT count(*) FROM operation_progress`) != 0 || host.count(t, `SELECT count(*) FROM operations WHERE status='running' AND claim_active`) != 1 || host.count(t, `SELECT count(*) FROM allocation_ports WHERE released_at IS NULL`) != 1 {
				t.Fatal("refused completion changed progress or released its hold")
			}
		})
	}
}

func TestLifecycleCompletionDoesNotInferSuccessFromOtherTerminalStates(t *testing.T) {
	for _, status := range []operation.Status{operation.Waiting, operation.Failed, operation.Cancelled} {
		t.Run(string(status), func(t *testing.T) {
			host, claim, _ := completionFixture(t)
			before, err := host.store.GetCluster(t.Context(), host.id, claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			sequence := int64(1)
			if status == operation.Cancelled {
				if err := host.store.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: sequence, Status: operation.Cancelling, Reason: "Cancel before dispatch."}); err != nil {
					t.Fatal(err)
				}
				sequence++
			}
			if err := host.store.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: sequence, Status: status, Reason: "The operation did not prove its target."}); err != nil {
				t.Fatal(err)
			}
			after, err := host.store.GetCluster(t.Context(), host.id, claim.ClusterID)
			if err != nil || after.ObservedLifecycle != before.ObservedLifecycle || after.ObservedRevision != before.ObservedRevision || after.DeletedAt != nil {
				t.Fatalf("non-success changed observed state: %+v, %v", after, err)
			}
		})
	}
}

func TestLifecycleCompletionRecoversCommitReply(t *testing.T) {
	host, claim, plan := completionFixture(t)
	completeSteps(t, host.store, claim, plan)
	host.store.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	update := ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Native postconditions passed."}
	requireCode(t, host.store.UpdateProgress(t.Context(), claim, update), OutcomeUnknown)
	host.store.commitTx = nil
	if err := host.store.UpdateProgress(t.Context(), claim, update); err != nil {
		t.Fatal(err)
	}
	if host.count(t, `SELECT count(*) FROM operation_progress`) != 1 {
		t.Fatal("retry duplicated terminal progress")
	}
	changed := update
	changed.Reason = "A different completion."
	if err := host.store.UpdateProgress(t.Context(), claim, changed); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("changed terminal replay: %v", err)
	}
}

// Persisting a plan is not evidence that all of its effects converge on the target.
func TestLifecycleCompletionRefusesContradictoryFinalProof(t *testing.T) {
	for _, name := range []string{"running only", "unordered ready", "later stop", "later inspect", "independent ready", "unrelated resource", "wrong allocated node"} {
		t.Run(name, func(t *testing.T) {
			host := placementFixture(t)
			claim := host.claimCluster(t, "contradictory-db")
			request, decision, _ := host.place(t, claim, smallBudget, nil)
			resource, node := request.Resources[0].ID, host.node
			if name == "unrelated resource" {
				resource = uuid.New()
			}
			if name == "wrong allocated node" {
				node = host.enroll(t)
			}
			plan := completionPlan(t, claim, node, resource, 1, "create")
			switch name {
			case "running only":
				plan.Steps = plan.Steps[:1]
			case "unordered ready":
				plan.Steps[1].DependsOn = nil
			case "later stop":
				step := completionPlan(t, claim, node, resource, 1, "stop").Steps[0]
				step.DependsOn = []string{plan.Steps[1].Command.StepId}
				plan.Steps = append(plan.Steps, step)
			case "later inspect":
				step := completionPlan(t, claim, node, resource, 1, "stop").Steps[0]
				step.Command.Input.Payload = &agentv1.CommandInput_InspectContainer{InspectContainer: &agentv1.InspectContainer{}}
				step.Postcondition = agentv1.Postcondition_POSTCONDITION_CONTAINER_INSPECTED
				hash, err := operation.CommandHash(step.Command)
				if err != nil {
					t.Fatal(err)
				}
				step.Command.InputHash = hash[:]
				step.DependsOn = []string{plan.Steps[1].Command.StepId}
				plan.Steps = append(plan.Steps, step)
			case "independent ready":
				step := completionPlan(t, claim, node, resource, 1, "create").Steps[1]
				step.DependsOn = []string{plan.Steps[0].Command.StepId}
				plan.Steps = append(plan.Steps, step)
			}
			if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, nil); err != nil {
				t.Fatal(err)
			}
			reconcilePlan(t, host.store, claim, plan)
			if err := host.store.ActivateClaim(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			completeSteps(t, host.store, claim, plan)
			if err := host.store.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Every step completed."}); err == nil {
				t.Fatal("unrelated or contradictory proof completed the lifecycle")
			}
			if host.count(t, `SELECT count(*) FROM clusters WHERE observed_revision=0 AND deleted_at IS NULL`) != 1 || host.count(t, `SELECT count(*) FROM resource_allocations WHERE released_at IS NULL`) != 1 {
				t.Fatal("refused proof changed lifecycle or released allocation")
			}
		})
	}
}

func TestLifecycleDeleteRollsBackAllMetadataOnReleaseFailure(t *testing.T) {
	host, create, createPlan := completionFixture(t)
	s := host.store
	completeSteps(t, s, create, createPlan)
	if err := s.UpdateProgress(t.Context(), create, ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "The engine is ready."}); err != nil {
		t.Fatal(err)
	}
	volume := declared(host, create.ClusterID)
	if err := s.RecordVolumeIdentity(t.Context(), volume); err != nil {
		t.Fatal(err)
	}
	admission, err := host.action(t, create.ClusterID, DeleteClusterAPIOperation, 1)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := s.AcquireClaim(t.Context(), ClaimRequest{ClaimID: uuid.New(), InstallationID: host.id, OperationID: admission.OperationID, RecoveryGeneration: host.generation, OwnerID: "delete-owner", TTL: time.Minute, CoordinationKey: "/delete/atomic", CoordinationLease: 1, CoordinationRevision: 1})
	if err != nil {
		t.Fatal(err)
	}
	plan := completionPlan(t, claim, host.node, uuid.MustParse(createPlan.Steps[0].Command.Input.ResourceId), 2, "delete")
	// A second native instance makes the delete account for every reserved node.
	secondNode, secondResource := host.enroll(t), uuid.New()
	s.seedAllocation(t, placement.Allocation{ID: uuid.New(), InstallationID: host.id, ClusterID: claim.ClusterID, NodeID: secondNode, ResourceID: secondResource, CreatedByOperation: create.OperationID, CreatedRevision: 1, OwnedResourceIDs: []uuid.UUID{secondResource}, Budget: smallBudget, Ports: []placement.Port{{Address: "127.0.0.1", Transport: "tcp", Number: 15433}}}, create.OperationID)
	plan.Steps = append(plan.Steps, completionPlan(t, claim, secondNode, secondResource, 2, "delete").Steps...)
	if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	reconcilePlan(t, s, claim, plan)
	if err := s.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	completeSteps(t, s, claim, plan)
	// Fail a release write after the transaction has written cleanup evidence.
	if _, err := s.pool.Exec(t.Context(), `CREATE FUNCTION refuse_port_release() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected release failure'; END $$;
 CREATE TRIGGER refuse_port_release BEFORE UPDATE OF released_at ON allocation_ports FOR EACH ROW EXECUTE FUNCTION refuse_port_release()`); err != nil {
		t.Fatal(err)
	}
	update := ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Both native instances are absent."}
	before, err := s.GetCluster(t.Context(), host.id, claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateProgress(t.Context(), claim, update); err == nil {
		t.Fatal("delete committed through a failed release")
	}
	after, err := s.GetCluster(t.Context(), host.id, claim.ClusterID)
	if err != nil || after.DeletedAt != nil || after.ObservedLifecycle != before.ObservedLifecycle || after.ObservedRevision != before.ObservedRevision || !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatalf("rollback lost old lifecycle: %+v, %v", after, err)
	}
	for _, table := range []string{"cluster_tombstones", "cluster_tombstone_nodes"} {
		if host.count(t, `SELECT count(*) FROM `+table) != 0 {
			t.Fatalf("rollback left %s", table)
		}
	}
	for _, table := range []string{"resource_allocations", "allocation_ports"} {
		if host.count(t, `SELECT count(*) FROM `+table+` WHERE released_at IS NULL`) != 2 {
			t.Fatalf("rollback released %s", table)
		}
	}
	if host.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1`, claim.OperationID) != 0 || host.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='running' AND claim_active`, claim.OperationID) != 1 {
		t.Fatal("rollback lost the active mutation slot")
	}
	if _, err := s.pool.Exec(t.Context(), `DROP TRIGGER refuse_port_release ON allocation_ports`); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateProgress(t.Context(), claim, update); err != nil {
		t.Fatal(err)
	}
	tombstone, err := s.ReadClusterTombstone(t.Context(), host.id, claim.ClusterID)
	if err != nil || !tombstone.Cleared || len(tombstone.Pending) != 0 || tombstone.RetainedVolumes != 1 {
		t.Fatalf("retry tombstone=%+v, %v", tombstone, err)
	}
	if host.count(t, `SELECT count(*) FROM cluster_tombstone_nodes WHERE acknowledged_at IS NOT NULL`) != 2 || host.count(t, `SELECT count(*) FROM allocation_ports WHERE released_at IS NULL`) != 0 {
		t.Fatal("retry did not acknowledge and release both nodes")
	}
	retained, err := s.ReadVolumeIdentity(t.Context(), host.id, volume.ResourceID)
	if err != nil || retained != volume {
		t.Fatalf("delete changed volume: %+v, %v", retained, err)
	}
	if err := s.UpdateProgress(t.Context(), claim, update); err != nil {
		t.Fatalf("delete replay after release: %v", err)
	}
}

func TestLifecycleCompletionReleaseLocksClusterBeforeReservation(t *testing.T) {
	host, claim, _ := completionFixture(t)
	s := host.store
	var reservation uuid.UUID
	if err := s.pool.QueryRow(t.Context(), `SELECT id FROM resource_allocations WHERE cluster_id=$1`, claim.ClusterID).Scan(&reservation); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: 1, Status: operation.Failed, Reason: "No command was dispatched."}); err != nil {
		t.Fatal(err)
	}
	blocker, err := s.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(t.Context())
	if _, err := blocker.Exec(t.Context(), `SELECT id FROM clusters WHERE id=$1 FOR UPDATE`, claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.ReleaseUnusedPlacement(ctx, host.id, claim.OperationID, reservation) }()
	waitForKeyWait(t, s)
	// The release is waiting for our cluster lock. It must not hold the reservation
	// while waiting, because lifecycle completion takes these locks in that order.
	checker, err := s.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer checker.Rollback(t.Context())
	if _, err := checker.Exec(t.Context(), `SELECT id FROM resource_allocations WHERE id=$1 FOR UPDATE NOWAIT`, reservation); err != nil {
		t.Fatalf("release locked reservation before cluster: %v", err)
	}
	if err := checker.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := blocker.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if host.count(t, `SELECT count(*) FROM resource_allocations WHERE released_at IS NULL`) != 0 {
		t.Fatal("unused reservation remains held")
	}
}

func TestLifecycleCompletionRejectsReadinessAfterStopOrDelete(t *testing.T) {
	for _, action := range []string{"stop", "delete"} {
		t.Run(action, func(t *testing.T) {
			host, create, createPlan := completionFixture(t)
			s := host.store
			completeSteps(t, s, create, createPlan)
			if err := s.UpdateProgress(t.Context(), create, ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "The engine is ready."}); err != nil {
				t.Fatal(err)
			}
			apiOperation := StopClusterAPIOperation
			if action == "delete" {
				apiOperation = DeleteClusterAPIOperation
			}
			admission, err := host.action(t, create.ClusterID, apiOperation, 1)
			if err != nil {
				t.Fatal(err)
			}
			claim, err := s.AcquireClaim(t.Context(), ClaimRequest{ClaimID: uuid.New(), InstallationID: host.id, OperationID: admission.OperationID, RecoveryGeneration: host.generation, OwnerID: "later-observation", TTL: time.Minute, CoordinationKey: "/completion/observe", CoordinationLease: 1, CoordinationRevision: 1})
			if err != nil {
				t.Fatal(err)
			}
			resource := uuid.MustParse(createPlan.Steps[0].Command.Input.ResourceId)
			plan := completionPlan(t, claim, host.node, resource, 2, action)
			ready := completionPlan(t, claim, host.node, resource, 2, "start").Steps[1]
			ready.DependsOn = []string{plan.Steps[len(plan.Steps)-1].Command.StepId}
			plan.Steps = append(plan.Steps, ready)
			if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
				t.Fatal(err)
			}
			reconcilePlan(t, s, claim, plan)
			if err := s.ActivateClaim(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			completeSteps(t, s, claim, plan)
			if err := s.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Every step succeeded."}); !errors.Is(err, operation.ErrCommandConflict) {
				t.Fatalf("later ready observation did not invalidate %s: %v", action, err)
			}
			if host.count(t, `SELECT count(*) FROM clusters WHERE observed_revision=1 AND observed_lifecycle='running' AND deleted_at IS NULL`) != 1 || host.count(t, `SELECT count(*) FROM allocation_ports WHERE released_at IS NULL`) != 1 || host.count(t, `SELECT count(*) FROM cluster_tombstones`) != 0 {
				t.Fatal("contradictory completion changed state or released native holds")
			}
		})
	}
}

func TestLifecycleCompletionAcceptsTransitiveFinalProof(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "transitive-db")
	request, decision, _ := host.place(t, claim, smallBudget, nil)
	plan := completionPlan(t, claim, host.node, request.Resources[0].ID, 1, "create")
	// The last ready step follows the start through the first ready observation.
	final := completionPlan(t, claim, host.node, request.Resources[0].ID, 1, "create").Steps[1]
	final.DependsOn = []string{plan.Steps[1].Command.StepId}
	plan.Steps = append(plan.Steps, final)
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, nil); err != nil {
		t.Fatal(err)
	}
	reconcilePlan(t, host.store, claim, plan)
	if err := host.store.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	completeSteps(t, host.store, claim, plan)
	if err := host.store.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "The final observation follows every earlier effect."}); err != nil {
		t.Fatal(err)
	}
}

func TestLifecycleCompletionAcceptsCheckedReceiptsFromPreviousEpoch(t *testing.T) {
	host, original, plan := completionFixture(t)
	s := host.store
	completeSteps(t, s, original, plan)
	if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, original.OperationID); err != nil {
		t.Fatal(err)
	}
	takeover, err := s.AcquireClaim(t.Context(), ClaimRequest{ClaimID: uuid.New(), InstallationID: host.id, OperationID: original.OperationID, RecoveryGeneration: host.generation, OwnerID: "historical-proof-owner", TTL: time.Minute, CoordinationKey: "/completion/historical", CoordinationLease: 2, CoordinationRevision: 2})
	if err != nil {
		t.Fatal(err)
	}
	if takeover.ExecutionEpoch <= original.ExecutionEpoch {
		t.Fatal("takeover did not advance the execution epoch")
	}
	reconcilePlan(t, s, takeover, plan)
	if err := s.ActivateClaim(t.Context(), takeover); err != nil {
		t.Fatal(err)
	}
	// The new owner reconciles the barrier but does not dispatch or repeat a step.
	if host.count(t, `SELECT count(*) FROM operation_step_completions WHERE operation_id=$1 AND execution_epoch=$2`, original.OperationID, original.ExecutionEpoch) != len(plan.Steps) || host.count(t, `SELECT count(*) FROM command_dispatches WHERE operation_id=$1 AND execution_epoch=$2`, original.OperationID, takeover.ExecutionEpoch) != 0 {
		t.Fatal("fixture replaced historical proof with current-epoch work")
	}
	if err := s.UpdateProgress(t.Context(), takeover, ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "The previous owner checked every native postcondition."}); err != nil {
		t.Fatalf("historical checked receipts could not complete the lifecycle: %v", err)
	}
	observed, err := s.GetCluster(t.Context(), host.id, original.ClusterID)
	if err != nil || observed.ObservedRevision != 1 || observed.ObservedLifecycle != cluster.ObservedRunning {
		t.Fatalf("historical proof did not converge observed state: %+v, %v", observed, err)
	}
}
