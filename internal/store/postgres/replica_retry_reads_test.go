package postgres

import (
	"bytes"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestScaleRetryReadChecksDispatchAndResultIdentity(t *testing.T) {
	for _, change := range []string{"unchanged", "result_epoch", "result_hash", "result_command", "result_state", "result_sequence", "malformed_result", "command_node", "command_epoch", "command_input", "command_plan", "dispatch_step", "malformed_command", "oversized_command"} {
		t.Run(change, func(t *testing.T) {
			s, request, claim, plan, node, _ := readsFixture(t)
			first, second := plan.Steps[0].Command, plan.Steps[1].Command
			for _, command := range []*agentv1.Command{first, second} {
				if err := s.RecordDispatch(t.Context(), claim, command); err != nil {
					t.Fatal(err)
				}
			}
			result := &agentv1.TerminalResult{Epoch: proto.Clone(first.Epoch).(*agentv1.Epoch), CommandId: first.CommandId, InputHash: bytes.Clone(first.InputHash), Sequence: 1, State: "failed", FailureCode: "capacity_unavailable"}
			if err := s.RecordAgentResult(t.Context(), claim.InstallationID, node, result); err != nil {
				t.Fatal(err)
			}
			if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET kind='cluster.scale',status='failed',claim_active=false WHERE id=$1`, claim.OperationID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.pool.Exec(t.Context(), `UPDATE clusters SET desired_replicas=1 WHERE id=$1`, claim.ClusterID); err != nil {
				t.Fatal(err)
			}
			admitted, err := s.AdmitClusterAction(t.Context(), ClusterActionRequest{Identity: RequestIdentity{InstallationID: claim.InstallationID, ActorID: "retry-test", APIOperation: ScaleClusterAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(), ClusterID: claim.ClusterID, ExpectedRevision: 1, Replicas: new(1)})
			if err != nil {
				t.Fatal(err)
			}
			request.ClaimID, request.OperationID = uuid.New(), admitted.OperationID
			current, err := s.AcquireClaim(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			changed := proto.Clone(first).(*agentv1.Command)
			switch change {
			case "result_epoch":
				result.Epoch.ExecutionEpoch++
			case "result_hash":
				result.InputHash[0] ^= 1
			case "result_command":
				result.CommandId = uuid.NewString()
			case "result_state":
				result.State = "unknown"
			case "result_sequence":
				result.Sequence = 0
			case "command_node":
				changed.Epoch.NodeId = uuid.NewString()
			case "command_epoch":
				changed.Epoch.ExecutionEpoch++
			case "command_input":
				changed.Input.ResourceId = uuid.NewString()
				hash, err := operation.CommandHash(changed)
				if err != nil {
					t.Fatal(err)
				}
				changed.InputHash = hash[:]
			case "command_plan":
				changed.PlanVersion++
			}
			encoded, err := proto.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if change == "malformed_result" {
				encoded = []byte{0xff}
			}
			if _, err := s.pool.Exec(t.Context(), `UPDATE command_results SET result_bytes=$2 WHERE command_id=$1`, first.CommandId, encoded); err != nil {
				t.Fatal(err)
			}
			encoded, err = proto.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			if change == "malformed_command" {
				encoded = []byte{0xff}
			}
			if change == "oversized_command" {
				encoded = make([]byte, maxExecutionStateBytes+1)
			}
			if _, err := s.pool.Exec(t.Context(), `UPDATE command_dispatches SET command_bytes=$2 WHERE command_id=$1`, first.CommandId, encoded); err != nil {
				t.Fatal(err)
			}
			if change == "dispatch_step" {
				if _, err := s.pool.Exec(t.Context(), `UPDATE command_dispatches SET step_id=$2 WHERE command_id=$1`, first.CommandId, uuid.New()); err != nil {
					t.Fatal(err)
				}
			}
			retained, evidence, err := s.ReadScaleRetryPlan(t.Context(), current)
			if change != "unchanged" {
				want := operation.ErrCommandConflict
				if change == "oversized_command" {
					want = ErrExecutionStateTooLarge
				}
				if !errors.Is(err, want) {
					t.Fatalf("invalid evidence: %v, want %v", err, want)
				}
				return
			}
			if err != nil || !proto.Equal(retained, plan) || len(evidence) != 2 {
				t.Fatalf("retained plan or evidence: %v", err)
			}
			for _, each := range evidence {
				switch each.Command.CommandId {
				case first.CommandId:
					if !proto.Equal(each.Command, first) || !proto.Equal(each.Result, result) {
						t.Fatal("read changed the original receipt")
					}
				case second.CommandId:
					if !proto.Equal(each.Command, second) || each.Result != nil {
						t.Fatal("read inferred a missing receipt")
					}
				default:
					t.Fatal("read included an unrelated command")
				}
			}
			stale := current
			stale.ID = uuid.New()
			if _, _, err := s.ReadScaleRetryPlan(t.Context(), stale); !errors.Is(err, operation.ErrStaleOwner) {
				t.Fatalf("stale read: %v", err)
			}
		})
	}
}
