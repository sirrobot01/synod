package postgres

import (
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// readsPlan starts count containers on one node under the current claim.
func readsPlan(t *testing.T, claim operation.Claim, node uuid.UUID, count int) *agentv1.ExecutionPlan {
	t.Helper()
	plan := &agentv1.ExecutionPlan{Version: 1}
	for range count {
		command := &agentv1.Command{
			Epoch:       &agentv1.Epoch{InstallationId: claim.InstallationID.String(), NodeId: node.String(), ClusterId: claim.ClusterID.String(), RecoveryGeneration: claim.RecoveryGeneration.String(), ExecutionEpoch: claim.ExecutionEpoch},
			OperationId: claim.OperationID.String(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: 1, PlanVersion: 1,
			Input: &agentv1.CommandInput{ResourceId: uuid.NewString(), PayloadVersion: 1, Timeout: durationpb.New(time.Minute), Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}},
		}
		hash, err := operation.CommandHash(command)
		if err != nil {
			t.Fatal(err)
		}
		command.InputHash = hash[:]
		plan.Steps = append(plan.Steps, &agentv1.PlanStep{Command: command, MaxAttempts: 3, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING})
	}
	return plan
}

// drainNode reports and reconciles one drained barrier, then activates the claim.
func drainNode(t *testing.T, s *Store, claim operation.Claim, node, session uuid.UUID) {
	t.Helper()
	epoch := &agentv1.Epoch{InstallationId: claim.InstallationID.String(), NodeId: node.String(), ClusterId: claim.ClusterID.String(), RecoveryGeneration: claim.RecoveryGeneration.String(), ExecutionEpoch: claim.ExecutionEpoch}
	report := &agentv1.BarrierResult{Epoch: epoch, AdmissionsClosed: true, Drained: true}
	if err := s.RecordSessionBarrier(t.Context(), claim, session, report); err != nil {
		t.Fatal(err)
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileBarrier(t.Context(), claim, node, sha256.Sum256(encoded), "Inspected the node. No effect remains."); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
}

func readsFixture(t *testing.T) (*Store, ClaimRequest, operation.Claim, *agentv1.ExecutionPlan, uuid.UUID, uuid.UUID) {
	t.Helper()
	return readsFixtureWith(t, nil)
}

// readsFixtureWith lets edit change the plan before it is installed.
func readsFixtureWith(t *testing.T, edit func(*agentv1.ExecutionPlan)) (*Store, ClaimRequest, operation.Claim, *agentv1.ExecutionPlan, uuid.UUID, uuid.UUID) {
	t.Helper()
	s, request, claim := claimFixture(t)
	node := uuid.New()
	plan := readsPlan(t, claim, node, 2)
	if edit != nil {
		edit(plan)
	}
	if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	// Seed enrolled identity for this Store-only fixture. TLS enrollment has separate tests.
	if _, err := s.pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at) VALUES ($1,$2,$3,clock_timestamp())`, node, claim.InstallationID, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	session := uuid.New()
	hello := &agentv1.Hello{InstallationId: claim.InstallationID.String(), NodeId: node.String(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1}}
	if err := s.RecordAgentSession(t.Context(), claim.InstallationID, node, session, hello); err != nil {
		t.Fatal(err)
	}
	drainNode(t, s, claim, node, session)
	return s, request, claim, plan, node, session
}

// admit records one agent acknowledgement of a committed dispatch.
func admit(t *testing.T, s *Store, claim operation.Claim, node, session uuid.UUID, command *agentv1.Command, state, failure string) uuid.UUID {
	t.Helper()
	if err := s.RecordRecoveryCheckpoint(t.Context(), claim, sha256.Sum256([]byte("frontier")), nil); err != nil {
		t.Fatal(err)
	}
	attempt := uuid.New()
	if _, err := s.RecordDeliveryAttempt(t.Context(), claim, uuid.MustParse(command.CommandId), session, attempt, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ack := &agentv1.AdmissionAck{DeliveryAttemptId: attempt.String(), Epoch: proto.Clone(command.Epoch).(*agentv1.Epoch), CommandId: command.CommandId, State: state, ErrorCode: failure, InputHash: command.InputHash}
	if err := s.RecordAgentAdmission(t.Context(), uuid.MustParse(command.Epoch.InstallationId), node, session, ack); err != nil {
		t.Fatal(err)
	}
	return attempt
}

// proveStep receives a succeeded receipt and retains it as a step completion.
func proveStep(t *testing.T, s *Store, claim operation.Claim, node uuid.UUID, command *agentv1.Command) *agentv1.TerminalResult {
	t.Helper()
	result := &agentv1.TerminalResult{
		Epoch: proto.Clone(command.Epoch).(*agentv1.Epoch), CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded",
		Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: "running"}},
	}
	if err := s.RecordAgentResult(t.Context(), claim.InstallationID, node, result); err != nil {
		t.Fatal(err)
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordStepCompletion(t.Context(), claim, uuid.MustParse(command.StepId), result.Epoch, sha256.Sum256(encoded)); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestLoadExecutionStateReadsThisRun(t *testing.T) {
	s, _, claim, plan, node, session := readsFixture(t)
	first, second := plan.Steps[0].Command, plan.Steps[1].Command
	state, err := s.LoadExecutionState(t.Context(), claim)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(state.Plan, plan) {
		t.Fatal("the read changed the immutable plan")
	}
	if state.Status != operation.Running || !state.Active || state.CancellationRequestedAt != nil || state.ProgressSequence != 0 {
		t.Fatalf("state %+v", state)
	}
	if len(state.Barriers) != 1 || state.Barriers[0].NodeID != node || state.Barriers[0].SessionID != session || !state.Barriers[0].Reconciled || !state.Barriers[0].Report.Drained {
		t.Fatalf("barriers %+v", state.Barriers)
	}
	if state.Recovery != nil {
		t.Fatal("a checkpoint appeared without a write")
	}
	if len(state.Steps) != 2 {
		t.Fatalf("steps %d, want 2", len(state.Steps))
	}
	for i, step := range state.Steps {
		if step.StepID != uuid.MustParse(plan.Steps[i].Command.StepId) || step.Dispatch != nil || step.Result != nil || step.Completion != nil || step.Delivery != nil {
			t.Fatalf("step %d before dispatch: %+v", i, step)
		}
	}

	if err := s.RecordDispatch(t.Context(), claim, first); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordDispatch(t.Context(), claim, second); err != nil {
		t.Fatal(err)
	}
	admit(t, s, claim, node, session, first, "terminal", "")
	attempt := admit(t, s, claim, node, session, second, "rejected", "command_refused")
	result := proveStep(t, s, claim, node, first)
	// A delivery attempt requires a stored recovery frontier for this claim.
	frontier := sha256.Sum256([]byte("frontier"))
	if err := s.RecordRecoveryCheckpoint(t.Context(), claim, frontier, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: 1, Status: operation.Running, Reason: "first step proved"}); err != nil {
		t.Fatal(err)
	}

	state, err = s.LoadExecutionState(t.Context(), claim)
	if err != nil {
		t.Fatal(err)
	}
	if state.ProgressSequence != 1 || state.Recovery == nil || state.Recovery.FrontierHash != frontier || len(state.Recovery.InspectionIDs) != 0 {
		t.Fatalf("progress %d recovery %+v", state.ProgressSequence, state.Recovery)
	}
	proved := state.Steps[0]
	if !proto.Equal(proved.Dispatch, first) || !proto.Equal(proved.ResultCommand, first) || !proto.Equal(proved.Result, result) {
		t.Fatalf("proved step %+v", proved)
	}
	if proved.Admission == nil || proved.Admission.State != "terminal" || proved.AdmissionSessionID != session || proved.Rejection != nil {
		t.Fatalf("proved admission %+v", proved)
	}
	if proved.Completion == nil || proved.Completion.StepID != proved.StepID || proved.Completion.ReceiptEpoch.ExecutionEpoch != claim.ExecutionEpoch {
		t.Fatalf("completion %+v", proved.Completion)
	}
	pending := state.Steps[1]
	if !proto.Equal(pending.Dispatch, second) || pending.Result != nil || pending.ResultCommand != nil || pending.Completion != nil {
		t.Fatalf("pending step %+v", pending)
	}
	if pending.Admission != nil || pending.Rejection == nil || pending.Rejection.ErrorCode != "command_refused" || pending.RejectionSessionID != session {
		t.Fatalf("pending admission %+v", pending)
	}
	if pending.Delivery == nil || pending.Delivery.ID != attempt || pending.Delivery.Count != 1 || pending.Delivery.SessionID != session {
		t.Fatalf("delivery %+v", pending.Delivery)
	}
}

func TestLoadExecutionStateKeepsHistoricalReceipts(t *testing.T) {
	s, request, claim, plan, node, session := readsFixture(t)
	first := plan.Steps[0].Command
	if err := s.RecordDispatch(t.Context(), claim, first); err != nil {
		t.Fatal(err)
	}
	admit(t, s, claim, node, session, first, "terminal", "")
	result := proveStep(t, s, claim, node, first)

	second := plan.Steps[1].Command
	if err := s.RecordDispatch(t.Context(), claim, second); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordRecoveryCheckpoint(t.Context(), claim, sha256.Sum256([]byte("frontier")), nil); err != nil {
		t.Fatal(err)
	}
	attempt := uuid.New()
	if _, err := s.RecordDeliveryAttempt(t.Context(), claim, uuid.MustParse(second.CommandId), session, attempt, time.Millisecond); err != nil {
		t.Fatal(err)
	}

	if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	next := request
	next.ClaimID = uuid.New()
	next.OwnerID = "owner-2"
	takeover, err := s.AcquireClaim(t.Context(), next)
	if err != nil {
		t.Fatal(err)
	}
	if takeover.ExecutionEpoch == claim.ExecutionEpoch {
		t.Fatal("takeover kept the old execution epoch")
	}
	if _, err := s.LoadExecutionState(t.Context(), claim); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("old owner read state: %v", err)
	}
	drainNode(t, s, takeover, node, session)

	state, err := s.LoadExecutionState(t.Context(), takeover)
	if err != nil {
		t.Fatal(err)
	}
	pending := state.Steps[1]
	if pending.Dispatch != nil || pending.Admission != nil || pending.Delivery == nil || pending.Delivery.ID != attempt || pending.Delivery.Count != 1 {
		t.Fatalf("takeover lost the delivery budget or inherited authority: %+v", pending)
	}
	proved := state.Steps[0]
	if proved.Dispatch != nil || proved.Admission != nil || proved.Delivery == nil || proved.UnansweredDeliveries != 0 {
		t.Fatalf("the new run inherited dispatch records: %+v", proved)
	}
	if proved.Completion == nil || proved.Completion.ReceiptEpoch.ExecutionEpoch != claim.ExecutionEpoch {
		t.Fatalf("completion lost its epoch: %+v", proved.Completion)
	}
	if !proto.Equal(proved.Result, result) || !proto.Equal(proved.ResultCommand, first) {
		t.Fatalf("historical receipt lost: %+v", proved)
	}

	// The same command under the new epoch is a separate dispatch identity.
	current := proto.Clone(first).(*agentv1.Command)
	current.Epoch.ExecutionEpoch = takeover.ExecutionEpoch
	if err := s.RecordDispatch(t.Context(), takeover, current); err != nil {
		t.Fatal(err)
	}
	state, err = s.LoadExecutionState(t.Context(), takeover)
	if err != nil {
		t.Fatal(err)
	}
	proved = state.Steps[0]
	if !proto.Equal(proved.Dispatch, current) || !proto.Equal(proved.ResultCommand, first) || !proto.Equal(proved.Result, result) {
		t.Fatalf("epochs merged: %+v", proved)
	}
}

func TestLoadDispatchResolvesStableIdentity(t *testing.T) {
	s, _, claim, plan, node, session := readsFixture(t)
	first := plan.Steps[0].Command
	cursor := DispatchCursor{CommandID: uuid.MustParse(first.CommandId), ExecutionEpoch: claim.ExecutionEpoch, RecoveryGeneration: claim.RecoveryGeneration}
	if _, err := s.LoadDispatch(t.Context(), claim, cursor); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("undispatched command: %v", err)
	}
	if err := s.RecordDispatch(t.Context(), claim, first); err != nil {
		t.Fatal(err)
	}
	evidence, err := s.LoadDispatch(t.Context(), claim, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(evidence.Command, first) || evidence.Admission != nil || evidence.Result != nil {
		t.Fatalf("dispatch %+v", evidence)
	}
	admit(t, s, claim, node, session, first, "running", "")
	result := proveStep(t, s, claim, node, first)
	if evidence, err = s.LoadDispatch(t.Context(), claim, cursor); err != nil {
		t.Fatal(err)
	}
	if evidence.Admission == nil || evidence.Admission.State != "running" || !proto.Equal(evidence.Result, result) {
		t.Fatalf("acknowledged dispatch %+v", evidence)
	}
	for name, broken := range map[string]DispatchCursor{
		"no command":     {ExecutionEpoch: cursor.ExecutionEpoch, RecoveryGeneration: cursor.RecoveryGeneration},
		"no epoch":       {CommandID: cursor.CommandID, RecoveryGeneration: cursor.RecoveryGeneration},
		"no generation":  {CommandID: cursor.CommandID, ExecutionEpoch: cursor.ExecutionEpoch},
		"negative epoch": {CommandID: cursor.CommandID, ExecutionEpoch: -1, RecoveryGeneration: cursor.RecoveryGeneration},
	} {
		_, err := s.LoadDispatch(t.Context(), claim, broken)
		if got, ok := errors.AsType[*AdmissionError](err); !ok || got.Code != InvalidRequest {
			t.Fatalf("%s: %v", name, err)
		}
	}
	other := cursor
	other.ExecutionEpoch = claim.ExecutionEpoch + 5
	if _, err := s.LoadDispatch(t.Context(), claim, other); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("unknown epoch: %v", err)
	}
}

func TestLoadExecutionStateRequiresAnInstalledPlan(t *testing.T) {
	s, _, claim := claimFixture(t)
	if _, err := s.LoadExecutionState(t.Context(), claim); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("plan version zero: %v", err)
	}
	stale := claim
	stale.OwnerID = "owner-2"
	if _, err := s.LoadExecutionState(t.Context(), stale); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("other owner: %v", err)
	}
}

// Concurrent owners of one claim must read the same committed state without deadlock.
func TestLoadExecutionStateUnderConcurrentReads(t *testing.T) {
	s, _, claim, plan, node, session := readsFixture(t)
	first := plan.Steps[0].Command
	if err := s.RecordDispatch(t.Context(), claim, first); err != nil {
		t.Fatal(err)
	}
	admit(t, s, claim, node, session, first, "terminal", "")
	proveStep(t, s, claim, node, first)
	states := make(chan ExecutionState, 16)
	errs := make(chan error, 16)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			<-start
			state, err := s.LoadExecutionState(t.Context(), claim)
			states <- state
			errs <- err
		})
	}
	close(start)
	wg.Wait()
	close(states)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for state := range states {
		if len(state.Steps) != 2 || state.Steps[0].Completion == nil || !proto.Equal(state.Steps[0].Dispatch, first) || state.Steps[1].Dispatch != nil {
			t.Fatalf("inconsistent read: %+v", state.Steps)
		}
	}
}
