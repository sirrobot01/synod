package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func workerFixture(t *testing.T) (*Store, operation.Claim, *agentv1.ExecutionPlan, uuid.UUID) {
	t.Helper()
	s, _, claim := claimFixture(t)
	plan := claimPlan(t, claim)
	if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	node := uuid.MustParse(plan.Steps[0].Command.Epoch.NodeId)
	// Seed enrolled identity for this Store-only fixture. TLS/enrollment has separate integration tests.
	if _, err := s.pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at) VALUES ($1,$2,$3,clock_timestamp())`, node, claim.InstallationID, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	sessionID := uuid.New()
	hello := &agentv1.Hello{InstallationId: claim.InstallationID.String(), NodeId: node.String(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1}}
	if err := s.RecordAgentSession(t.Context(), claim.InstallationID, node, sessionID, hello); err != nil {
		t.Fatal(err)
	}
	return s, claim, plan, sessionID
}

func TestWorkerSessionBarrierAndInactiveWait(t *testing.T) {
	s, claim, plan, sessionID := workerFixture(t)
	node := uuid.MustParse(plan.Steps[0].Command.Epoch.NodeId)
	presence, err := s.CheckAgentSession(t.Context(), claim, node, sessionID, 20*time.Second)
	if err != nil || presence.SessionID != sessionID {
		t.Fatalf("session: %v", err)
	}
	if err := s.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: 1, Status: operation.Waiting, Reason: "barrier_pending", UnknownEffects: true}); err != nil {
		t.Fatal(err)
	}
	var active bool
	if err := s.pool.QueryRow(t.Context(), `SELECT claim_active FROM operations WHERE id=$1`, claim.OperationID).Scan(&active); err != nil || active {
		t.Fatal("wait activated claim", err)
	}
	if err := s.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: 2, Status: operation.Running, Reason: "premature"}); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("inactive running: %v", err)
	}
	report := &agentv1.BarrierResult{Epoch: proto.Clone(plan.Steps[0].Command.Epoch).(*agentv1.Epoch), AdmissionsClosed: true, Effects: []*agentv1.Effect{{CommandId: plan.Steps[0].Command.CommandId, ResourceId: plan.Steps[0].Command.Input.ResourceId, NativeOperationId: "retained", Active: true, OutcomeUnknown: true, State: "unknown"}}}
	if err := s.RecordSessionBarrier(t.Context(), claim, sessionID, report); err != nil {
		t.Fatal(err)
	}
	omitted := proto.Clone(report).(*agentv1.BarrierResult)
	omitted.Effects = nil
	omitted.Drained = true
	if err := s.RecordSessionBarrier(t.Context(), claim, sessionID, omitted); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("omitted effect: %v", err)
	}
	report.Effects[0].Active = false
	report.Effects[0].OutcomeUnknown = false
	report.Effects[0].State = "resolved"
	report.Drained = true
	s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.EOF
	}
	requireCode(t, s.RecordSessionBarrier(t.Context(), claim, sessionID, report), OutcomeUnknown)
	s.commitTx = nil
	if err := s.RecordSessionBarrier(t.Context(), claim, sessionID, report); err != nil {
		t.Fatal(err)
	}
	encoded, _ := (proto.MarshalOptions{Deterministic: true}).Marshal(report)
	if err := s.ReconcileBarrier(t.Context(), claim, node, sha256.Sum256(encoded), "retained native effect inspected"); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if err := s.ValidateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerSessionReplacementAndFreshness(t *testing.T) {
	s, claim, plan, sessionID := workerFixture(t)
	node := uuid.MustParse(plan.Steps[0].Command.Epoch.NodeId)
	presence, err := s.GetAgentPresence(t.Context(), claim.InstallationID, node)
	if err != nil {
		t.Fatal(err)
	}
	report := &agentv1.BarrierResult{Epoch: plan.Steps[0].Command.Epoch, AdmissionsClosed: true, Drained: true}
	if err := s.RecordSessionBarrier(t.Context(), claim, sessionID, report); err != nil {
		t.Fatal(err)
	}
	next := uuid.New()
	if err := s.RecordAgentSession(t.Context(), claim.InstallationID, node, next, presence.Hello); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CheckAgentSession(t.Context(), claim, node, sessionID, 20*time.Second); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old session: %v", err)
	}
	if err := s.RecordSessionBarrier(t.Context(), claim, next, report); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("barrier session replaced: %v", err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE agent_presence SET last_seen_at=clock_timestamp()-interval '1 minute' WHERE node_id=$1`, node); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CheckAgentSession(t.Context(), claim, node, next, 20*time.Second); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("stale presence: %v", err)
	}
	presence.Hello.JournalGeneration = uuid.NewString()
	third := uuid.New()
	if err := s.RecordAgentSession(t.Context(), claim.InstallationID, node, third, presence.Hello); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CheckAgentSession(t.Context(), claim, node, third, 20*time.Second); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("changed journal: %v", err)
	}
}

func TestWorkerAdmissionReceiptIsMonotonic(t *testing.T) {
	s, claim, plan, sessionID := workerFixture(t)
	reconcilePlan(t, s, claim, plan)
	if err := s.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	command := plan.Steps[0].Command
	if err := s.RecordDispatch(t.Context(), claim, command); err != nil {
		t.Fatal(err)
	}
	node := uuid.MustParse(command.Epoch.NodeId)
	if err := s.RecordRecoveryCheckpoint(t.Context(), claim, [32]byte{1}, nil); err != nil {
		t.Fatal(err)
	}
	attempt, err := s.RecordDeliveryAttempt(t.Context(), claim, uuid.MustParse(command.CommandId), sessionID, uuid.New(), time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ack := &agentv1.AdmissionAck{DeliveryAttemptId: attempt.ID.String(), Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, State: "running"}
	s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.EOF
	}
	requireCode(t, s.RecordAgentAdmission(t.Context(), claim.InstallationID, node, sessionID, ack), OutcomeUnknown)
	s.commitTx = nil
	var wg sync.WaitGroup
	errs := make(chan error, 24)
	for i := range 24 {
		wg.Go(func() {
			next := proto.Clone(ack).(*agentv1.AdmissionAck)
			next.State = []string{"queued", "running", "terminal"}[i%3]
			errs <- s.RecordAgentAdmission(t.Context(), claim.InstallationID, node, sessionID, next)
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	ack.State = "rejected"
	ack.ErrorCode = "agent_busy"
	if err := s.RecordAgentAdmission(t.Context(), claim.InstallationID, node, sessionID, ack); err != nil {
		t.Fatal(err)
	}
	var rank int
	var accepted, rejected []byte
	if err := s.pool.QueryRow(t.Context(), `SELECT accepted_rank,accepted_bytes,rejection_bytes FROM command_admissions WHERE command_id=$1`, command.CommandId).Scan(&rank, &accepted, &rejected); err != nil {
		t.Fatal(err)
	}
	var stored agentv1.AdmissionAck
	if proto.Unmarshal(accepted, &stored) != nil || rank != 3 || stored.State != "terminal" {
		t.Fatal("admission evidence regressed")
	}
	if proto.Unmarshal(rejected, &stored) != nil || stored.ErrorCode != "agent_busy" {
		t.Fatal("rejection was lost")
	}
	if err := s.RecordAgentAdmission(t.Context(), claim.InstallationID, node, uuid.New(), ack); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("foreign session: %v", err)
	}
	ack.InputHash = make([]byte, 32)
	if err := s.RecordAgentAdmission(t.Context(), claim.InstallationID, node, sessionID, ack); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("changed input: %v", err)
	}
	current, err := s.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
	if err != nil || current.Status != operation.Running {
		t.Fatal("admission changed operation", err)
	}
}

func TestAdmissionBindsDeliveryAttempt(t *testing.T) {
	s, _, claim, plan, node, session := readsFixture(t)
	if err := s.RecordRecoveryCheckpoint(t.Context(), claim, [32]byte{1}, nil); err != nil {
		t.Fatal(err)
	}
	var attempts []DeliveryAttempt
	for _, step := range plan.Steps {
		if err := s.RecordDispatch(t.Context(), claim, step.Command); err != nil {
			t.Fatal(err)
		}
		attempt, err := s.RecordDeliveryAttempt(t.Context(), claim, uuid.MustParse(step.Command.CommandId), session, uuid.New(), time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		attempts = append(attempts, attempt)
	}
	command := plan.Steps[0].Command
	ack := &agentv1.AdmissionAck{DeliveryAttemptId: attempts[0].ID.String(), Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, State: "rejected", ErrorCode: "agent_busy"}
	for name, change := range map[string]func(*agentv1.AdmissionAck){
		"missing attempt":       func(a *agentv1.AdmissionAck) { a.DeliveryAttemptId = "" },
		"unknown attempt":       func(a *agentv1.AdmissionAck) { a.DeliveryAttemptId = uuid.NewString() },
		"other command attempt": func(a *agentv1.AdmissionAck) { a.DeliveryAttemptId = attempts[1].ID.String() },
		"wrong command":         func(a *agentv1.AdmissionAck) { a.CommandId = plan.Steps[1].Command.CommandId },
		"wrong epoch":           func(a *agentv1.AdmissionAck) { a.Epoch.ExecutionEpoch++ },
		"wrong generation":      func(a *agentv1.AdmissionAck) { a.Epoch.RecoveryGeneration = uuid.NewString() },
	} {
		t.Run(name, func(t *testing.T) {
			broken := proto.Clone(ack).(*agentv1.AdmissionAck)
			change(broken)
			if err := s.RecordAgentAdmission(t.Context(), claim.InstallationID, node, session, broken); err == nil {
				t.Fatal("mismatched reply was accepted")
			}
		})
	}
	state, err := s.LoadExecutionState(t.Context(), claim)
	if err != nil || state.Steps[0].Rejection != nil || state.Steps[0].UnansweredDeliveries != 1 {
		t.Fatalf("invalid evidence changed the pending delivery: %+v %v", state, err)
	}
	if err := s.RecordAgentAdmission(t.Context(), claim.InstallationID, node, session, ack); err != nil {
		t.Fatal(err)
	}
	state, err = s.LoadExecutionState(t.Context(), claim)
	if err != nil || state.Steps[0].UnansweredDeliveries != 0 || state.Steps[1].UnansweredDeliveries != 1 {
		t.Fatalf("refusal settled another command: %+v %v", state, err)
	}
	presence, err := s.GetAgentPresence(t.Context(), claim.InstallationID, node)
	if err != nil {
		t.Fatal(err)
	}
	next := uuid.New()
	if err := s.RecordAgentSession(t.Context(), claim.InstallationID, node, next, presence.Hello); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAgentAdmission(t.Context(), claim.InstallationID, node, session, ack); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("replaced session: %v", err)
	}
	if err := s.RecordAgentAdmission(t.Context(), claim.InstallationID, node, next, ack); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("new session used an old delivery: %v", err)
	}
}
