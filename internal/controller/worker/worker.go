// Package worker executes one durably claimed operation plan.
// It commits every decision before it sends a command. It never infers a native outcome.
package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

var ErrSessionUnavailable = errors.New("agent_session_unavailable")
var ErrEvidencePending = errors.New("evidence_pending")
var ErrUnboundEvidence = errors.New("unbound_evidence")

// Transport sends controller messages to one authenticated agent session.
// *controlstream.Server satisfies it.
type Transport interface {
	Session(nodeID uuid.UUID) (controlstream.Peer, bool)
	Send(nodeID uuid.UUID, sessionID string, message *agentv1.ControllerMessage) error
}

// Authority is the elected ownership term that fences management dispatch.
// *coordinator.Leadership satisfies it.
type Authority interface {
	Verify(ctx context.Context) error
}

// Config binds one worker to one claimed operation.
type Config struct {
	Store     *postgres.Store
	Transport Transport
	Authority Authority
	Claim     operation.Claim
	Operation operation.Operation
	// EvidenceWait bounds one wait for committed agent evidence.
	EvidenceWait time.Duration
	// RetryDelay is the delay that a committed delivery attempt reserves.
	RetryDelay time.Duration
	// SessionMaxAge is the oldest presence record that can carry a dispatch.
	SessionMaxAge time.Duration
}

// Worker drives one claim under elected and durable ownership.
type Worker struct {
	config  Config
	mu      sync.Mutex
	changed chan struct{}
	// requested holds the boundary each command was asked to stop at, so a
	// cancellation is sent once instead of on every pass of the loop.
	requested map[string]agentv1.CancellationBoundary
	// answered holds the last cancellation answer for a command. The loop owns
	// the status, so it folds the answer into durable progress rather than the
	// receive path writing a status of its own.
	answered map[string]string
}

// New checks the worker inputs and binds it to one claimed operation.
func New(config Config) (*Worker, error) {
	if config.Store == nil || config.Transport == nil || config.Authority == nil {
		return nil, errors.New("worker requires a store, a transport, and an elected authority")
	}
	claim, op := config.Claim, config.Operation
	if claim.ID == uuid.Nil || claim.ExecutionEpoch < 1 || claim.RecoveryGeneration == uuid.Nil || claim.OwnerID == "" {
		return nil, errors.New("worker requires a complete claim")
	}
	if op.ID != claim.OperationID || op.InstallationID != claim.InstallationID || op.ClusterID != claim.ClusterID {
		return nil, errors.New("the claim and the operation name different work")
	}
	if config.EvidenceWait == 0 {
		config.EvidenceWait = 10 * time.Second
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 500 * time.Millisecond
	}
	if config.SessionMaxAge == 0 {
		config.SessionMaxAge = 20 * time.Second
	}
	if config.EvidenceWait < time.Second || config.EvidenceWait > time.Minute || config.RetryDelay < time.Millisecond || config.RetryDelay > time.Minute || config.SessionMaxAge < time.Second || config.SessionMaxAge > time.Minute {
		return nil, errors.New("invalid worker timings")
	}
	return &Worker{config: config, changed: make(chan struct{}), requested: make(map[string]agentv1.CancellationBoundary), answered: make(map[string]string)}, nil
}

// Run drives one claim from preparation to a terminal status.
// It stops at the first loss of either ownership authority.
// It stops without a status change when evidence that it needs has not arrived.
// Work that an agent already admitted stays Running for a later owner.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.Prepare(ctx); err != nil {
		return err
	}
	for {
		// Subscribe before the read. Evidence that commits after it must still wake the wait.
		changed := w.subscribe()
		state, completed, err := w.resume(ctx)
		if err != nil {
			return err
		}
		if finished(state.Status) {
			return nil
		}
		if err := w.checkpoint(ctx, state, completed); err != nil {
			return err
		}
		found, err := w.reconcileReceipts(ctx, state)
		if err != nil {
			return err
		}
		if found.retained > 0 {
			// New completions release dependent steps, so the next pass reads them.
			// A cancelling operation cannot return to running.
			reason := "steps_proved: " + strconv.Itoa(found.retained) + " plan steps are proved by durable receipts"
			if err := w.progress(ctx, state.ProgressSequence+1, ongoing(state.Status), reason, false); err != nil {
				return err
			}
			continue
		}
		if stop, err := w.conclude(ctx, state, completed, found); err != nil || stop {
			return err
		}
		if state.CancellationRequestedAt != nil {
			return w.cancel(ctx, state, found)
		}
		pass, err := w.dispatch(ctx, state, completed)
		if errors.Is(err, postgres.ErrAttemptsExhausted) {
			return w.progress(ctx, state.ProgressSequence+1, operation.Failed, err.Error(), false)
		}
		if err != nil {
			return err
		}
		if pass.committed == 0 && pass.retryAt.IsZero() && !inFlight(state) {
			return fmt.Errorf("%w: no plan step can advance", ErrEvidencePending)
		}
		if err := w.await(ctx, changed, pass.retryAt); err != nil {
			return err
		}
	}
}

// ongoing is the non-terminal status that reports continued work from the current status.
// An operation that an operator is cancelling stays cancelling.
func ongoing(status operation.Status) operation.Status {
	if status == operation.Cancelling {
		return operation.Cancelling
	}
	return operation.Running
}

// finished reports a durable status that no owner can change.
func finished(status operation.Status) bool {
	return status == operation.Succeeded || status == operation.Failed || status == operation.Cancelled
}

// inFlight reports a command that this run sent and that no receipt has answered.
func inFlight(state postgres.ExecutionState) bool {
	return slices.ContainsFunc(state.Steps, func(evidence postgres.StepEvidence) bool {
		return evidence.Dispatch != nil && evidence.Result == nil
	})
}

// verdict is what one pass over durable receipts concluded.
type verdict struct {
	// retained counts receipts that this pass turned into step completions.
	retained int
	// failure names the first step that a receipt proved did not succeed.
	failure string
	// unknown names the first receipt that left a native effect running or unresolved.
	unknown string
	// stopped counts receipts that prove a step stopped at its cancellation boundary.
	stopped int
}

// reconcileReceipts turns proved receipts into durable step completions.
// It never treats a receipt as proof of a postcondition that the receipt does not show.
func (w *Worker) reconcileReceipts(ctx context.Context, state postgres.ExecutionState) (verdict, error) {
	var found verdict
	for i, evidence := range state.Steps {
		if evidence.Completion != nil || evidence.Result == nil {
			continue
		}
		receipt, target := evidence.Result, "step "+evidence.StepID.String()+" on node "+state.Plan.Steps[i].Command.Epoch.NodeId
		switch {
		case unresolved(receipt):
			if found.unknown == "" {
				found.unknown = "effect_unresolved: " + target + " left a native effect running or unresolved"
			}
			continue
		case receipt.State == "cancelled" && state.CancellationRequestedAt != nil:
			// The operator asked for this. A step that stopped at its declared
			// boundary is not a failure, and it proves no postcondition either.
			found.stopped++
			continue
		case receipt.State != "succeeded":
			found.record("step_failed: " + target + " reported " + receipt.State + " " + receipt.FailureCode)
			continue
		case checkResult(state.Plan.Steps[i], receipt) != nil:
			found.record("postcondition_unproved: " + target + " did not prove its postcondition")
			continue
		}
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(receipt)
		if err != nil {
			return found, err
		}
		if err := w.config.Store.RecordStepCompletion(ctx, w.config.Claim, evidence.StepID, receipt.Epoch, sha256.Sum256(encoded)); err != nil {
			return found, err
		}
		found.retained++
	}
	return found, nil
}

func (v *verdict) record(failure string) {
	if v.failure == "" {
		v.failure = failure
	}
}

// unresolved reports a receipt that left a native effect running or its outcome unknown.
func unresolved(receipt *agentv1.TerminalResult) bool {
	return slices.ContainsFunc(receipt.Effects, func(effect *agentv1.Effect) bool {
		return effect == nil || effect.Active || effect.OutcomeUnknown
	})
}

// conclude records the status that durable evidence decides.
// It reports whether the worker must stop. An unknown outcome stops without a terminal status.
func (w *Worker) conclude(ctx context.Context, state postgres.ExecutionState, completed map[string]bool, found verdict) (bool, error) {
	switch {
	case found.unknown != "":
		// Unresolved native state outranks a failure. An operator must see it.
		// A cancelling operation cannot move to waiting, so cancel reports the blocker.
		if state.Status == operation.Cancelling {
			return false, nil
		}
		if state.Status == operation.Waiting {
			return true, nil
		}
		return true, w.progress(ctx, state.ProgressSequence+1, operation.Waiting, found.unknown, true)
	case found.failure != "":
		return true, w.progress(ctx, state.ProgressSequence+1, operation.Failed, found.failure, false)
	case len(completed) == len(state.Plan.Steps):
		return true, w.progress(ctx, state.ProgressSequence+1, operation.Succeeded, "plan_proved: every plan step is proved by a durable receipt", false)
	}
	return false, nil
}

// cancel stops forward dispatch and asks each holding agent to stop its command.
// It finishes only when nothing is outstanding. An unresolved effect keeps the
// operation cancelling, because nobody can prove the native state stopped.
//
// Cancellation is a durable request, not a promise of rollback. Nothing here
// removes a native effect to make the request succeed.
func (w *Worker) cancel(ctx context.Context, state postgres.ExecutionState, found verdict) error {
	if err := w.requestCancellation(ctx, state); err != nil {
		return err
	}
	blocker := found.unknown
	if blocker == "" && inFlight(state) {
		blocker = w.holding(state)
	}
	sequence := state.ProgressSequence
	if state.Status != operation.Cancelling {
		reason := "cancellation_requested: an operator requested cancellation"
		if blocker != "" {
			reason = "cancellation_blocked: " + blocker
		}
		sequence++
		if err := w.progress(ctx, sequence, operation.Cancelling, reason, false); err != nil {
			return err
		}
	}
	if blocker != "" {
		return fmt.Errorf("%w: %s", ErrEvidencePending, blocker)
	}
	reason := "cancelled_clean: no command was in flight and no effect was unresolved"
	if found.stopped > 0 {
		reason = "cancelled_at_boundary: " + strconv.Itoa(found.stopped) + " steps stopped at their cancellation boundary and left no unresolved effect"
	}
	return w.progress(ctx, sequence+1, operation.Cancelled, reason, false)
}

// requestCancellation asks the exact session that admitted each outstanding
// command to stop it. A command that no session has admitted is not held by any
// agent, so nothing is sent for it and the worker waits for its admission.
func (w *Worker) requestCancellation(ctx context.Context, state postgres.ExecutionState) error {
	for i, evidence := range state.Steps {
		if evidence.Dispatch == nil || evidence.Result != nil || evidence.Admission == nil {
			continue
		}
		commandID := evidence.Dispatch.CommandId
		if w.asked(commandID) {
			continue
		}
		nodeID, err := uuid.Parse(evidence.Dispatch.Epoch.NodeId)
		if err != nil {
			return err
		}
		// The admitting session is the only one that provably holds the command.
		// A later session never inherited it, so it is never asked to stop it.
		peer, ok := w.config.Transport.Session(nodeID)
		if !ok || peer.SessionID != evidence.AdmissionSessionID.String() {
			continue
		}
		if err := w.config.Authority.Verify(ctx); err != nil {
			return err
		}
		request := &agentv1.CancelCommand{CommandId: commandID, Epoch: proto.Clone(evidence.Dispatch.Epoch).(*agentv1.Epoch)}
		message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Cancel{Cancel: request}}
		if err := w.config.Transport.Send(nodeID, peer.SessionID, message); err != nil {
			// A busy session is retried on the next pass. Any other send failure
			// leaves the command outstanding, which keeps the operation cancelling.
			if errors.Is(err, controlstream.ErrBusy) {
				continue
			}
			return err
		}
		w.markAsked(commandID, state.Plan.Steps[i].CancellationBoundary)
	}
	return nil
}

// holding names why an outstanding command keeps the operation cancelling.
// It reports the agent's own answer when one arrived, because "not cancellable"
// and "no answer yet" are different facts to an operator.
func (w *Worker) holding(state postgres.ExecutionState) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, evidence := range state.Steps {
		if evidence.Dispatch == nil || evidence.Result != nil {
			continue
		}
		commandID := evidence.Dispatch.CommandId
		boundary, sent := w.requested[commandID]
		switch answer := w.answered[commandID]; {
		case answer == "not_cancellable":
			return "command " + commandID + " passed its " + boundaryName(boundary) + " boundary and must finish"
		case answer != "":
			return "command " + commandID + " answered " + answer + " and has not reported a result"
		case sent:
			return "command " + commandID + " was asked to stop at its " + boundaryName(boundary) + " boundary and has not answered"
		}
	}
	return "a dispatched command is in flight"
}

// asked reports a command that this run already asked an agent to stop.
func (w *Worker) asked(commandID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, sent := w.requested[commandID]
	return sent
}

// markAsked records one sent cancellation and the boundary it was sent under.
func (w *Worker) markAsked(commandID string, boundary agentv1.CancellationBoundary) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.requested[commandID] = boundary
}

// boundaryName reads a declared cancellation boundary as the reason text an
// operator sees. Before-execution stops only work the agent has not begun.
// Safe-stop lets the agent stop at its own declared point. Neither one permits
// interrupting a native action part-way.
func boundaryName(boundary agentv1.CancellationBoundary) string {
	if boundary == agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP {
		return "safe stop"
	}
	return "before execution"
}

// progress records one status change of the claimed operation.
func (w *Worker) progress(ctx context.Context, sequence int64, status operation.Status, reason string, unknown bool) error {
	if err := w.config.Authority.Verify(ctx); err != nil {
		return err
	}
	return w.config.Store.UpdateProgress(ctx, w.config.Claim, postgres.ProgressUpdate{Sequence: sequence, Status: status, Reason: reason, UnknownEffects: unknown})
}

// Handle commits agent evidence that needs claim context, then wakes the loop.
// It binds every record to the exact session that the transport authenticated.
// It never grants authority and it never reconciles a barrier.
func (w *Worker) Handle(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
	sessionID, err := uuid.Parse(peer.SessionID)
	if err != nil || sessionID == uuid.Nil || peer.InstallationID != w.config.Claim.InstallationID || message == nil {
		return ErrUnboundEvidence
	}
	switch value := message.Message.(type) {
	case *agentv1.AgentMessage_BarrierResult:
		err = w.config.Store.RecordSessionBarrier(ctx, w.config.Claim, sessionID, value.BarrierResult)
	case *agentv1.AgentMessage_AdmissionAck:
		err = w.config.Store.RecordAgentAdmission(ctx, peer.InstallationID, peer.NodeID, sessionID, value.AdmissionAck)
	case *agentv1.AgentMessage_TerminalResult:
		// agentstream.Receiver commits the same receipt. Both paths are idempotent.
		err = w.config.Store.RecordAgentResult(ctx, peer.InstallationID, peer.NodeID, value.TerminalResult)
	case *agentv1.AgentMessage_CancellationResult:
		// This is the agent's answer to a request, not proof that work stopped.
		// The durable proof is the receipt, which the agent sends separately. The
		// loop owns the status, so the answer is held for it to record.
		err = w.recordCancellation(peer, value.CancellationResult)
	default:
		return ErrUnboundEvidence
	}
	if err != nil {
		return err
	}
	w.notify()
	return nil
}

// recordCancellation holds one agent answer for the loop to report.
// It binds the answer to a command this worker actually asked to stop, so a
// session cannot report on work it was never given.
func (w *Worker) recordCancellation(peer controlstream.Peer, result *agentv1.CancellationResult) error {
	if result == nil || result.Epoch == nil || result.CommandId == "" {
		return ErrUnboundEvidence
	}
	if result.Epoch.NodeId != peer.NodeID.String() || result.Epoch.ClusterId != w.config.Claim.ClusterID.String() {
		return ErrUnboundEvidence
	}
	if result.State != "cancelled" && result.State != "not_cancellable" {
		return ErrUnboundEvidence
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	// An answer this run never asked for is ordinary: a previous owner sent the
	// request, and this worker started with an empty record of it. The durable
	// proof is the receipt, so dropping the answer loses nothing. Failing here
	// would close the agent stream over a message that carries no evidence.
	if _, sent := w.requested[result.CommandId]; !sent {
		return nil
	}
	w.answered[result.CommandId] = result.State
	return nil
}

// Prepare drains and reconciles every affected node, then activates the claim.
// A drained bit alone never reconciles a node. Every reported effect must be resolved.
// Each node receives at most one drain request. A node that stays undrained ends the call.
func (w *Worker) Prepare(ctx context.Context) error {
	requested := make(map[uuid.UUID]bool)
	for {
		// Subscribe before the read. Evidence that commits after it must still wake the wait.
		changed := w.subscribe()
		state, _, err := w.resume(ctx)
		if err != nil {
			return err
		}
		if state.Active || finished(state.Status) {
			return nil
		}
		waiting := false
		for _, barrier := range state.Barriers {
			switch {
			case barrier.Reconciled:
			case barrier.Report != nil && operation.BarrierDrained(barrier.Report) == nil:
				if err := w.reconcile(ctx, barrier); err != nil {
					return err
				}
			case barrier.Report != nil && requested[barrier.NodeID]:
				return fmt.Errorf("%w: node %s did not report a drained barrier", ErrEvidencePending, barrier.NodeID)
			default:
				if !requested[barrier.NodeID] {
					requested[barrier.NodeID] = true
					if err := w.requestBarrier(ctx, barrier.NodeID); err != nil {
						return err
					}
				}
				waiting = true
			}
		}
		if !waiting {
			if err := w.config.Authority.Verify(ctx); err != nil {
				return err
			}
			return w.config.Store.ActivateClaim(ctx, w.config.Claim)
		}
		if err := w.await(ctx, changed, time.Time{}); err != nil {
			return err
		}
	}
}

// resume reads current ownership state and the steps that durable evidence already proved.
func (w *Worker) resume(ctx context.Context) (postgres.ExecutionState, map[string]bool, error) {
	if err := w.config.Authority.Verify(ctx); err != nil {
		return postgres.ExecutionState{}, nil, err
	}
	state, err := w.config.Store.LoadExecutionState(ctx, w.config.Claim)
	if err != nil {
		return postgres.ExecutionState{}, nil, err
	}
	if err := validatePlan(state.Plan, w.config.Operation); err != nil {
		return postgres.ExecutionState{}, nil, err
	}
	completed := make(map[string]bool, len(state.Steps))
	for i, evidence := range state.Steps {
		if evidence.Completion == nil {
			continue
		}
		if err := checkResult(state.Plan.Steps[i], evidence.Result); err != nil {
			return postgres.ExecutionState{}, nil, fmt.Errorf("step %s: %w", evidence.StepID, err)
		}
		completed[evidence.StepID.String()] = true
	}
	return state, completed, nil
}

// checkpoint fixes the frontier that this ownership run inherited.
// It writes once for the claim. Work that this run proves does not change that frontier.
func (w *Worker) checkpoint(ctx context.Context, state postgres.ExecutionState, completed map[string]bool) error {
	if state.Recovery != nil {
		return nil
	}
	_, hash, err := recoveryFrontier(state.Plan, completed)
	if err != nil {
		return err
	}
	return w.config.Store.RecordRecoveryCheckpoint(ctx, w.config.Claim, hash, nil)
}

// reconcile records the checked residual state of one drained node.
func (w *Worker) reconcile(ctx context.Context, barrier postgres.BarrierEvidence) error {
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(barrier.Report)
	if err != nil {
		return err
	}
	summary := "Checked " + strconv.Itoa(len(barrier.Report.Effects)) + " reported effects. None is active and none has an unknown outcome."
	return w.config.Store.ReconcileBarrier(ctx, w.config.Claim, barrier.NodeID, sha256.Sum256(encoded), summary)
}

// requestBarrier asks one current session to close admissions and drain its work.
func (w *Worker) requestBarrier(ctx context.Context, nodeID uuid.UUID) error {
	peer, _, err := w.session(ctx, nodeID)
	if err != nil {
		return err
	}
	return w.config.Transport.Send(nodeID, peer.SessionID, &agentv1.ControllerMessage{
		Message: &agentv1.ControllerMessage_PrepareEpoch{PrepareEpoch: &agentv1.PrepareEpoch{Epoch: w.epoch(nodeID)}},
	})
}

// dispatchPass is what one dispatch pass committed and what it waits for next.
type dispatchPass struct {
	// committed counts commands that this pass durably recorded as dispatched.
	committed int
	// retryAt is the earliest reserved retry time. It is zero when nothing is deferred.
	retryAt time.Time
}

// dispatch commits a dispatch and an attempt before any command leaves the controller.
// A full transport queue or a reserved retry defers one step. It does not end the pass.
// An exhausted retry budget ends the call. The caller fails the operation.
func (w *Worker) dispatch(ctx context.Context, state postgres.ExecutionState, completed map[string]bool) (dispatchPass, error) {
	var pass dispatchPass
	for i, step := range state.Plan.Steps {
		evidence := state.Steps[i]
		if evidence.Completion != nil || evidence.Admission != nil || evidence.Result != nil {
			continue
		}
		if !ready(step, completed) {
			continue
		}
		// PostgreSQL does not fence etcd, so elected authority needs its own check.
		// RecordDispatch below rechecks the durable claim inside its own transaction.
		if err := w.config.Authority.Verify(ctx); err != nil {
			return pass, err
		}
		nodeID, err := uuid.Parse(step.Command.Epoch.NodeId)
		if err != nil {
			return pass, err
		}
		peer, sessionID, err := w.session(ctx, nodeID)
		if err != nil {
			return pass, err
		}
		commandID, err := uuid.Parse(step.Command.CommandId)
		if err != nil {
			return pass, err
		}
		command := proto.Clone(step.Command).(*agentv1.Command)
		command.Epoch.ExecutionEpoch = w.config.Claim.ExecutionEpoch
		command.Epoch.RecoveryGeneration = w.config.Claim.RecoveryGeneration.String()
		// The dispatch identity must exist before an attempt can reserve the retry budget.
		if err := w.config.Store.RecordDispatch(ctx, w.config.Claim, command); err != nil {
			return pass, err
		}
		pass.committed++
		if _, err := w.config.Store.RecordDeliveryAttempt(ctx, w.config.Claim, commandID, sessionID, uuid.New(), w.config.RetryDelay); err != nil {
			if errors.Is(err, postgres.ErrRetryPending) {
				pass.retryAt = earlier(pass.retryAt, w.retryTime(evidence))
				continue
			}
			if errors.Is(err, postgres.ErrAttemptsExhausted) {
				return pass, fmt.Errorf("%w: step %s on node %s used its whole delivery budget%s", err, step.Command.StepId, nodeID, refusal(evidence))
			}
			return pass, err
		}
		message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Command{Command: command}}
		if err := w.config.Transport.Send(nodeID, peer.SessionID, message); err != nil {
			// The attempt is spent and the command did not leave the controller.
			// A later attempt reuses the same command ID and input hash.
			if errors.Is(err, controlstream.ErrBusy) {
				pass.retryAt = earlier(pass.retryAt, time.Now().Add(w.config.RetryDelay))
				continue
			}
			return pass, err
		}
	}
	return pass, nil
}

// retryTime is the reserved next attempt time of one step.
func (w *Worker) retryTime(evidence postgres.StepEvidence) time.Time {
	if evidence.Delivery != nil {
		return evidence.Delivery.NextAttemptAt
	}
	return time.Now().Add(w.config.RetryDelay)
}

// earlier folds one candidate into the earliest time seen so far.
func earlier(current, candidate time.Time) time.Time {
	if current.IsZero() || candidate.Before(current) {
		return candidate
	}
	return current
}

// refusal names the agent error code that explains a command the agent would not admit.
func refusal(evidence postgres.StepEvidence) string {
	if evidence.Rejection == nil || evidence.Rejection.ErrorCode == "" {
		return ""
	}
	return " after the agent refused it with " + evidence.Rejection.ErrorCode
}

// ready reports whether durable evidence proved every declared dependency.
func ready(step *agentv1.PlanStep, completed map[string]bool) bool {
	for _, dependency := range step.DependsOn {
		if !completed[dependency] {
			return false
		}
	}
	return true
}

// session requires a connected peer and a current database-observed presence record.
func (w *Worker) session(ctx context.Context, nodeID uuid.UUID) (controlstream.Peer, uuid.UUID, error) {
	peer, ok := w.config.Transport.Session(nodeID)
	if !ok {
		return controlstream.Peer{}, uuid.Nil, ErrSessionUnavailable
	}
	sessionID, err := uuid.Parse(peer.SessionID)
	if err != nil || sessionID == uuid.Nil {
		return controlstream.Peer{}, uuid.Nil, ErrSessionUnavailable
	}
	if _, err := w.config.Store.CheckAgentSession(ctx, w.config.Claim, nodeID, sessionID, w.config.SessionMaxAge); err != nil {
		return controlstream.Peer{}, uuid.Nil, err
	}
	return peer, sessionID, nil
}

func (w *Worker) epoch(nodeID uuid.UUID) *agentv1.Epoch {
	claim := w.config.Claim
	return &agentv1.Epoch{
		InstallationId: claim.InstallationID.String(), NodeId: nodeID.String(), ClusterId: claim.ClusterID.String(),
		RecoveryGeneration: claim.RecoveryGeneration.String(), ExecutionEpoch: claim.ExecutionEpoch,
	}
}

// subscribe must run before a send so that a fast reply cannot pass the wait.
func (w *Worker) subscribe() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.changed
}

func (w *Worker) notify() {
	w.mu.Lock()
	defer w.mu.Unlock()
	close(w.changed)
	w.changed = make(chan struct{})
}

// await waits for committed evidence, or for a reserved retry when one is due sooner.
// A retry wake is normal progress. Only the evidence deadline ends the run.
func (w *Worker) await(ctx context.Context, changed <-chan struct{}, retryAt time.Time) error {
	wait, retry := w.config.EvidenceWait, false
	if !retryAt.IsZero() {
		if until := time.Until(retryAt); until < wait {
			wait, retry = max(until, 0), true
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-changed:
		return nil
	case <-timer.C:
		if retry {
			return nil
		}
		return ErrEvidencePending
	case <-ctx.Done():
		return ctx.Err()
	}
}
