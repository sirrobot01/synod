package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/coordinator"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func claimFixture(t *testing.T) (*Store, ClaimRequest, operation.Claim) {
	t.Helper()
	s := testStore(t)
	request := testRequest()
	admission, err := s.CreateCluster(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	r := ClaimRequest{ClaimID: uuid.New(), InstallationID: request.Identity.InstallationID, OperationID: admission.OperationID, RecoveryGeneration: uuid.New(), OwnerID: "owner-1", TTL: 30 * time.Second, LeaseID: 1, LeaseRevision: 1}
	if err := s.InitializeGeneration(t.Context(), r.InstallationID, r.RecoveryGeneration); err != nil {
		t.Fatal(err)
	}
	claim, err := s.AcquireClaim(t.Context(), r)
	if err != nil {
		t.Fatalf("claim: %v / %v", err, errors.Unwrap(err))
	}
	return s, r, claim
}

func claimPlan(t *testing.T, claim operation.Claim) *agentv1.ExecutionPlan {
	t.Helper()
	command := &agentv1.Command{Epoch: &agentv1.Epoch{InstallationId: claim.InstallationID.String(), NodeId: uuid.NewString(), ClusterId: claim.ClusterID.String(), RecoveryGeneration: claim.RecoveryGeneration.String(), ExecutionEpoch: claim.ExecutionEpoch}, OperationId: claim.OperationID.String(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: 1, PlanVersion: 1,
		Input: &agentv1.CommandInput{ResourceId: uuid.NewString(), PayloadVersion: 1, Timeout: durationpb.New(time.Minute), Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}}}
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	return &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{{Command: command, MaxAttempts: 3, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING}}}
}

// reconcilePlan reports a drained barrier for each step. An agent must keep reporting
// an effect it reported as active or unknown, so resolved effects stay in the report.
func reconcilePlan(t *testing.T, s *Store, claim operation.Claim, plan *agentv1.ExecutionPlan, resolved ...*agentv1.Effect) {
	t.Helper()
	for _, step := range plan.Steps {
		epoch := proto.Clone(step.Command.Epoch).(*agentv1.Epoch)
		epoch.ExecutionEpoch = claim.ExecutionEpoch
		report := &agentv1.BarrierResult{Epoch: epoch, AdmissionsClosed: true, Drained: true, Effects: resolved}
		if err := s.RecordBarrier(t.Context(), claim, report); err != nil {
			t.Fatal(err)
		}
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.ReconcileBarrier(t.Context(), claim, uuid.MustParse(epoch.NodeId), sha256.Sum256(encoded), "Inspected the node. No active or unknown effects remain."); err != nil {
			t.Fatal(err)
		}
	}
}

func TestClaimIdentityExpiryAndTakeover(t *testing.T) {
	s, r, first := claimFixture(t)
	if first.ExecutionEpoch != 1 || first.Active || len(first.AffectedNodes) != 0 {
		t.Fatalf("initial claim: %+v", first)
	}
	replay, err := s.AcquireClaim(t.Context(), r)
	if err != nil || replay.ID != first.ID || replay.ExecutionEpoch != first.ExecutionEpoch || replay.ExpiresAt != first.ExpiresAt {
		t.Fatalf("claim replay: %+v: %v", replay, err)
	}
	other := r
	other.ClaimID = uuid.New()
	other.OwnerID = "owner-2"
	if _, err := s.AcquireClaim(t.Context(), other); !errors.Is(err, operation.ErrClaimConflict) {
		t.Fatalf("live claim conflict: %v", err)
	}
	later, err := s.RenewClaim(t.Context(), first, time.Minute)
	if err != nil || !later.After(first.ExpiresAt) {
		t.Fatalf("renewal: %v", err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenewClaim(t.Context(), first, time.Minute); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("revived expired owner: %v", err)
	}
	if _, err := s.AcquireClaim(t.Context(), r); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("revived old claim ID: %v", err)
	}
	second, err := s.AcquireClaim(t.Context(), other)
	if err != nil || second.ExecutionEpoch != 2 {
		t.Fatalf("takeover: %+v: %v", second, err)
	}
	if _, err := s.RenewClaim(t.Context(), first, time.Minute); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("old epoch renewed: %v", err)
	}
	if err := s.InitializeGeneration(t.Context(), r.InstallationID, uuid.New()); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("ordinary generation change: %v", err)
	}
}

func TestConcurrentClaims(t *testing.T) {
	s, r, _ := claimFixture(t)
	if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 16)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			next := r
			next.ClaimID = uuid.New()
			next.OwnerID = fmt.Sprintf("owner-%d", i)
			<-start
			_, err := s.AcquireClaim(t.Context(), next)
			results <- err
		})
	}
	close(start)
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, operation.ErrClaimConflict) {
			t.Fatal(err)
		}
	}
	if successes != 1 {
		t.Fatalf("claim winners: %d", successes)
	}
	var epoch int64
	if err := s.pool.QueryRow(t.Context(), `SELECT execution_epoch FROM clusters`).Scan(&epoch); err != nil || epoch != 2 {
		t.Fatalf("epoch %d: %v", epoch, err)
	}
}

func TestPlanBarrierAndDelayedDispatch(t *testing.T) {
	s, r, claim := claimFixture(t)
	plan := claimPlan(t, claim)
	command := plan.Steps[0].Command
	if err := s.ActivateClaim(t.Context(), claim); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("activated without plan: %v", err)
	}
	if err := s.RecordDispatch(t.Context(), claim, command); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("dispatched without plan: %v", err)
	}
	if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	changed := proto.Clone(plan).(*agentv1.ExecutionPlan)
	changed.Steps[0].MaxAttempts++
	if err := s.InstallPlan(t.Context(), claim, changed); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("rewrote immutable plan: %v", err)
	}
	if err := s.ActivateClaim(t.Context(), claim); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("activated without barrier: %v", err)
	}
	report := &agentv1.BarrierResult{Epoch: command.Epoch, AdmissionsClosed: true, Drained: true, Effects: []*agentv1.Effect{{NativeOperationId: "backup-1", Active: true}}}
	if err := s.RecordBarrier(t.Context(), claim, report); err != nil {
		t.Fatal(err)
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileBarrier(t.Context(), claim, uuid.MustParse(command.Epoch.NodeId), sha256.Sum256(encoded), "Native backup remains active."); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("ignored active native effect: %v", err)
	}
	reconcilePlan(t, s, claim, plan, &agentv1.Effect{NativeOperationId: "backup-1"})
	if err := s.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if err := s.ValidateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordDispatch(t.Context(), claim, command); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordDispatch(t.Context(), claim, command); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	next := r
	next.ClaimID = uuid.New()
	next.OwnerID = "owner-2"
	takeover, err := s.AcquireClaim(t.Context(), next)
	if err != nil {
		t.Fatal(err)
	}
	if len(takeover.AffectedNodes) != 1 || takeover.AffectedNodes[0].String() != command.Epoch.NodeId {
		t.Fatalf("lost affected agent: %+v", takeover)
	}
	if err := s.RecordDispatch(t.Context(), claim, command); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("old owner dispatched: %v", err)
	}
	if err := s.RecordBarrier(t.Context(), takeover, report); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("accepted old barrier result: %v", err)
	}
	if err := s.ActivateClaim(t.Context(), takeover); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("takeover ignored offline node: %v", err)
	}
	persisted, err := s.ReadPlan(t.Context(), takeover)
	if err != nil || !proto.Equal(plan, persisted) {
		t.Fatalf("plan changed during takeover: %v", err)
	}
	reconcilePlan(t, s, takeover, plan)
	if err := s.ActivateClaim(t.Context(), takeover); err != nil {
		t.Fatal(err)
	}
	newCommand := proto.Clone(command).(*agentv1.Command)
	newCommand.Epoch.ExecutionEpoch = takeover.ExecutionEpoch
	if err := s.RecordDispatch(t.Context(), takeover, newCommand); err != nil {
		t.Fatal(err)
	}
	var targets, identities int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM command_dispatches`).Scan(&targets); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM command_identities`).Scan(&identities); err != nil {
		t.Fatal(err)
	}
	if targets != 2 || identities != 1 {
		t.Fatalf("dispatch history %d, command identities %d", targets, identities)
	}
}

func TestProgressUnknownEffectsAndStaleEpoch(t *testing.T) {
	host, claim, plan := completionFixture(t)
	s := host.store
	r := ClaimRequest{InstallationID: claim.InstallationID, OperationID: claim.OperationID,
		RecoveryGeneration: claim.RecoveryGeneration, TTL: time.Minute,
		LeaseID: 1, LeaseRevision: 1}
	update := ProgressUpdate{Sequence: 1, Status: operation.Waiting, Reason: "Inspect the uncertain result.", UnknownEffects: true}
	if err := s.UpdateProgress(t.Context(), claim, update); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateProgress(t.Context(), claim, update); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateClaim(t.Context(), claim); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("activation bypassed unknown effects: %v", err)
	}
	changed := update
	changed.UnknownEffects = false
	if err := s.UpdateProgress(t.Context(), claim, changed); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("changed progress replay: %v", err)
	}
	_, err := s.pool.Exec(t.Context(), `INSERT INTO operations SELECT (jsonb_populate_record(NULL::operations,to_jsonb(o)||jsonb_build_object('id',$1::text,'claim_id',NULL,'recovery_generation',NULL,'lease_id',NULL,'lease_revision',NULL,'owner_id',NULL,'claim_expires_at',NULL,'claim_active',false))).* FROM operations o`, uuid.NewString())
	requireCode(t, admissionError(err), OperationConflict)
	terminal := ProgressUpdate{Sequence: 2, Status: operation.Failed, Reason: "Unknown is not a known failure.", UnknownEffects: true}
	requireCode(t, s.UpdateProgress(t.Context(), claim, terminal), InvalidRequest)
	if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	r.ClaimID = uuid.New()
	r.OwnerID = "owner-2"
	takeover, err := s.AcquireClaim(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateProgress(t.Context(), claim, update); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("old epoch progress: %v", err)
	}
	reconcilePlan(t, s, takeover, plan)
	if err := s.ActivateClaim(t.Context(), takeover); err != nil {
		t.Fatal(err)
	}
	completeSteps(t, s, takeover, plan)
	if err := s.UpdateProgress(t.Context(), takeover, ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "All postconditions passed."}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenewClaim(t.Context(), takeover, time.Minute); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("renewed terminal operation: %v", err)
	}
}

func TestClaimCommitReplyRecovery(t *testing.T) {
	s, r, _ := claimFixture(t)
	if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	r.ClaimID = uuid.New()
	r.OwnerID = "owner-2"
	s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	result, err := s.AcquireClaim(t.Context(), r)
	requireCode(t, err, OutcomeUnknown)
	if result.ID != uuid.Nil {
		t.Fatal("returned unconfirmed claim")
	}
	s.commitTx = nil
	recovered, err := s.AcquireClaim(t.Context(), r)
	if err != nil || recovered.ID != r.ClaimID || recovered.ExecutionEpoch != 2 {
		t.Fatalf("claim recovery: %+v: %v", recovered, err)
	}
}

func TestClaimChecksTimeAfterLockWait(t *testing.T) {
	s, _, claim := claimFixture(t)
	locker, err := s.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(t.Context())
	if _, err := locker.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()+interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := s.RenewClaim(t.Context(), claim, time.Minute); result <- err }()
	waitForKeyWait(t, s)
	if _, err := locker.Exec(t.Context(), `SELECT pg_sleep(1.1)`); err != nil {
		t.Fatal(err)
	}
	if err := locker.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("renewed after expiry during lock wait: %v", err)
	}
}

func TestCoordinatorAndPostgresAuthority(t *testing.T) {
	s, r, old := claimFixture(t)
	coord, err := coordinator.New(s.pool)
	if err != nil {
		t.Fatal(err)
	}
	leader, err := coord.Campaign(t.Context(), r.InstallationID, old.ClusterID, "real-leader", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close(t.Context())
	if err := leader.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	r.ClaimID = uuid.New()
	r.OwnerID = leader.OwnerID
	r.LeaseID = leader.LeaseID
	r.LeaseRevision = leader.Revision
	claim, err := s.AcquireClaim(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := leader.Verify(t.Context()); !errors.Is(err, coordinator.ErrNotLeader) {
		t.Fatalf("lease loss not reported: %v", err)
	}
	// Operation claim can remain live after lease loss. Dispatch requires both.
	if _, err := s.RenewClaim(t.Context(), claim, time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestHistoricalClaimIDCannotBeReused(t *testing.T) {
	s, original, _ := claimFixture(t)
	if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	next := original
	next.ClaimID = uuid.New()
	next.OwnerID = "owner-2"
	if _, err := s.AcquireClaim(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireClaim(t.Context(), original); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("reused historical claim ID: %v", err)
	}
	var epoch int64
	if err := s.pool.QueryRow(t.Context(), `SELECT execution_epoch FROM clusters`).Scan(&epoch); err != nil || epoch != 2 {
		t.Fatalf("rejected claim changed epoch %d: %v", epoch, err)
	}
}

func TestChangedRecoveryGenerationInvalidatesClaim(t *testing.T) {
	s, request, claim := claimFixture(t)
	generation := uuid.New()
	if _, err := s.pool.Exec(t.Context(), `UPDATE installation_generations SET recovery_generation=$1`, generation); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenewClaim(t.Context(), claim, time.Minute); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("old generation retained authority: %v", err)
	}
	request.RecoveryGeneration = generation
	if _, err := s.AcquireClaim(t.Context(), request); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("claim replay adopted a changed generation: %v", err)
	}
}

func TestInstallPostgresRecoveryPlan(t *testing.T) {
	s, _, claim := claimFixture(t)
	plan := claimPlan(t, claim)
	command := plan.Steps[0].Command
	admin, application := uuid.NewString(), uuid.NewString()
	command.Input.Secrets = []*agentv1.SecretReference{{CredentialId: admin, Version: 1, Purpose: "superuser"}, {CredentialId: application, Version: 1, Purpose: "application"}}
	command.Input.Payload = &agentv1.CommandInput_RecoverPostgresSetup{RecoverPostgresSetup: &agentv1.RecoverPostgresSetup{Postgres: &agentv1.PostgresContainerConfig{Port: 5432, SuperuserCredentialId: admin, SuperuserCredentialVersion: 1, ApplicationCredentialId: application, ApplicationCredentialVersion: 1}}}
	plan.Steps[0].Postcondition = agentv1.Postcondition_POSTCONDITION_POSTGRES_SETUP_RECOVERED
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	changed := proto.Clone(plan).(*agentv1.ExecutionPlan)
	changed.Steps[0].Command.Input.GetRecoverPostgresSetup().Postgres.Port++
	hash, err = operation.CommandHash(changed.Steps[0].Command)
	if err != nil {
		t.Fatal(err)
	}
	changed.Steps[0].Command.InputHash = hash[:]
	if err := s.InstallPlan(t.Context(), claim, changed); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("recovery plan identity changed: %v", err)
	}
}

func TestInstallPatroniStartFailureProof(t *testing.T) {
	s, _, claim := claimFixture(t)
	plan := claimPlan(t, claim)
	step := plan.Steps[0]
	var refs []*agentv1.SecretReference
	for _, purpose := range []string{"superuser", "application", "replication", "patroni_rewind", "patroni_rest", "patroni_member_tls", "patroni_dcs_tls"} {
		refs = append(refs, &agentv1.SecretReference{CredentialId: uuid.NewString(), Version: 1, Purpose: purpose})
	}
	member := &agentv1.EnsureContainer{Engine: "postgres", ImageDigest: "postgres@sha256:" + strings.Repeat("a", 64), VolumeId: uuid.NewString(), NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 64 << 20,
		Postgres: &agentv1.PostgresContainerConfig{Port: 15432, SuperuserCredentialId: refs[0].CredentialId, SuperuserCredentialVersion: 1, ApplicationCredentialId: refs[1].CredentialId, ApplicationCredentialVersion: 1, ReplicationCredentialId: refs[2].CredentialId, ReplicationCredentialVersion: 1,
			Patroni: &agentv1.PatroniContainerConfig{Address: "192.0.2.10", RestPort: 8008, DcsEndpoints: []string{"192.0.2.20:2379"}, PeerNetworks: []string{"192.0.2.0/24"}, ClientNetworks: []string{"198.51.100.0/24"}, ProvisionId: uuid.NewString(), AllowInitialization: true, RequiredFreeBytes: 1 << 30, Credentials: refs}}}
	step.Command.Input.Payload = &agentv1.CommandInput_VerifyPatroniStartFailure{VerifyPatroniStartFailure: &agentv1.VerifyPatroniStartFailure{Container: member}}
	step.Postcondition = agentv1.Postcondition_POSTCONDITION_PATRONI_START_FAILED
	hash, err := operation.CommandHash(step.Command)
	if err != nil {
		t.Fatal(err)
	}
	step.Command.InputHash = hash[:]
	if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal("native failure proof plan refused", err)
	}
	if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal("native failure proof plan replay refused", err)
	}
	changed := proto.Clone(plan).(*agentv1.ExecutionPlan)
	changed.Steps[0].Command.Input.GetVerifyPatroniStartFailure().Container.Postgres.Patroni.ProvisionId = uuid.NewString()
	hash, err = operation.CommandHash(changed.Steps[0].Command)
	if err != nil {
		t.Fatal(err)
	}
	changed.Steps[0].Command.InputHash = hash[:]
	if err := s.InstallPlan(t.Context(), claim, changed); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("native provision identity changed after plan installation: %v", err)
	}
}
