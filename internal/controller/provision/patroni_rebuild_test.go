package provision

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// rebuildPatroniMember previews and admits the rebuild of one member, then plans it.
func rebuildPatroniMember(t *testing.T, f *patroniFixture, target postgres.Member) *agentv1.ExecutionPlan {
	t.Helper()
	preview, err := f.store.PreviewMemberRebuild(t.Context(), f.claim.InstallationID, f.claim.ClusterID, target.MemberID, "integration")
	if err != nil {
		t.Fatal(err)
	}
	if preview.Action != "rebuild_member" || preview.MemberID != target.MemberID || preview.ResourceID != target.ResourceID || len(preview.RetainedVolumes) != 1 {
		t.Fatalf("member rebuild preview: %+v", preview)
	}
	admission, err := f.store.AdmitClusterAction(t.Context(), postgres.ClusterActionRequest{
		Identity:  postgres.RequestIdentity{InstallationID: f.claim.InstallationID, ActorID: "integration", APIOperation: postgres.RebuildMemberAPIOperation, IdempotencyKey: uuid.NewString()},
		RequestID: uuid.NewString(), ClusterID: f.claim.ClusterID, ExpectedRevision: preview.ExpectedRevision,
		Confirmation: &cluster.ActionConfirmation{PlanToken: preview.PlanToken, Confirm: true, DataDisposition: "retain"},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{
		ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: admission.OperationID,
		RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "owner-rebuild-member", TTL: 2 * time.Minute,
		CoordinationKey: "/lifecycle/rebuild-member", CoordinationLease: 1, CoordinationRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	f.fixture.claim = claim
	return installedPlanFor(t, f.fixture, claim.OperationID)
}

// A member rebuild proves the other two members ready and the target failed before it
// removes the target. The fresh member keeps the node and address and gets new storage,
// ports, and a member certificate. Completion needs exactly one writer.
func TestPatroniMemberRebuildReplacesFailedMember(t *testing.T) {
	f := createdPatroni(t)
	created := installedPlan(t, f.fixture)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(members) != 3 {
		t.Fatalf("members: %+v %v", members, err)
	}
	target := members[0]
	var stored *agentv1.EnsureContainer
	var node string
	for _, step := range created.Steps {
		if container := step.Command.Input.GetEnsureContainer(); container != nil && step.Command.Input.ResourceId == target.ResourceID.String() {
			stored, node = container, step.Command.Epoch.NodeId
		}
	}
	plan := rebuildPatroniMember(t, f, target)
	// Member steps, then remove the target's routes, add the fresh member's routes, replace two routers, and check six routes.
	if operation.MemberRebuildPrefix(plan) != 10 || len(plan.Steps) != 36 {
		t.Fatalf("rebuild plan has %d steps, want ten member steps and 26 route steps", len(plan.Steps))
	}
	for _, step := range plan.Steps[:2] {
		check := step.Command.Input.GetCheckReadiness()
		if check.GetPostgresRole() != "any" || step.Command.Input.ResourceId == target.ResourceID.String() || len(step.DependsOn) != 0 {
			t.Fatalf("peer readiness: %+v", check)
		}
	}
	notServing := plan.Steps[2].Command.Input.GetVerifyPatroniMemberNotServing()
	failed := plan.Steps[4].Command.Input.GetVerifyPatroniMemberFailed()
	if notServing == nil || failed == nil || !proto.Equal(notServing.Container, stored) || !proto.Equal(failed.Container, stored) {
		t.Fatal("failure proofs do not carry the stored member configuration")
	}
	if !slices.Equal(plan.Steps[5].DependsOn, []string{plan.Steps[3].Command.StepId, plan.Steps[4].Command.StepId}) {
		t.Fatal("removal does not wait for the stop and the storage proof")
	}
	fresh := plan.Steps[7].Command
	configuration := fresh.Input.GetEnsureContainer()
	native, old := configuration.GetPostgres().GetPatroni(), stored.Postgres.Patroni
	if fresh.Epoch.NodeId != node || fresh.Input.ResourceId == target.ResourceID.String() || native.Address != old.Address ||
		!slices.Equal(native.PeerNetworks, old.PeerNetworks) || !slices.Equal(native.DcsEndpoints, old.DcsEndpoints) ||
		configuration.VolumeId == stored.VolumeId || configuration.Postgres.Port == stored.Postgres.Port || native.RestPort == old.RestPort ||
		native.ProvisionId == old.ProvisionId || !native.AllowInitialization || configuration.ImageDigest != stored.ImageDigest {
		t.Fatalf("fresh member configuration: %+v", configuration)
	}
	for index, reference := range native.Credentials {
		same := proto.Equal(reference, old.Credentials[index])
		if (reference.Purpose == postgres.PatroniMemberTLSCredential) == same {
			t.Fatalf("credential %s: fresh member reuses=%v", reference.Purpose, same)
		}
	}
	if begin := plan.Steps[8].Command.Input.GetStartPatroniMember(); begin == nil || !begin.AllowInitialization || !proto.Equal(begin.Container, configuration) {
		t.Fatal("fresh member start does not repeat its configuration")
	}
	if check := plan.Steps[9].Command.Input.GetCheckReadiness(); check.GetPostgresRole() != "any" || check.Port != configuration.Postgres.Port {
		t.Fatalf("fresh member readiness: %+v", check)
	}
	// A repeated plan keeps the installed reservation and certificate.
	if err := f.planner.Plan(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(installedPlanFor(t, f.fixture, f.claim.OperationID), plan) {
		t.Fatal("a repeated plan changed the installed plan")
	}
	roles := map[string]string{plan.Steps[0].Command.StepId: "ready_primary", plan.Steps[1].Command.StepId: "ready_replica", plan.Steps[9].Command.StepId: "ready_replica"}
	recordPatroniReceiptsWith(t, f, plan.Steps, roles)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	live, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(live) != 3 {
		t.Fatalf("members after rebuild: %+v %v", live, err)
	}
	for _, member := range live {
		if member.MemberID == target.MemberID || member.State != "active" || member.Role != "patroni" {
			t.Fatalf("member after rebuild: %+v", member)
		}
	}
	var retention string
	var released bool
	if err := f.pool.QueryRow(t.Context(), `SELECT
 (SELECT retention FROM volume_identities WHERE installation_id=$1 AND resource_id=$2),
 (SELECT released_at IS NOT NULL FROM resource_allocations WHERE installation_id=$1 AND resource_id=$3)`,
		f.claim.InstallationID, uuid.MustParse(stored.VolumeId), target.ResourceID).Scan(&retention, &released); err != nil {
		t.Fatal(err)
	}
	if retention != string(postgres.Retained) || !released {
		t.Fatalf("old volume retention=%s reservation released=%v", retention, released)
	}
	endpoints, err := f.store.InspectClusterEndpoints(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(endpoints.Endpoints) != 6 || endpoints.WriterURL == "" || endpoints.ReaderURL == "" {
		t.Fatalf("endpoints after rebuild: %+v %v", endpoints, err)
	}
}

// A rebuild completes only with exactly one writer among the three readiness checks.
func TestPatroniMemberRebuildNeedsOneWriter(t *testing.T) {
	f := createdPatroni(t)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	plan := rebuildPatroniMember(t, f, members[1])
	roles := map[string]string{plan.Steps[0].Command.StepId: "ready_primary", plan.Steps[1].Command.StepId: "ready_primary", plan.Steps[9].Command.StepId: "ready_replica"}
	recordPatroniReceiptsWith(t, f, plan.Steps, roles)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); !errors.Is(err, operation.ErrPostconditionUnproved) {
		t.Fatalf("rebuild with two writers: %v", err)
	}
}

// A failed rebuild retires the old member only with removal proof. Without that proof,
// it discards the undispatched fresh reservation and keeps the old member.
func TestPatroniMemberRebuildFailureSettles(t *testing.T) {
	for name, proved := range map[string]int{"before removal": 3, "after removal": 6} {
		t.Run(name, func(t *testing.T) {
			f := createdPatroni(t)
			members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			target := members[2]
			plan := recordRebuildPrefix(t, f, target, proved)
			if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Failed, Reason: "Fixture terminal boundary.", FailureCode: "fixture_failure"}); err != nil {
				t.Fatal(err)
			}
			states := map[uuid.UUID]string{}
			var removed []uuid.UUID
			rows, err := f.pool.Query(t.Context(), `SELECT resource_id,state,removed_at IS NOT NULL FROM cluster_members WHERE installation_id=$1 AND cluster_id=$2`, f.claim.InstallationID, f.claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var resource uuid.UUID
				var state string
				var gone bool
				if err := rows.Scan(&resource, &state, &gone); err != nil {
					t.Fatal(err)
				}
				if gone {
					removed = append(removed, resource)
				} else {
					states[resource] = state
				}
			}
			rows.Close()
			fresh := uuid.MustParse(plan.Steps[7].Command.Input.ResourceId)
			if proved == 6 {
				if !slices.Equal(removed, []uuid.UUID{target.ResourceID}) || states[fresh] != "residual" {
					t.Fatalf("after removal proof: removed=%v states=%v", removed, states)
				}
				return
			}
			if !slices.Equal(removed, []uuid.UUID{fresh}) || states[target.ResourceID] != "active" || len(states) != 3 {
				t.Fatalf("before removal proof: removed=%v states=%v", removed, states)
			}
		})
	}
}

// recordRebuildPrefix plans a rebuild of target and records receipts for its first steps.
func recordRebuildPrefix(t *testing.T, f *patroniFixture, target postgres.Member, steps int) *agentv1.ExecutionPlan {
	t.Helper()
	plan := rebuildPatroniMember(t, f, target)
	roles := map[string]string{plan.Steps[0].Command.StepId: "ready_primary", plan.Steps[1].Command.StepId: "ready_replica"}
	recordPatroniReceiptsWith(t, f, plan.Steps[:steps], roles)
	return plan
}

// A member rebuild preview names one active member of a three-member Patroni cluster.
// Admission replays exactly and refuses a membership that moved after review.
func TestPatroniMemberRebuildAdmission(t *testing.T) {
	f := createdPatroni(t)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	code := func(err error) postgres.ErrorCode {
		refused, _ := errors.AsType[*postgres.AdmissionError](err)
		if refused == nil {
			return ""
		}
		return refused.Code
	}
	if _, err := f.store.PreviewMemberRebuild(t.Context(), f.claim.InstallationID, f.claim.ClusterID, uuid.New(), "integration"); code(err) != postgres.NotFound {
		t.Fatalf("preview of an unknown member: %v", err)
	}
	preview, err := f.store.PreviewMemberRebuild(t.Context(), f.claim.InstallationID, f.claim.ClusterID, members[0].MemberID, "integration")
	if err != nil {
		t.Fatal(err)
	}
	request := postgres.ClusterActionRequest{
		Identity:  postgres.RequestIdentity{InstallationID: f.claim.InstallationID, ActorID: "integration", APIOperation: postgres.RebuildMemberAPIOperation, IdempotencyKey: uuid.NewString()},
		RequestID: uuid.NewString(), ClusterID: f.claim.ClusterID, ExpectedRevision: preview.ExpectedRevision,
		Confirmation: &cluster.ActionConfirmation{PlanToken: preview.PlanToken, Confirm: true, DataDisposition: "retain"},
	}
	// A member that is no longer active makes the reviewed plan stale.
	if _, err := f.pool.Exec(t.Context(), `UPDATE cluster_members SET state='residual' WHERE installation_id=$1 AND member_id=$2`, f.claim.InstallationID, members[1].MemberID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AdmitClusterAction(t.Context(), request); code(err) != postgres.OperationConflict {
		t.Fatalf("admission after membership moved: %v", err)
	}
	if _, err := f.store.PreviewMemberRebuild(t.Context(), f.claim.InstallationID, f.claim.ClusterID, members[0].MemberID, "integration"); code(err) != postgres.OperationConflict {
		t.Fatalf("preview with a residual member: %v", err)
	}
	if _, err := f.pool.Exec(t.Context(), `UPDATE cluster_members SET state='active' WHERE installation_id=$1 AND member_id=$2`, f.claim.InstallationID, members[1].MemberID); err != nil {
		t.Fatal(err)
	}
	request.Identity.IdempotencyKey = uuid.NewString()
	first, err := f.store.AdmitClusterAction(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.store.AdmitClusterAction(t.Context(), request)
	if err != nil || !again.Replayed || again.OperationID != first.OperationID {
		t.Fatalf("replay: %+v %v", again, err)
	}
	op, err := f.store.GetOperation(t.Context(), f.claim.InstallationID, first.OperationID)
	if err != nil || op.Kind != "cluster.rebuild_member" {
		t.Fatalf("admitted operation: %+v %v", op, err)
	}
}

// A standalone cluster has no member rebuild.
func TestMemberRebuildRefusesStandalone(t *testing.T) {
	f := newFixture(t, postgresSpec())
	_, err := f.store.PreviewMemberRebuild(t.Context(), f.claim.InstallationID, f.claim.ClusterID, uuid.New(), "integration")
	if refused, ok := errors.AsType[*postgres.AdmissionError](err); !ok || refused.Code != postgres.UnsupportedSpec {
		t.Fatalf("standalone member rebuild preview: %v", err)
	}
}

// Plan validation refuses a rebuild that skips a proof, moves the member, or reorders removal.
func TestPatroniMemberRebuildPlanShape(t *testing.T) {
	f := createdPatroni(t)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	plan := rebuildPatroniMember(t, f, members[0])
	op, err := f.store.GetOperation(t.Context(), f.claim.InstallationID, f.claim.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := operation.ValidatePlan(plan, op); err != nil {
		t.Fatal(err)
	}
	for name, tamper := range map[string]func(*agentv1.ExecutionPlan){
		"removal before storage proof": func(p *agentv1.ExecutionPlan) { p.Steps[5].DependsOn = p.Steps[5].DependsOn[:1] },
		"proof before peers ready":     func(p *agentv1.ExecutionPlan) { p.Steps[2].DependsOn = nil },
		"member on another node":       func(p *agentv1.ExecutionPlan) { p.Steps[7].Command.Epoch.NodeId = p.Steps[0].Command.Epoch.NodeId },
		"peer check names the target": func(p *agentv1.ExecutionPlan) {
			p.Steps[1].Command.Input.ResourceId = p.Steps[2].Command.Input.ResourceId
		},
		"start without initialization": func(p *agentv1.ExecutionPlan) {
			p.Steps[8].Command.Input.GetStartPatroniMember().AllowInitialization = false
		},
		"primary-only final check": func(p *agentv1.ExecutionPlan) { p.Steps[9].Command.Input.GetCheckReadiness().PostgresRole = "primary" },
		"missing final check":      func(p *agentv1.ExecutionPlan) { p.Steps = p.Steps[:9] },
	} {
		tampered := proto.Clone(plan).(*agentv1.ExecutionPlan)
		tamper(tampered)
		// Seal each command again, so only the shape rule can refuse it.
		for _, step := range tampered.Steps {
			hash, err := operation.CommandHash(step.Command)
			if err != nil {
				t.Fatal(err)
			}
			step.Command.InputHash = hash[:]
		}
		if err := operation.ValidatePlan(tampered, op); err == nil {
			t.Errorf("%s: plan accepted", name)
		}
	}
}

// failRebuild records receipts for the first steps of a rebuild plan, optionally dispatches
// the next step with a failed result, and fails the operation. Failure "unknown" dispatches
// the next step without a result.
func failRebuild(t *testing.T, f *patroniFixture, plan *agentv1.ExecutionPlan, steps int, roles map[string]string, failure string) {
	t.Helper()
	recordPatroniReceiptsWith(t, f, plan.Steps[:steps], roles)
	if failure != "" {
		command := plan.Steps[steps].Command
		if err := f.store.RecordDispatch(t.Context(), f.claim, command); err != nil {
			t.Fatal(err)
		}
	}
	if failure != "" && failure != "unknown" {
		command := plan.Steps[steps].Command
		result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "failed", FailureCode: failure}
		if err := f.store.RecordAgentResult(t.Context(), f.claim.InstallationID, uuid.MustParse(command.Epoch.NodeId), result); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Failed, Reason: "Fixture terminal boundary.", FailureCode: "fixture_failure"}); err != nil {
		t.Fatal(err)
	}
}

// residualMember returns the one residual member of the fixture cluster.
func residualMember(t *testing.T, f *patroniFixture) postgres.Member {
	t.Helper()
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	var found []postgres.Member
	for _, member := range members {
		if member.State == "residual" {
			found = append(found, member)
		}
	}
	if len(found) != 1 || len(members) != 3 {
		t.Fatalf("members: %+v", members)
	}
	return found[0]
}

// completeRebuild records every receipt with one writer and completes the operation.
func completeRebuild(t *testing.T, f *patroniFixture, plan *agentv1.ExecutionPlan) {
	t.Helper()
	roles := map[string]string{plan.Steps[0].Command.StepId: "ready_primary", plan.Steps[1].Command.StepId: "ready_replica", plan.Steps[operation.MemberRebuildPrefix(plan)-1].Command.StepId: "ready_replica"}
	recordPatroniReceiptsWith(t, f, plan.Steps, roles)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(members) != 3 {
		t.Fatalf("members after retry: %+v %v", members, err)
	}
	for _, member := range members {
		if member.State != "active" {
			t.Fatalf("member after retry: %+v", member)
		}
	}
}

// A residual member whose start never ran, or ran to a running member, resumes with its own
// reservation and storage. Initialization is allowed only when no start ran.
func TestPatroniMemberRebuildResumesResidual(t *testing.T) {
	for name, tc := range map[string]struct {
		steps      int
		failure    string
		initialize bool
	}{"start never ran": {6, "", true}, "start refused by an agent guard": {8, "capacity_unavailable", true}, "start reached running": {9, "", false}} {
		t.Run(name, func(t *testing.T) {
			f := createdPatroni(t)
			members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			failed := rebuildPatroniMember(t, f, members[0])
			failRebuild(t, f, failed, tc.steps, map[string]string{failed.Steps[0].Command.StepId: "ready_primary", failed.Steps[1].Command.StepId: "ready_replica"}, tc.failure)
			residual := residualMember(t, f)
			stored := failed.Steps[7].Command.Input.GetEnsureContainer()
			if residual.ResourceID.String() != failed.Steps[7].Command.Input.ResourceId {
				t.Fatal("residual member is not the fresh member")
			}
			// Stop and start would act on incomplete storage.
			for _, api := range []string{postgres.StopClusterAPIOperation, postgres.StartClusterAPIOperation} {
				found, err := f.store.GetCluster(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.store.AdmitClusterAction(t.Context(), postgres.ClusterActionRequest{
					Identity:  postgres.RequestIdentity{InstallationID: f.claim.InstallationID, ActorID: "integration", APIOperation: api, IdempotencyKey: uuid.NewString()},
					RequestID: uuid.NewString(), ClusterID: f.claim.ClusterID, ExpectedRevision: found.DesiredRevision,
				})
				if refused, ok := errors.AsType[*postgres.AdmissionError](err); !ok || refused.Code != postgres.OperationConflict {
					t.Fatalf("%s with a residual member: %v", api, err)
				}
			}
			// Delete stays available. Its preview changes nothing.
			if _, err := f.store.PreviewClusterDelete(t.Context(), f.claim.InstallationID, f.claim.ClusterID, "integration"); err != nil {
				t.Fatal("delete preview with a residual member", err)
			}
			// With a residual member, an active member is not a rebuild target.
			if _, err := f.store.PreviewMemberRebuild(t.Context(), f.claim.InstallationID, f.claim.ClusterID, members[1].MemberID, "integration"); err == nil {
				t.Fatal("an active member was previewed beside a residual member")
			}
			plan := rebuildPatroniMember(t, f, residual)
			if operation.MemberRebuildPrefix(plan) != 6 || len(plan.Steps) != 28 {
				t.Fatalf("resume plan has %d steps, want six member steps and 22 route steps", len(plan.Steps))
			}
			start := plan.Steps[4].Command.Input.GetStartPatroniMember()
			if !proto.Equal(plan.Steps[3].Command.Input.GetEnsureContainer(), stored) || start.AllowInitialization != tc.initialize ||
				plan.Steps[2].Command.Input.ResourceId != stored.VolumeId || plan.Steps[3].Command.Input.ResourceId != residual.ResourceID.String() {
				t.Fatalf("resume does not keep the residual member: start=%+v", start)
			}
			var reservations int
			if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM resource_allocations WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL`, f.claim.InstallationID, f.claim.ClusterID).Scan(&reservations); err != nil || reservations != 3 {
				t.Fatalf("resume reserved capacity: %d %v", reservations, err)
			}
			completeRebuild(t, f, plan)
		})
	}
}

// A residual member whose latest start ran and failed is replaced after proof. Storage that
// never completed needs the start-failure proof. Storage that completed once needs the storage proof.
func TestPatroniMemberRebuildReplacesResidual(t *testing.T) {
	for name, tc := range map[string]struct {
		everRunning bool
		failure     string
	}{"first start failed": {false, "configuration_conflict"}, "first start without receipt": {false, "unknown"}, "restart failed": {true, "configuration_conflict"}} {
		everRunning := tc.everRunning
		t.Run(name, func(t *testing.T) {
			f := createdPatroni(t)
			members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			roles := func(plan *agentv1.ExecutionPlan) map[string]string {
				return map[string]string{plan.Steps[0].Command.StepId: "ready_primary", plan.Steps[1].Command.StepId: "ready_replica"}
			}
			failed := rebuildPatroniMember(t, f, members[0])
			if everRunning {
				// The fresh member reached running, and then a resume failed to restart it.
				failRebuild(t, f, failed, 9, roles(failed), "")
				resumed := rebuildPatroniMember(t, f, residualMember(t, f))
				failRebuild(t, f, resumed, 4, roles(resumed), "configuration_conflict")
			} else {
				failRebuild(t, f, failed, 8, roles(failed), tc.failure)
			}
			residual := residualMember(t, f)
			plan := rebuildPatroniMember(t, f, residual)
			if operation.MemberRebuildPrefix(plan) != 9 || len(plan.Steps) != 35 {
				t.Fatalf("replace plan has %d steps, want nine member steps and 26 route steps", len(plan.Steps))
			}
			stop, proof := plan.Steps[2].Command, plan.Steps[3].Command.Input
			if stop.Input.GetStopContainer() == nil || stop.Input.ResourceId != residual.ResourceID.String() ||
				(proof.GetVerifyPatroniMemberFailed() != nil) != everRunning || (proof.GetVerifyPatroniStartFailure() != nil) == everRunning ||
				!slices.Equal(plan.Steps[4].DependsOn, []string{plan.Steps[2].Command.StepId, plan.Steps[3].Command.StepId}) {
				t.Fatalf("replace proof: %+v", proof)
			}
			fresh := plan.Steps[6].Command
			if fresh.Input.ResourceId == residual.ResourceID.String() || fresh.Epoch.NodeId != stop.Epoch.NodeId || !plan.Steps[7].Command.Input.GetStartPatroniMember().AllowInitialization {
				t.Fatal("replace does not create a fresh member on the same node")
			}
			completeRebuild(t, f, plan)
			var removed bool
			if err := f.pool.QueryRow(t.Context(), `SELECT removed_at IS NOT NULL FROM cluster_members WHERE installation_id=$1 AND resource_id=$2`, f.claim.InstallationID, residual.ResourceID).Scan(&removed); err != nil || !removed {
				t.Fatalf("replaced residual member removed=%v %v", removed, err)
			}
		})
	}
}

// A failed resume leaves the residual member residual.
func TestPatroniMemberResumeFailureKeepsResidual(t *testing.T) {
	f := createdPatroni(t)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	failed := rebuildPatroniMember(t, f, members[0])
	failRebuild(t, f, failed, 6, map[string]string{failed.Steps[0].Command.StepId: "ready_primary", failed.Steps[1].Command.StepId: "ready_replica"}, "")
	residual := residualMember(t, f)
	resumed := rebuildPatroniMember(t, f, residual)
	failRebuild(t, f, resumed, 3, map[string]string{resumed.Steps[0].Command.StepId: "ready_primary", resumed.Steps[1].Command.StepId: "ready_replica"}, "")
	if again := residualMember(t, f); again.MemberID != residual.MemberID {
		t.Fatalf("residual member changed: %+v", again)
	}
}

// Plan validation fixes the residual resume and replace shapes too.
func TestPatroniMemberRetryPlanShapes(t *testing.T) {
	for name, steps := range map[string]int{"resume": 6, "replace": 8} {
		t.Run(name, func(t *testing.T) {
			f := createdPatroni(t)
			members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			failed := rebuildPatroniMember(t, f, members[0])
			failure := ""
			if name == "replace" {
				failure = "configuration_conflict"
			}
			failRebuild(t, f, failed, steps, map[string]string{failed.Steps[0].Command.StepId: "ready_primary", failed.Steps[1].Command.StepId: "ready_replica"}, failure)
			plan := rebuildPatroniMember(t, f, residualMember(t, f))
			op, err := f.store.GetOperation(t.Context(), f.claim.InstallationID, f.claim.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			last := operation.MemberRebuildPrefix(plan) - 1
			tampers := map[string]func(*agentv1.ExecutionPlan){
				"member on a peer node": func(p *agentv1.ExecutionPlan) { p.Steps[last].Command.Epoch.NodeId = p.Steps[0].Command.Epoch.NodeId },
				"member before peers":   func(p *agentv1.ExecutionPlan) { p.Steps[2].DependsOn = p.Steps[2].DependsOn[:1] },
				"primary-only check": func(p *agentv1.ExecutionPlan) {
					p.Steps[last].Command.Input.GetCheckReadiness().PostgresRole = "primary"
				},
				"peer check on a target": func(p *agentv1.ExecutionPlan) {
					p.Steps[0].Command.Input.ResourceId = p.Steps[last].Command.Input.ResourceId
				},
			}
			if name == "replace" {
				tampers["removal before proof"] = func(p *agentv1.ExecutionPlan) { p.Steps[4].DependsOn = p.Steps[4].DependsOn[:1] }
				tampers["proof replaced by a start"] = func(p *agentv1.ExecutionPlan) {
					p.Steps[3].Command.Input.Payload = &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: p.Steps[3].Command.Input.GetVerifyPatroniStartFailure().Container}}
					p.Steps[3].Postcondition = agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING
				}
				tampers["fresh start without initialization"] = func(p *agentv1.ExecutionPlan) {
					p.Steps[7].Command.Input.GetStartPatroniMember().AllowInitialization = false
				}
			}
			for tamperName, tamper := range tampers {
				tampered := proto.Clone(plan).(*agentv1.ExecutionPlan)
				tamper(tampered)
				for _, step := range tampered.Steps {
					hash, err := operation.CommandHash(step.Command)
					if err != nil {
						t.Fatal(err)
					}
					step.Command.InputHash = hash[:]
				}
				if err := operation.ValidatePlan(tampered, op); err == nil {
					t.Errorf("%s: plan accepted", tamperName)
				}
			}
		})
	}
}

// Plan validation fixes the route tail of a routed rebuild: one chain of route-only steps.
func TestPatroniRebuildRouteTailShape(t *testing.T) {
	f := createdPatroni(t)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	plan := rebuildPatroniMember(t, f, members[0])
	op, err := f.store.GetOperation(t.Context(), f.claim.InstallationID, f.claim.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := operation.ValidatePlan(plan, op); err != nil {
		t.Fatal(err)
	}
	prefix := operation.MemberRebuildPrefix(plan)
	var ensure int
	for index := prefix; index < len(plan.Steps); index++ {
		if plan.Steps[index].Command.Input.GetEnsureContainer().GetRoute() != nil {
			ensure = index
			break
		}
	}
	for name, tamper := range map[string]func(*agentv1.ExecutionPlan){
		"route step on a member": func(p *agentv1.ExecutionPlan) {
			p.Steps[prefix].Command.Input.ResourceId = p.Steps[2].Command.Input.ResourceId
		},
		"broken chain": func(p *agentv1.ExecutionPlan) { p.Steps[prefix+1].DependsOn = nil },
		"member container in tail": func(p *agentv1.ExecutionPlan) {
			p.Steps[ensure].Command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: p.Steps[7].Command.Input.GetEnsureContainer()}
			p.Steps[ensure].Command.Input.Secrets = p.Steps[7].Command.Input.Secrets
		},
		"role-any route check": func(p *agentv1.ExecutionPlan) {
			p.Steps[len(p.Steps)-1].Command.Input.GetCheckReadiness().PostgresRole = "any"
		},
	} {
		tampered := proto.Clone(plan).(*agentv1.ExecutionPlan)
		tamper(tampered)
		for _, step := range tampered.Steps {
			hash, err := operation.CommandHash(step.Command)
			if err != nil {
				t.Fatal(err)
			}
			step.Command.InputHash = hash[:]
		}
		if err := operation.ValidatePlan(tampered, op); err == nil {
			t.Errorf("%s: plan accepted", name)
		}
	}
}
