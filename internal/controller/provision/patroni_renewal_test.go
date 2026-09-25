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

// renewPatroni admits and plans a certificate renewal of the fixture cluster.
func renewPatroni(t *testing.T, f *patroniFixture) *agentv1.ExecutionPlan {
	t.Helper()
	claim := f.fixture.act(t, postgres.RenewCertificatesAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	f.fixture.claim = claim
	return installedPlanFor(t, f.fixture, claim.OperationID)
}

// memberContainers reads each live member's current container configuration by resource.
func memberContainers(t *testing.T, f *patroniFixture) map[string]*agentv1.EnsureContainer {
	t.Helper()
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]*agentv1.EnsureContainer{}
	for _, member := range members {
		container, err := f.store.ReadMemberContainer(t.Context(), f.claim.InstallationID, f.claim.ClusterID, member.ResourceID)
		if err != nil {
			t.Fatal(err)
		}
		found[member.ResourceID.String()] = container
	}
	return found
}

// versions returns the certificate credential versions of one configuration.
func versions(container *agentv1.EnsureContainer) (member, dcs int64) {
	for _, reference := range container.Postgres.Patroni.Credentials {
		switch reference.Purpose {
		case postgres.PatroniMemberTLSCredential:
			member = reference.Version
		case postgres.PatroniDCSTLSCredential:
			dcs = reference.Version
		}
	}
	return member, dcs
}

// finalRoles marks the three final checks with one writer unless writers says otherwise.
func finalRoles(plan *agentv1.ExecutionPlan, writers int) map[string]string {
	roles := map[string]string{}
	prefix := operation.RenewalPrefix(plan)
	for index, step := range plan.Steps[prefix-3 : prefix] {
		roles[step.Command.StepId] = "ready_replica"
		if index < writers {
			roles[step.Command.StepId] = "ready_primary"
		}
	}
	return roles
}

// A running cluster renews one member at a time. Each member is recreated with new versions of
// its member certificate and the DCS certificate and restarts without initialization.
// Completion switches each member to its renewed configuration and needs one final writer.
func TestPatroniRenewalRollsMembers(t *testing.T) {
	f := createdPatroni(t)
	before := memberContainers(t, f)
	expiries, err := f.store.ListCertificateExpiries(t.Context(), f.claim.InstallationID)
	if err != nil || len(expiries) != 1 || expiries[0].ClusterID != f.claim.ClusterID || time.Until(expiries[0].ExpiresAt) < 29*24*time.Hour {
		t.Fatalf("expiries after create: %+v %v", expiries, err)
	}
	// The earliest certificate that a member uses sets the cluster expiry.
	if _, err := f.pool.Exec(t.Context(), `UPDATE cluster_credentials SET expires_at=clock_timestamp()+interval '5 days' WHERE installation_id=$1 AND cluster_id=$2 AND purpose='patroni_dcs_tls'`, f.claim.InstallationID, f.claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	if expiries, err := f.store.ListCertificateExpiries(t.Context(), f.claim.InstallationID); err != nil || len(expiries) != 1 || time.Until(expiries[0].ExpiresAt) > 6*24*time.Hour {
		t.Fatalf("expiries with a close DCS certificate: %+v %v", expiries, err)
	}
	plan := renewPatroni(t, f)
	// Member steps, then for each member a pooler and router replace (four steps each) and two route checks.
	if operation.RenewalPrefix(plan) != 21 || len(plan.Steps) != 51 {
		t.Fatalf("running renewal has %d steps, want 21 member steps and 30 route steps", len(plan.Steps))
	}
	renewed := map[string]bool{}
	for block := range 3 {
		base := 6 * block
		stop, recreate, start := plan.Steps[base+2].Command, plan.Steps[base+3].Command.Input.GetRecreatePatroniMember(), plan.Steps[base+4].Command.Input.GetStartPatroniMember()
		member := stop.Input.ResourceId
		if stop.Input.GetStopContainer() == nil || recreate == nil || start == nil || start.AllowInitialization || renewed[member] {
			t.Fatalf("block %d does not stop, recreate, and restart one member", block)
		}
		renewed[member] = true
		if !proto.Equal(recreate.Previous, before[member]) || !proto.Equal(start.Container, recreate.Next) {
			t.Fatalf("block %d does not renew the stored configuration", block)
		}
		for _, peer := range plan.Steps[base : base+2] {
			if peer.Command.Input.ResourceId == member || peer.Command.Input.GetCheckReadiness().GetPostgresRole() != "any" {
				t.Fatalf("block %d checks the wrong peers", block)
			}
		}
		if memberVersion, dcsVersion := versions(recreate.Next); memberVersion != 2 || dcsVersion != 2 {
			t.Fatalf("block %d renews to member version %d and DCS version %d", block, memberVersion, dcsVersion)
		}
	}
	// A repeated plan keeps the installed versions.
	if err := f.planner.Plan(t.Context(), f.claim); err != nil || !proto.Equal(installedPlanFor(t, f.fixture, f.claim.OperationID), plan) {
		t.Fatal("a repeated plan changed the renewal", err)
	}
	var renewedCredentials int
	if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$2 AND version=2 AND expires_at>clock_timestamp()`, f.claim.InstallationID, f.claim.ClusterID).Scan(&renewedCredentials); err != nil || renewedCredentials != 4 {
		t.Fatalf("renewed credentials=%d %v", renewedCredentials, err)
	}
	roles := finalRoles(plan, 1)
	for _, step := range plan.Steps[:18] {
		if step.Command.Input.GetCheckReadiness() != nil {
			roles[step.Command.StepId] = "ready_replica"
		}
	}
	recordPatroniReceiptsWith(t, f, plan.Steps, roles)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	if expiries, err := f.store.ListCertificateExpiries(t.Context(), f.claim.InstallationID); err != nil || len(expiries) != 1 || time.Until(expiries[0].ExpiresAt) < 29*24*time.Hour {
		t.Fatalf("expiries after renewal: %+v %v", expiries, err)
	}
	after := memberContainers(t, f)
	for member, container := range after {
		if memberVersion, dcsVersion := versions(container); memberVersion != 2 || dcsVersion != 2 {
			t.Fatalf("member %s runs member version %d and DCS version %d after renewal", member, memberVersion, dcsVersion)
		}
	}
	// A later start repeats the renewed configuration.
	claim := f.fixture.act(t, postgres.StartClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	start := installedPlanFor(t, f.fixture, claim.OperationID)
	for _, step := range start.Steps[:3] {
		if !proto.Equal(step.Command.Input.GetStartPatroniMember().GetContainer(), after[step.Command.Input.ResourceId]) {
			t.Fatal("a start after renewal does not use the renewed configuration")
		}
	}
}

// A renewal completes only with exactly one writer among the final checks.
func TestPatroniRenewalNeedsOneWriter(t *testing.T) {
	f := createdPatroni(t)
	plan := renewPatroni(t, f)
	recordPatroniReceiptsWith(t, f, plan.Steps, finalRoles(plan, 2))
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); !errors.Is(err, operation.ErrPostconditionUnproved) {
		t.Fatalf("renewal with two final writers: %v", err)
	}
}

// A failed renewal switches only members whose recreate completed. The next renewal uses a
// version after every stored one, including versions that no member received, and each
// member renews from the configuration it runs.
func TestPatroniRenewalFailureKeepsProvedMembers(t *testing.T) {
	f := createdPatroni(t)
	before := memberContainers(t, f)
	first := renewPatroni(t, f)
	recordPatroniReceiptsWith(t, f, first.Steps[:4], nil)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Failed, Reason: "Fixture terminal boundary.", FailureCode: "fixture_failure"}); err != nil {
		t.Fatal(err)
	}
	// A renewal that just failed is not admitted again at the next sweep.
	if expiries, err := f.store.ListCertificateExpiries(t.Context(), f.claim.InstallationID); err != nil || len(expiries) != 0 {
		t.Fatalf("a failed renewal is a candidate again at once: %+v %v", expiries, err)
	}
	renewedMember := first.Steps[3].Command.Input.ResourceId
	after := memberContainers(t, f)
	for member, container := range after {
		want := before[member]
		if member == renewedMember {
			want = first.Steps[3].Command.Input.GetRecreatePatroniMember().Next
		}
		if !proto.Equal(container, want) {
			t.Fatalf("member %s configuration after a failed renewal", member)
		}
	}
	second := renewPatroni(t, f)
	for block := range 3 {
		recreate := second.Steps[6*block+3].Command.Input.GetRecreatePatroniMember()
		member := second.Steps[6*block+3].Command.Input.ResourceId
		memberVersion, dcsVersion := versions(recreate.Next)
		if !proto.Equal(recreate.Previous, after[member]) || memberVersion != 3 || dcsVersion != 3 {
			t.Fatalf("member %s renews to member version %d and DCS version %d", member, memberVersion, dcsVersion)
		}
	}
}

// A stopped cluster recreates its members without starting them.
func TestPatroniRenewalStoppedCluster(t *testing.T) {
	f := createdPatroni(t)
	stop := f.fixture.planFor(t, postgres.StopClusterAPIOperation)
	recordPatroniReceipts(t, f, stop.Steps)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	plan := renewPatroni(t, f)
	// A stopped cluster replaces each pooler and router without starting them.
	if operation.RenewalPrefix(plan) != 3 || len(plan.Steps) != 21 {
		t.Fatalf("stopped renewal has %d steps, want three recreates and 18 route steps", len(plan.Steps))
	}
	// The recreates form one chain, so each node's route steps follow its own recreate.
	for index, step := range plan.Steps[:3] {
		var want []string
		if index > 0 {
			want = []string{plan.Steps[index-1].Command.StepId}
		}
		if step.Command.Input.GetRecreatePatroniMember() == nil || !slices.Equal(step.DependsOn, want) {
			t.Fatal("stopped renewal does more than recreate members in order")
		}
	}
	recordPatroniReceipts(t, f, plan.Steps)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	found, err := f.store.GetCluster(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || found.ObservedLifecycle != cluster.ObservedStopped {
		t.Fatalf("stopped cluster after renewal: %+v %v", found, err)
	}
}

// Renewal is a Patroni action on three active members.
func TestPatroniRenewalAdmission(t *testing.T) {
	standalone := newFixture(t, postgresSpec())
	found, err := standalone.store.GetCluster(t.Context(), standalone.claim.InstallationID, standalone.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	request := postgres.ClusterActionRequest{
		Identity:  postgres.RequestIdentity{InstallationID: standalone.claim.InstallationID, ActorID: "integration", APIOperation: postgres.RenewCertificatesAPIOperation, IdempotencyKey: uuid.NewString()},
		RequestID: uuid.NewString(), ClusterID: standalone.claim.ClusterID, ExpectedRevision: found.DesiredRevision,
	}
	if _, err := standalone.store.AdmitClusterAction(t.Context(), request); err == nil {
		t.Fatal("a standalone cluster admitted a Patroni renewal")
	}
	f := createdPatroni(t)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(t.Context(), `UPDATE cluster_members SET state='residual' WHERE installation_id=$1 AND member_id=$2`, f.claim.InstallationID, members[0].MemberID); err != nil {
		t.Fatal(err)
	}
	found, err = f.store.GetCluster(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	request.Identity.InstallationID, request.ClusterID, request.ExpectedRevision = f.claim.InstallationID, f.claim.ClusterID, found.DesiredRevision
	_, err = f.store.AdmitClusterAction(t.Context(), request)
	if refused, ok := errors.AsType[*postgres.AdmissionError](err); !ok || refused.Code != postgres.OperationConflict {
		t.Fatalf("renewal with a residual member: %v", err)
	}
	if expiries, err := f.store.ListCertificateExpiries(t.Context(), f.claim.InstallationID); err != nil || len(expiries) != 0 {
		t.Fatalf("a cluster with a residual member is a renewal candidate: %+v %v", expiries, err)
	}
}

// Plan validation fixes the renewal order.
func TestPatroniRenewalPlanShape(t *testing.T) {
	f := createdPatroni(t)
	plan := renewPatroni(t, f)
	op, err := f.store.GetOperation(t.Context(), f.claim.InstallationID, f.claim.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	for name, tamper := range map[string]func(*agentv1.ExecutionPlan){
		"stop before peers":  func(p *agentv1.ExecutionPlan) { p.Steps[8].DependsOn = p.Steps[8].DependsOn[:1] },
		"blocks in parallel": func(p *agentv1.ExecutionPlan) { p.Steps[6].DependsOn, p.Steps[7].DependsOn = nil, nil },
		"start with initialization": func(p *agentv1.ExecutionPlan) {
			p.Steps[4].Command.Input.GetStartPatroniMember().AllowInitialization = true
		},
		"peer check on the member": func(p *agentv1.ExecutionPlan) {
			p.Steps[0].Command.Input.ResourceId = p.Steps[2].Command.Input.ResourceId
		},
		"member renewed twice":       func(p *agentv1.ExecutionPlan) { p.Steps = slices.Delete(p.Steps, 18, 21) },
		"final check without writer": func(p *agentv1.ExecutionPlan) { p.Steps[20].Command.Input.GetCheckReadiness().PostgresRole = "replica" },
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
