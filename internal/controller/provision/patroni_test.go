package provision

import (
	"crypto/sha256"
	"errors"
	"net/netip"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/coordinator"
	enginepostgres "github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

const patroniImage = "synod-patroni@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// hostDomain gives one node its client address and host failure domain.
// onHost declares a node's host domain, address, and architecture.
func onHost(domain, address, architecture string) func(*placement.NodePolicy) {
	return func(policy *placement.NodePolicy) {
		hostDomain(domain, address)(policy)
		policy.Architecture = architecture
	}
}

func hostDomain(domain, address string) func(*placement.NodePolicy) {
	return func(policy *placement.NodePolicy) {
		policy.ClientAddress = address
		if domain != "" {
			policy.FailureDomains = map[string]string{"host": domain}
		}
	}
}

type patroniFixture struct {
	*fixture
	planner *Planner
	etcd    *clientv3.Client
	ca      *pki.CA
	nodes   map[uuid.UUID]string
}

// newPatroniFixture admits one Patroni cluster on three approved amd64 nodes with the given domains.
func newPatroniFixture(t *testing.T, domains [3][2]string) *patroniFixture {
	t.Helper()
	return newPatroniFixtureOn(t, domains, [3]string{nodeArchitecture, nodeArchitecture, nodeArchitecture})
}

// newPatroniFixtureOn declares each node's architecture as well.
func newPatroniFixtureOn(t *testing.T, domains [3][2]string, architectures [3]string) *patroniFixture {
	t.Helper()
	endpoint := os.Getenv("SYNOD_TEST_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set SYNOD_TEST_ETCD_ENDPOINT through scripts/test-etcd.sh")
	}
	spec := cluster.CreateSpec{SchemaVersion: 1, Name: "ha", Engine: cluster.PostgreSQL,
		PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "patroni", FailureDomain: "host"}}
	f := newFixtureWith(t, spec, onHost(domains[0][0], domains[0][1], architectures[0]))
	nodes := map[uuid.UUID]string{f.node: domains[0][1]}
	for index, domain := range domains[1:] {
		node := uuid.New()
		if _, err := f.pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at)
 VALUES ($1,$2,$3,clock_timestamp())`, node, f.claim.InstallationID, make([]byte, 32)); err != nil {
			t.Fatal(err)
		}
		readyNodeWith(t, f.store, f.claim.InstallationID, node, onHost(domain[0], domain[1], architectures[index+1]))
		nodes[node] = domain[1]
	}
	etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{"http://" + endpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { etcd.Close() })
	accounts, err := coordinator.NewPatroniAccounts(etcd, f.claim.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	nativeCert, nativeKey, err := pki.CreatePatroniCA(f.claim.InstallationID, time.Now(), 365*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadPatroniCA(f.claim.InstallationID, nativeCert, nativeKey)
	if err != nil {
		t.Fatal(err)
	}
	etcdTrust, _, err := pki.CreateCA(uuid.New(), time.Now(), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	config := f.planner.config
	config.Patroni = &PatroniConfig{Accounts: accounts, CA: ca, EtcdTrust: etcdTrust, DCSEndpoints: []string{"192.0.2.20:2379"}, Image: patroniImage}
	planner, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return &patroniFixture{fixture: f, planner: planner, etcd: etcd, ca: ca, nodes: nodes}
}

func TestPatroniProvisionPlansThreeMembersAndCompletes(t *testing.T) {
	f := newPatroniFixture(t, [3][2]string{{"host-a", "192.0.2.10"}, {"host-b", "192.0.2.11"}, {"host-c", "192.0.2.12"}})
	if err := f.planner.Plan(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	plan := installedPlan(t, f.fixture)
	if len(plan.Steps) != 30 {
		t.Fatalf("plan has %d steps, want four per member, four route steps per member, and two route checks per member", len(plan.Steps))
	}
	// After every member is ready, each node gets a pooler and a router, then both routes are checked.
	for index := range 3 {
		deploy := plan.Steps[12+4*index : 16+4*index]
		pooler, router := deploy[0].Command.Input.GetEnsureContainer().GetRoute(), deploy[2].Command.Input.GetEnsureContainer().GetRoute()
		if pooler.GetPooler() == nil || router.GetRouter() == nil || len(router.Router.Backends) != 3 ||
			deploy[1].Command.Input.GetStartContainer() == nil || deploy[3].Command.Input.GetStartContainer() == nil {
			t.Fatalf("member %d routes: %+v %+v", index, pooler, router)
		}
		if index == 0 && len(deploy[0].DependsOn) != 3 {
			t.Fatal("routes start before every member is ready")
		}
		for position, role := range []string{"primary", "replica"} {
			check := plan.Steps[24+2*index+position].Command.Input.GetCheckReadiness()
			if check.GetPostgresRole() != role || check.Address != enginepostgres.ReadinessLoopback(netip.MustParseAddr(router.Router.Address)).Addr().String() || check.GetPostgresTls().GetServerName() != router.Router.Address {
				t.Fatalf("member %d %s route check: %+v", index, role, check)
			}
		}
	}
	nodes := map[string]bool{}
	var primaryReady string
	for index := range 3 {
		volume, container, start, ready := plan.Steps[4*index], plan.Steps[4*index+1], plan.Steps[4*index+2], plan.Steps[4*index+3]
		node := container.Command.Epoch.NodeId
		nodes[node] = true
		address := f.nodes[uuid.MustParse(node)]
		configuration := container.Command.Input.GetEnsureContainer()
		native := configuration.GetPostgres().GetPatroni()
		if volume.Command.Input.GetEnsureVolume() == nil || native == nil || native.Address != address || configuration.NetworkMode != agentv1.NetworkMode_NETWORK_MODE_HOST ||
			len(configuration.Ports) != 0 || configuration.ImageDigest != patroniImage || !native.AllowInitialization || len(native.PeerNetworks) != 3 ||
			!slices.Equal(native.ClientNetworks, []string{"0.0.0.0/0", "::/0"}) || native.RequiredFreeBytes != f.planner.config.Budget.DiskBytes {
			t.Fatalf("member %d configuration: %+v", index, configuration)
		}
		memberTLS := 0
		for _, reference := range native.Credentials {
			if reference.Purpose == postgres.PatroniMemberTLSCredential {
				memberTLS++
			}
		}
		sameReferences := slices.EqualFunc(container.Command.Input.Secrets, native.Credentials, func(a, b *agentv1.SecretReference) bool { return proto.Equal(a, b) })
		if len(native.Credentials) != 8 || memberTLS != 1 || !sameReferences {
			t.Fatalf("member %d references %d credentials and %d member identities", index, len(native.Credentials), memberTLS)
		}
		begin := start.Command.Input.GetStartPatroniMember()
		if begin == nil || !begin.AllowInitialization || !proto.Equal(begin.Container, configuration) {
			t.Fatalf("member %d start: %+v", index, begin)
		}
		check := ready.Command.Input.GetCheckReadiness()
		role := "replica"
		if index == 0 {
			role, primaryReady = "primary", ready.Command.StepId
		} else if !slices.Contains(start.DependsOn, primaryReady) {
			t.Fatalf("member %d starts before the first member proves primary", index)
		}
		if check.Address != enginepostgres.ReadinessLoopback(netip.MustParseAddr(address)).Addr().String() || check.PostgresRole != role || check.Database != "app" || check.Role != "synod_app" ||
			check.GetPostgresTls().GetServerName() != address || string(check.GetPostgresTls().GetRootCaPem()) != string(f.ca.CertificatePEM()) || check.Port != configuration.Postgres.Port {
			t.Fatalf("member %d readiness: %+v", index, check)
		}
	}
	if len(nodes) != 3 {
		t.Fatal("members share a node")
	}
	var members, credentials, databasePorts, restPorts int
	if err := f.pool.QueryRow(t.Context(), `SELECT
 (SELECT count(*) FROM cluster_members WHERE installation_id=$1 AND cluster_id=$2 AND role='patroni'),
 (SELECT count(*) FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$2),
 (SELECT count(*) FROM allocation_ports WHERE installation_id=$1 AND cluster_id=$2 AND name='database'),
 (SELECT count(*) FROM allocation_ports WHERE installation_id=$1 AND cluster_id=$2 AND name='rest')`,
		f.claim.InstallationID, f.claim.ClusterID).Scan(&members, &credentials, &databasePorts, &restPorts); err != nil {
		t.Fatal(err)
	}
	if members != 3 || credentials != 10 || databasePorts != 3 || restPorts != 3 {
		t.Fatalf("members=%d credentials=%d database ports=%d rest ports=%d", members, credentials, databasePorts, restPorts)
	}
	name, scope := f.planner.config.Patroni.Accounts.PatroniScope(f.claim.ClusterID)
	role, err := f.etcd.Auth.RoleGet(t.Context(), name)
	if err != nil || len(role.Perm) != 1 || string(role.Perm[0].Key) != scope {
		t.Fatalf("DCS account role: %+v %v", role, err)
	}

	// A repeated plan after installation must not touch etcd, or it would change the
	// password the installed plan delivers.
	if _, err := f.etcd.Auth.UserDelete(t.Context(), name); err != nil {
		t.Fatal(err)
	}
	if err := f.planner.Plan(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	if _, err := f.etcd.Auth.UserGet(t.Context(), name); !errors.Is(err, rpctypes.ErrUserNotFound) {
		t.Fatal("a repeated plan reached etcd", err)
	}
	if !proto.Equal(installedPlan(t, f.fixture), plan) {
		t.Fatal("a repeated plan changed the installed plan")
	}

	// Every member must prove readiness before the create completes.
	recordPatroniReceipts(t, f, plan.Steps)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	found, err := f.store.GetCluster(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || found.ObservedLifecycle != cluster.ObservedRunning {
		t.Fatalf("created cluster: %+v %v", found, err)
	}
	live, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(live) != 3 {
		t.Fatalf("members: %+v %v", live, err)
	}
	for _, member := range live {
		if member.Role != "patroni" || member.State != "active" {
			t.Fatalf("member after completion: %+v", member)
		}
	}
	endpoints, err := f.store.InspectClusterEndpoints(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(endpoints.Endpoints) != 6 || endpoints.WriterURL == "" || endpoints.ReaderURL == "" {
		t.Fatalf("endpoints after completion: %+v %v", endpoints, err)
	}
}

// A refused placement leaves no credential, member, or etcd account behind.
func TestPatroniProvisionRefusesWeakPlacement(t *testing.T) {
	for name, domains := range map[string][3][2]string{
		"shared host domain":     {{"host-a", "192.0.2.10"}, {"host-a", "192.0.2.11"}, {"host-c", "192.0.2.12"}},
		"missing host domain":    {{"host-a", "192.0.2.10"}, {"", "192.0.2.11"}, {"host-c", "192.0.2.12"}},
		"missing client address": {{"host-a", "192.0.2.10"}, {"host-b", ""}, {"host-c", "192.0.2.12"}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newPatroniFixture(t, domains)
			if err := f.planner.Plan(t.Context(), f.claim); !errors.Is(err, placement.ErrUnsatisfied) {
				t.Fatalf("weak placement planned: %v", err)
			}
			var planned, credentials int
			if err := f.pool.QueryRow(t.Context(), `SELECT
 (SELECT plan_version FROM operations WHERE installation_id=$1 AND id=$2),
 (SELECT count(*) FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$3)`,
				f.claim.InstallationID, f.claim.OperationID, f.claim.ClusterID).Scan(&planned, &credentials); err != nil {
				t.Fatal(err)
			}
			name, _ := f.planner.config.Patroni.Accounts.PatroniScope(f.claim.ClusterID)
			if _, err := f.etcd.Auth.RoleGet(t.Context(), name); planned != 0 || credentials != 0 || !errors.Is(err, rpctypes.ErrRoleNotFound) {
				t.Fatalf("refused placement left plan=%d credentials=%d role error=%v", planned, credentials, err)
			}
		})
	}
}

// recordPatroniReceipts reports drained barriers for every node, then records a
// successful receipt and completion for each given step in order.
func recordPatroniReceipts(t *testing.T, f *patroniFixture, steps []*agentv1.PlanStep) {
	t.Helper()
	recordPatroniReceiptsWith(t, f, steps, nil)
}

// recordPatroniReceiptsWith lets roles name the state each role-any readiness step reports.
func recordPatroniReceiptsWith(t *testing.T, f *patroniFixture, steps []*agentv1.PlanStep, roles map[string]string) {
	t.Helper()
	barriers := map[string]bool{}
	for _, step := range steps {
		epoch := step.Command.Epoch
		if barriers[epoch.NodeId] {
			continue
		}
		barriers[epoch.NodeId] = true
		report := &agentv1.BarrierResult{Epoch: epoch, AdmissionsClosed: true, Drained: true}
		if err := f.store.RecordBarrier(t.Context(), f.claim, report); err != nil {
			t.Fatal(err)
		}
		encoded, err := proto.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.store.ReconcileBarrier(t.Context(), f.claim, uuid.MustParse(epoch.NodeId), sha256.Sum256(encoded), "No old native work remains."); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.store.ActivateClaim(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		command := step.Command
		if err := f.store.RecordDispatch(t.Context(), f.claim, command); err != nil {
			t.Fatal(err)
		}
		state, prefix := "present", "docker-command:"
		switch step.Postcondition {
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS:
			state = "created"
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING:
			state = "running"
		case agentv1.Postcondition_POSTCONDITION_ENGINE_READY:
			state, prefix = "ready", "readiness:"
			if step.Command.Input.GetCheckReadiness().GetPostgresRole() == "any" {
				state = "ready_replica"
			}
			if role, named := roles[step.Command.StepId]; named {
				state = role
			}
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED:
			state = "exited"
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT:
			state = "absent"
		case agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_NOT_SERVING:
			state = "patroni_member_not_serving"
		case agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_FAILED:
			state = "patroni_member_storage_failed"
		case agentv1.Postcondition_POSTCONDITION_PATRONI_START_FAILED:
			state = "patroni_storage_attempt_failed"
		case agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_RECREATED:
			state = "patroni_member_recreated"
		case agentv1.Postcondition_POSTCONDITION_PATRONI_LEADER:
			state, prefix = "patroni_leader", "patroni-switchover:"
		}
		result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded",
			Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: prefix + command.CommandId, State: state}}}
		if err := f.store.RecordAgentResult(t.Context(), f.claim.InstallationID, uuid.MustParse(command.Epoch.NodeId), result); err != nil {
			t.Fatal(err)
		}
		encoded, err := proto.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.store.RecordStepCompletion(t.Context(), f.claim, uuid.MustParse(command.StepId), result.Epoch, sha256.Sum256(encoded)); err != nil {
			t.Fatal(err)
		}
	}
}

// A create whose last member never proved readiness cannot complete.
func TestPatroniProvisionNeedsEveryMemberReady(t *testing.T) {
	f := newPatroniFixture(t, [3][2]string{{"host-a", "192.0.2.10"}, {"host-b", "192.0.2.11"}, {"host-c", "192.0.2.12"}})
	if err := f.planner.Plan(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	plan := installedPlan(t, f.fixture)
	recordPatroniReceipts(t, f, plan.Steps[:len(plan.Steps)-1])
	// Completion needs a recorded completion for every step, so the missing readiness
	// proof stops it before the per-member readiness rule.
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("create completed without the last member's readiness proof: %v", err)
	}
	found, err := f.store.GetCluster(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || found.ObservedLifecycle == cluster.ObservedRunning {
		t.Fatalf("cluster after refused completion: %+v %v", found, err)
	}
}

// A controller without Patroni configuration never plans a Patroni cluster, even if
// one was admitted before the configuration was removed.
func TestPatroniProvisionRequiresConfiguration(t *testing.T) {
	spec := cluster.CreateSpec{SchemaVersion: 1, Name: "ha", Engine: cluster.PostgreSQL,
		PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "patroni", FailureDomain: "host"}}
	f := newFixture(t, spec)
	if err := f.planner.Plan(t.Context(), f.claim); err == nil {
		t.Fatal("an unconfigured controller planned a Patroni cluster")
	}
	var planned, credentials int
	if err := f.pool.QueryRow(t.Context(), `SELECT
 (SELECT plan_version FROM operations WHERE installation_id=$1 AND id=$2),
 (SELECT count(*) FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$3)`,
		f.claim.InstallationID, f.claim.OperationID, f.claim.ClusterID).Scan(&planned, &credentials); err != nil || planned != 0 || credentials != 0 {
		t.Fatalf("plan=%d credentials=%d err=%v", planned, credentials, err)
	}
}

// createdPatroni plans a Patroni create and completes it with receipts from every member.
func createdPatroni(t *testing.T) *patroniFixture {
	t.Helper()
	f := newPatroniFixture(t, [3][2]string{{"host-a", "192.0.2.10"}, {"host-b", "192.0.2.11"}, {"host-c", "192.0.2.12"}})
	if err := f.planner.Plan(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	recordPatroniReceipts(t, f, installedPlan(t, f.fixture).Steps)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	return f
}

// Patroni members stop and leave together. Standalone replica and recovery actions are
// refused. A completed delete requests DCS cleanup once.
func TestPatroniLifecycleStopAndDelete(t *testing.T) {
	f := createdPatroni(t)
	found, err := f.store.GetCluster(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	for _, api := range []string{postgres.ScaleClusterAPIOperation} {
		request := postgres.ClusterActionRequest{
			Identity:  postgres.RequestIdentity{InstallationID: f.claim.InstallationID, ActorID: "integration", APIOperation: api, IdempotencyKey: uuid.NewString()},
			RequestID: uuid.NewString(), ClusterID: f.claim.ClusterID, ExpectedRevision: found.DesiredRevision,
		}
		if api == postgres.ScaleClusterAPIOperation {
			request.Replicas = new(1)
		}
		_, err := f.store.AdmitClusterAction(t.Context(), request)
		if refused, ok := errors.AsType[*postgres.AdmissionError](err); !ok || refused.Code != postgres.UnsupportedSpec {
			t.Fatalf("%s on a Patroni cluster: %v", api, err)
		}
	}
	for name, preview := range map[string]func() error{
		"setup recovery": func() error {
			_, err := f.store.PreviewSetupRecovery(t.Context(), f.claim.InstallationID, f.claim.ClusterID, "integration")
			return err
		},
		"replica rebuild": func() error {
			_, err := f.store.PreviewReplicaRebuild(t.Context(), f.claim.InstallationID, f.claim.ClusterID, "integration")
			return err
		},
	} {
		if err := preview(); err == nil {
			t.Fatalf("%s preview accepted a Patroni cluster", name)
		}
	}
	stop := f.fixture.planFor(t, postgres.StopClusterAPIOperation)
	if len(stop.Steps) != 9 {
		t.Fatalf("stop plan has %d steps, want one per member and two routes per member", len(stop.Steps))
	}
	// Members stop together. Each node's router and pooler stop after that node's previous step,
	// because an agent runs one command of a cluster at a time.
	last := map[string]string{}
	for index, step := range stop.Steps {
		node := step.Command.Epoch.NodeId
		var want []string
		if previous, seen := last[node]; seen {
			want = []string{previous}
		}
		if step.Command.Input.GetStopContainer() == nil || !slices.Equal(step.DependsOn, want) || (index < 3) != (want == nil) {
			t.Fatalf("stop step %d depends on %v, want %v", index, step.DependsOn, want)
		}
		last[node] = step.Command.StepId
	}
	remove := f.fixture.planFor(t, postgres.DeleteClusterAPIOperation)
	if len(remove.Steps) != 18 {
		t.Fatalf("delete plan has %d steps, want stop and remove per member and per route", len(remove.Steps))
	}
	// Member stops start together. Every later step on a node follows that node's previous
	// step, and a removal follows exactly its own stop.
	stops := map[string]bool{}
	clear(last)
	for _, step := range remove.Steps {
		node := step.Command.Epoch.NodeId
		previous, seen := last[node]
		switch {
		case step.Command.Input.GetRemoveContainer() != nil:
			if len(step.DependsOn) != 1 || !stops[step.DependsOn[0]] {
				t.Fatalf("removal does not depend on exactly its own stop: %v", step.DependsOn)
			}
		case !seen && len(step.DependsOn) != 0:
			t.Fatal("Patroni delete orders member stops")
		case seen && !slices.Equal(step.DependsOn, []string{previous}):
			t.Fatalf("delete step on node %s depends on %v, want %s", node, step.DependsOn, previous)
		}
		if step.Command.Input.GetStopContainer() != nil {
			stops[step.Command.StepId] = true
		}
		last[node] = step.Command.StepId
	}
	recordPatroniReceipts(t, f, remove.Steps)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	pending, err := f.store.ListPendingDCSCleanups(t.Context(), f.claim.InstallationID)
	if err != nil || !slices.Equal(pending, []uuid.UUID{f.claim.ClusterID}) {
		t.Fatalf("pending DCS cleanups=%v err=%v", pending, err)
	}
	accounts := f.planner.config.Patroni.Accounts
	for range 2 {
		if err := accounts.DeleteScope(t.Context(), f.claim.ClusterID); err != nil {
			t.Fatal(err)
		}
		if err := f.store.CompleteDCSCleanup(t.Context(), f.claim.InstallationID, f.claim.ClusterID); err != nil {
			t.Fatal(err)
		}
	}
	if pending, err := f.store.ListPendingDCSCleanups(t.Context(), f.claim.InstallationID); err != nil || len(pending) != 0 {
		t.Fatalf("completed cleanup still pending: %v %v", pending, err)
	}
	name, _ := accounts.PatroniScope(f.claim.ClusterID)
	if _, err := f.etcd.Auth.UserGet(t.Context(), name); !errors.Is(err, rpctypes.ErrUserNotFound) {
		t.Fatal("deleted Patroni cluster keeps its DCS account", err)
	}
}

// A Patroni start starts every member at once with its stored configuration, and it
// completes only when exactly one member proves primary.
func TestPatroniStartProvesOneWriter(t *testing.T) {
	for name, primaries := range map[string]int{"one primary": 1, "no primary": 0, "two primaries": 2} {
		t.Run(name, func(t *testing.T) {
			f := createdPatroni(t)
			created := installedPlan(t, f.fixture)
			stored := map[string]*agentv1.EnsureContainer{}
			for _, step := range created.Steps {
				if container := step.Command.Input.GetEnsureContainer(); container != nil {
					stored[step.Command.Input.ResourceId] = container
				}
			}
			claim := f.fixture.act(t, postgres.StartClusterAPIOperation)
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			f.fixture.claim = claim
			plan := installedPlanFor(t, f.fixture, claim.OperationID)
			if len(plan.Steps) != 18 {
				t.Fatalf("start plan has %d steps, want member starts and checks, route starts, and route checks", len(plan.Steps))
			}
			var starts []string
			for _, step := range plan.Steps[:3] {
				begin := step.Command.Input.GetStartPatroniMember()
				if begin == nil || begin.AllowInitialization || len(step.DependsOn) != 0 || !proto.Equal(begin.Container, stored[step.Command.Input.ResourceId]) {
					t.Fatalf("start step: %+v depends on %v", begin, step.DependsOn)
				}
				starts = append(starts, step.Command.StepId)
			}
			roles := map[string]string{}
			for index, step := range plan.Steps[3:6] {
				check := step.Command.Input.GetCheckReadiness()
				address := stored[step.Command.Input.ResourceId].Postgres.Patroni.Address
				if check.GetPostgresRole() != "any" || check.Address != enginepostgres.ReadinessLoopback(netip.MustParseAddr(address)).Addr().String() || check.GetPostgresTls().GetServerName() != address || !slices.Equal(step.DependsOn, starts) {
					t.Fatalf("readiness step: %+v depends on %v", check, step.DependsOn)
				}
				roles[step.Command.StepId] = "ready_replica"
				if index < primaries {
					roles[step.Command.StepId] = "ready_primary"
				}
			}
			recordPatroniReceiptsWith(t, f, plan.Steps, roles)
			err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."})
			if primaries == 1 && err != nil {
				t.Fatal("start with one writer refused", err)
			}
			if primaries != 1 && !errors.Is(err, operation.ErrPostconditionUnproved) {
				t.Fatalf("start with %d writers: %v", primaries, err)
			}
		})
	}
}
