package provision

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	imageinspect "github.com/sirrobot01/synod/internal/runtime/image"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// databaseFixture admits a tier 1 app with one standby on a second, arm64 device.
func databaseFixture(t *testing.T, database app.DatabaseSpec) (*fixture, uuid.UUID, postgres.CreateAppRequest) {
	t.Helper()
	f := newFixture(t)
	standby := uuid.New()
	if _, err := f.pool.Exec(t.Context(), `INSERT INTO nodes(id,installation_id,public_key_hash,enrolled_at) VALUES($1,$2,$3,clock_timestamp())`, standby, f.claim.InstallationID, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	readyNodeWith(t, f.store, f.claim.InstallationID, standby, func(p *placement.NodePolicy) {
		p.Architecture = "arm64"
		p.Engines = nil
		p.ClientAddress = "192.0.2.11"
	})
	f.planner.config.DatabaseImages = []DatabaseImage{
		{Engine: "postgres", Version: "17.11", Digest: "postgres@sha256:d74eeac9a635390a49bc21bd49fccd973de707e2a53a76ac49b552b8712ec46f"},
		{Engine: "redis", Version: "8.2.7", Digest: "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3"},
	}
	f.planner.config.ResolveImage = func(_ context.Context, s app.ServiceSpec) (imageinspect.Resolution, error) {
		s.SupportedArchitectures = []string{"amd64", "arm64"}
		return imageinspect.Resolution{Service: s}, nil
	}
	replication := app.DefaultDatabaseReplication()
	request := postgres.CreateAppRequest{Identity: postgres.RequestIdentity{InstallationID: f.claim.InstallationID, ActorID: "database-test", APIOperation: postgres.CreateAppAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: "database-test",
		Spec: app.AppSpec{SchemaVersion: 1, Name: "photos-db", Tier: app.TierDatabase, Copies: 1, Replication: &replication, Database: &database,
			DevicePreference: app.DevicePreference{Preferred: []string{f.node.String()}}, Router: app.RouterSpec{Protocol: "tcp", Port: 15432, ReaderPort: 15433}}}
	return f, standby, request
}

func TestTier1DeployRunsAPrimaryAndAStandbyOnEachArchitecture(t *testing.T) {
	f, standby, request := databaseFixture(t, app.DatabaseSpec{Engine: app.EnginePostgres, Version: "17", Databases: []string{"immich"}})
	admitted, err := f.store.AdmitApp(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: admitted.OperationID, RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "database-test", TTL: time.Minute, LeaseID: 2, LeaseRevision: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatalf("deploy: %v (%v)", err, errors.Unwrap(err))
	}
	copies, err := f.store.GetAppCopies(t.Context(), claim.InstallationID, claim.AppID)
	if err != nil || len(copies) != 2 {
		t.Fatalf("copies: %+v %v", copies, err)
	}
	roles := map[uuid.UUID]string{}
	for _, copy := range copies {
		roles[copy.NodeID] = copy.Role
	}
	if roles[f.node] != "active" || roles[standby] != "standby" {
		t.Fatalf("roles: %v", roles)
	}
	writer, err := f.store.GetAppWriter(t.Context(), claim.InstallationID, claim.AppID)
	if err != nil || writer.Epoch != 1 || writer.NodeID != f.node {
		t.Fatalf("writer: %+v %v", writer, err)
	}
	plan, err := f.store.ReadPlan(t.Context(), claim)
	if err != nil || len(plan.Steps) != 4 {
		t.Fatalf("a standby needs its own engine container: %v %v", plan, err)
	}
	credentials := map[string]uuid.UUID{}
	for _, purpose := range []string{postgres.SuperuserCredential, postgres.ReplicationCredential, postgres.ApplicationCredential} {
		credential, err := f.store.ReadAppCredential(t.Context(), claim.InstallationID, claim.AppID, purpose)
		if err != nil || credential == nil {
			t.Fatalf("%s credential: %v", purpose, err)
		}
		credentials[purpose] = credential.CredentialID
	}
	nodes := map[string]bool{}
	for _, step := range plan.Steps {
		ensure := step.Command.Input.GetEnsureContainer()
		if ensure == nil {
			continue
		}
		nodes[step.Command.Epoch.NodeId] = true
		if ensure.ImageDigest != f.planner.config.DatabaseImages[0].Digest || !slices.ContainsFunc(ensure.Generic.Mounts, func(m *agentv1.Mount) bool { return m.Type == "engine" && m.Source == "data" }) || len(ensure.SecretEnv) != 1 || ensure.SecretEnv[0].Name != "POSTGRES_PASSWORD" {
			t.Fatalf("engine container: %+v", ensure)
		}
		if refs := step.Command.Input.Secrets; len(refs) != 1 || refs[0].CredentialId != credentials[postgres.SuperuserCredential].String() {
			t.Fatalf("secret references: %+v", refs)
		}
	}
	if !nodes[f.node.String()] || !nodes[standby.String()] {
		t.Fatalf("containers on %v", nodes)
	}
	record, err := f.store.ReadPlacement(t.Context(), claim.InstallationID, claim.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	readers := 0
	for _, allocation := range record.Allocations {
		if allocation.NodeID == standby && slices.ContainsFunc(copies, func(c app.Copy) bool { return c.ID == allocation.ResourceID }) && allocation.Budget.MemoryBytes != 1<<30 {
			t.Fatalf("a tier 1 standby runs its engine and needs the full budget: %+v", allocation)
		}
		for _, port := range allocation.Ports {
			if port.Name == "reader" && port.Number == 15433 {
				readers++
			}
		}
	}
	if readers == 0 {
		t.Fatal("the reader route port was not reserved")
	}
	// Each copy learns its engine role. The standby follows the primary's published port.
	tables, err := f.store.AppReplicationTargets(t.Context(), claim.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	primaryPolicy, err := f.store.ReadNodePlacementPolicy(t.Context(), claim.InstallationID, f.node)
	if err != nil {
		t.Fatal(err)
	}
	var primaryPort uint16
	for _, copy := range copies {
		if copy.Role == "active" {
			primaryPort = copy.HostPort
		}
	}
	for node, want := range map[uuid.UUID]string{f.node: "active", standby: "standby"} {
		table := tables[node]
		if table == nil || len(table.Apps) != 1 || table.Apps[0].Role != want || table.Apps[0].Database == nil || table.Apps[0].Database.Databases[0] != "immich" {
			t.Fatalf("%s table: %+v", want, table)
		}
		database := table.Apps[0].Database
		if want == "standby" && (database.PrimaryHost != primaryPolicy.ClientAddress || database.PrimaryPort != uint32(primaryPort)) {
			t.Fatalf("standby follows %s:%d", database.PrimaryHost, database.PrimaryPort)
		}
		if want == "active" && database.PrimaryHost != "" {
			t.Fatalf("the primary names an upstream: %+v", database)
		}
		prepared, _, err := f.store.PrepareAppReplication(t.Context(), claim.InstallationID, node, table)
		if err != nil {
			t.Fatal(err)
		}
		session := uuid.MustParse(prepared.SessionId)
		if err := f.store.RecordAppReplicationAck(t.Context(), claim.InstallationID, node, session, &agentv1.ReplicationAck{SessionId: prepared.SessionId, Revision: prepared.Revision}); err != nil {
			t.Fatal(err)
		}
		heartbeat := &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.Now(), WriterClockNanos: uint64(time.Hour)}
		if want == "standby" {
			heartbeat.Replicas = []*agentv1.ReplicaStatus{{AppId: claim.AppID.String(), Role: "standby", Epoch: 1, Sequence: 3, CompletedAt: timestamppb.Now(), State: "streaming", LagSeconds: 0.5}}
		}
		if err := f.store.RecordAgentHeartbeat(t.Context(), claim.InstallationID, node, session, heartbeat); err != nil {
			t.Fatal(err)
		}
	}
	status, err := f.store.GetAppStatus(t.Context(), claim.InstallationID, claim.AppID)
	if err != nil || len(status.Status.Standbys) != 1 || status.Status.Standbys[0].State != "streaming" || status.Status.Standbys[0].LagSeconds == nil || *status.Status.Standbys[0].LagSeconds < 0.5 {
		t.Fatalf("standby status: %+v %v", status.Status, err)
	}
	// Planning again reuses the recorded credentials.
	again, err := f.planner.databaseCredentials(t.Context(), mustApp(t, f, claim.AppID))
	if err != nil || len(again) != 3 || again[0].CredentialID != credentials[postgres.SuperuserCredential] {
		t.Fatalf("credentials changed on repeat: %+v %v", again, err)
	}
}

func TestTier1RedisDeployTakesTwoCredentials(t *testing.T) {
	f, _, request := databaseFixture(t, app.DatabaseSpec{Engine: app.EngineRedis, Version: "8"})
	request.Spec.Router.ReaderPort = 0
	admitted, err := f.store.AdmitApp(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: admitted.OperationID, RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "database-test", TTL: time.Minute, LeaseID: 2, LeaseRevision: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatalf("deploy: %v (%v)", err, errors.Unwrap(err))
	}
	plan, err := f.store.ReadPlan(t.Context(), claim)
	if err != nil {
		t.Fatal(err)
	}
	ensure := plan.Steps[0].Command.Input.GetEnsureContainer()
	if ensure == nil || len(ensure.SecretEnv) != 2 || len(plan.Steps[0].Command.Input.Secrets) != 2 || ensure.Ports[0].ContainerPort != 6379 {
		t.Fatalf("redis container: %+v", ensure)
	}
	if credential, err := f.store.ReadAppCredential(t.Context(), claim.InstallationID, claim.AppID, postgres.ApplicationCredential); err != nil || credential != nil {
		t.Fatalf("redis has no application role: %+v %v", credential, err)
	}
}

func mustApp(t *testing.T, f *fixture, id uuid.UUID) app.App {
	t.Helper()
	found, err := f.store.GetApp(t.Context(), f.claim.InstallationID, id)
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func TestTier1RoutesAndFailover(t *testing.T) {
	f, standby, request := databaseFixture(t, app.DatabaseSpec{Engine: app.EnginePostgres, Version: "17", Databases: []string{"immich"}})
	admitted, err := f.store.AdmitApp(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: admitted.OperationID, RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "database-test", TTL: time.Minute, LeaseID: 2, LeaseRevision: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatalf("deploy: %v (%v)", err, errors.Unwrap(err))
	}
	copies, err := f.store.GetAppCopies(t.Context(), claim.InstallationID, claim.AppID)
	if err != nil {
		t.Fatal(err)
	}
	tables, err := f.store.AppReplicationTargets(t.Context(), claim.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	sessions := map[uuid.UUID]uuid.UUID{}
	for node, table := range tables {
		prepared, _, err := f.store.PrepareAppReplication(t.Context(), claim.InstallationID, node, table)
		if err != nil {
			t.Fatal(err)
		}
		sessions[node] = uuid.MustParse(prepared.SessionId)
		if err := f.store.RecordAppReplicationAck(t.Context(), claim.InstallationID, node, sessions[node], &agentv1.ReplicationAck{SessionId: prepared.SessionId, Revision: prepared.Revision}); err != nil {
			t.Fatal(err)
		}
	}
	// Both copies run healthy engines. The standby streams.
	for _, copy := range copies {
		heartbeat := &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.Now(), WriterClockNanos: uint64(time.Hour), Observations: []*agentv1.Observation{{ResourceId: copy.ID.String(), Lifecycle: "running", Health: "healthy", ObservedAt: timestamppb.Now(), Ports: []*agentv1.PortBinding{{HostAddress: "0.0.0.0", HostPort: uint32(copy.HostPort), ContainerPort: 5432, Transport: "tcp"}}}}}
		if copy.Role == "standby" {
			heartbeat.Replicas = []*agentv1.ReplicaStatus{{AppId: claim.AppID.String(), Role: "standby", Epoch: 1, Sequence: 5, CompletedAt: timestamppb.Now(), State: "streaming", LagSeconds: 0.2}}
		}
		if err := f.store.RecordAgentHeartbeat(t.Context(), claim.InstallationID, copy.NodeID, sessions[copy.NodeID], heartbeat); err != nil {
			t.Fatal(err)
		}
	}
	if grant, err := f.store.PrepareAppWriterLease(t.Context(), claim.InstallationID, claim.AppID, f.node, sessions[f.node], 1, false); err != nil || grant == nil {
		t.Fatalf("grant: %v %v", grant, err)
	}
	routes, err := f.store.AppRouteTargets(t.Context(), claim.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range routes {
		if len(target.Table.Routes) != 2 {
			t.Fatalf("routes: %+v", target.Table.Routes)
		}
		for _, route := range target.Table.Routes {
			want := 1
			if route.IngressPort == 15433 {
				want = 2
			}
			if len(route.Backends) != want {
				t.Fatalf("port %d reaches %v", route.IngressPort, route.Backends)
			}
		}
	}
	if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='succeeded',claim_active=false,finished_at=clock_timestamp() WHERE id=$1`, claim.OperationID); err != nil {
		t.Fatal(err)
	}
	// The last complete standby stays eligible while its upstream is down.
	heartbeat := &agentv1.Heartbeat{Sequence: 2, ObservedAt: timestamppb.Now(), WriterClockNanos: uint64(time.Hour), Replicas: []*agentv1.ReplicaStatus{{AppId: claim.AppID.String(), Role: "standby", Epoch: 1, Sequence: 6, CompletedAt: timestamppb.Now(), State: "disconnected", LagSeconds: 91.2}}}
	if err := f.store.RecordAgentHeartbeat(t.Context(), claim.InstallationID, standby, sessions[standby], heartbeat); err != nil {
		t.Fatal(err)
	}
	candidates, err := f.store.AppFailoverCandidates(t.Context(), claim.InstallationID, claim.AppID)
	if err != nil || len(candidates) != 1 || candidates[0].NodeID != standby {
		t.Fatalf("disconnected standby candidates: %+v %v", candidates, err)
	}
	currentRoutes, err := f.store.AppRouteTargets(t.Context(), claim.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range currentRoutes {
		for _, route := range target.Table.Routes {
			if len(route.Backends) != 1 {
				t.Fatalf("a disconnected standby received readers: %+v", route)
			}
		}
	}
	// The primary's lease is older than the failover delay.
	if _, err := f.pool.Exec(t.Context(), `UPDATE apps SET writer_last_grant_at=clock_timestamp()-interval '91 seconds' WHERE id=$1`, claim.AppID); err != nil {
		t.Fatal(err)
	}
	id, err := f.store.AdmitAppFailover(t.Context(), claim.InstallationID, claim.AppID)
	if err != nil || id == uuid.Nil {
		t.Fatalf("failover admission: %v %v", id, err)
	}
	next, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: claim.InstallationID, OperationID: id, RecoveryGeneration: claim.RecoveryGeneration, OwnerID: "database-test", TTL: time.Minute, LeaseID: 3, LeaseRevision: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.planner.Plan(t.Context(), next); err != nil {
		t.Fatalf("promote: %v (%v)", err, errors.Unwrap(err))
	}
	writer, err := f.store.GetAppWriter(t.Context(), claim.InstallationID, claim.AppID)
	if err != nil || writer.Epoch != 2 || writer.NodeID != standby {
		t.Fatalf("promoted writer: %+v %v", writer, err)
	}
	for node, target := range routes {
		for _, route := range target.Table.Routes {
			stale := &agentv1.RouteTable{SessionId: target.Table.SessionId, Routes: []*agentv1.AppRoute{route}}
			if _, _, err := f.store.PrepareAppRoutes(t.Context(), claim.InstallationID, node, stale); !errors.Is(err, operation.ErrCommandConflict) {
				t.Fatalf("a prior epoch restored port %d: %v", route.IngressPort, err)
			}
		}
	}
	// The promoted copy's reported lag must survive later heartbeat changes.
	if _, err := f.pool.Exec(t.Context(), `UPDATE app_replicas SET lag_seconds=0 WHERE app_id=$1`, claim.AppID); err != nil {
		t.Fatal(err)
	}
	status, err := postgres.New(f.pool).GetAppStatus(t.Context(), claim.InstallationID, claim.AppID)
	if err != nil || status.Status.LossWindowSeconds == nil || *status.Status.LossWindowSeconds != 91.2 {
		t.Fatalf("failover loss window: %+v %v", status.Status, err)
	}
	plan, err := f.store.ReadPlan(t.Context(), next)
	if err != nil || len(plan.Steps) != 2 || plan.Steps[0].Command.Epoch.NodeId != standby.String() || len(plan.Steps[0].Command.Input.Secrets) != 1 {
		t.Fatalf("promotion plan: %+v %v", plan, err)
	}
	copies, err = f.store.GetAppCopies(t.Context(), claim.InstallationID, claim.AppID)
	if err != nil {
		t.Fatal(err)
	}
	for _, copy := range copies {
		want := map[uuid.UUID]string{standby: "active", f.node: "standby"}[copy.NodeID]
		if copy.Role != want {
			t.Fatalf("copy on %s is %s, want %s", copy.NodeID, copy.Role, want)
		}
	}
	record, err := f.store.ReadPlacement(t.Context(), claim.InstallationID, claim.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	for _, allocation := range record.Allocations {
		if allocation.NodeID == f.node && slices.ContainsFunc(copies, func(c app.Copy) bool { return c.ID == allocation.ResourceID }) && allocation.Budget.MemoryBytes == 0 {
			t.Fatal("the old primary lost the reservation it needs to rejoin as a standby")
		}
	}
	tables, err = f.store.AppReplicationTargets(t.Context(), claim.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables[f.node].Apps) != 0 {
		t.Fatalf("old primary must wait for a rebuild: %v", tables[f.node])
	}
	for node, table := range tables {
		prepared, _, err := f.store.PrepareAppReplication(t.Context(), claim.InstallationID, node, table)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.store.RecordAppReplicationAck(t.Context(), claim.InstallationID, node, sessions[node], &agentv1.ReplicationAck{SessionId: prepared.SessionId, Revision: prepared.Revision}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.store.PrepareAppWriterLease(t.Context(), claim.InstallationID, claim.AppID, f.node, sessions[f.node], 1, true); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("old writer renewed its lease: %v", err)
	}
	if grant, err := f.store.PrepareAppWriterLease(t.Context(), claim.InstallationID, claim.AppID, standby, sessions[standby], 2, true); err != nil || grant == nil {
		t.Fatalf("new writer lease: %+v %v", grant, err)
	}
	var promotedPort uint16
	for _, copy := range copies {
		if copy.NodeID != standby {
			continue
		}
		promotedPort = copy.HostPort
		heartbeat := &agentv1.Heartbeat{Sequence: 3, ObservedAt: timestamppb.Now(), WriterClockNanos: uint64(time.Hour), Observations: []*agentv1.Observation{{ResourceId: copy.ID.String(), Lifecycle: "running", Health: "healthy", ObservedAt: timestamppb.Now(), Ports: []*agentv1.PortBinding{{HostAddress: "0.0.0.0", HostPort: uint32(copy.HostPort), ContainerPort: 5432, Transport: "tcp"}}}}}
		if err := f.store.RecordAgentHeartbeat(t.Context(), claim.InstallationID, standby, sessions[standby], heartbeat); err != nil {
			t.Fatal(err)
		}
	}
	currentRoutes, err = f.store.AppRouteTargets(t.Context(), claim.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	for node, target := range currentRoutes {
		if len(target.Table.Routes) != 2 {
			t.Fatalf("new epoch lost a route: %+v", target.Table)
		}
		for _, route := range target.Table.Routes {
			if len(route.Backends) != 1 || route.Backends[0] != fmt.Sprintf("192.0.2.11:%d", promotedPort) {
				t.Fatalf("port %d does not follow the new writer: %+v", route.IngressPort, route)
			}
		}
		if _, _, err := f.store.PrepareAppRoutes(t.Context(), claim.InstallationID, node, target.Table); err != nil {
			t.Fatalf("new epoch routes: %v", err)
		}
	}

	exerciseDatabaseRejoin(t, f, next, plan, 0, standby, f.node, false)

}
