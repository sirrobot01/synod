package provision

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	imageinspect "github.com/sirrobot01/synod/internal/runtime/image"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAppPlansCommitCopiesPortsAndCommands(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		mode   app.CopiesMode
		worker bool
	}{
		{"fixed", app.CopiesModeFixed, false}, {"all_capable", app.CopiesModeAllCapable, false}, {"worker_fixed", app.CopiesModeFixed, true}, {"worker_all_capable", app.CopiesModeAllCapable, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			mode, worker := scenario.mode, scenario.worker
			f := newFixture(t)
			// The database fixture supplies approved amd64 capacity. Add a generic-only arm64 node.
			arm := uuid.New()
			if _, err := f.pool.Exec(t.Context(), `INSERT INTO nodes(id,installation_id,public_key_hash,enrolled_at) VALUES($1,$2,$3,clock_timestamp())`, arm, f.claim.InstallationID, make([]byte, 32)); err != nil {
				t.Fatal(err)
			}
			readyNodeWith(t, f.store, f.claim.InstallationID, arm, func(policy *placement.NodePolicy) {
				policy.Architecture = "arm64"
				policy.Engines = nil
				policy.Labels = map[string]string{"synod.name": "arm"}
				policy.ClientAddress = "192.0.2.11"
			})
			input := postgres.CreateAppRequest{Identity: postgres.RequestIdentity{InstallationID: f.claim.InstallationID, ActorID: "actor", APIOperation: postgres.CreateAppAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: "app-request",
				Spec: app.AppSpec{SchemaVersion: 1, Name: "web", Tier: app.TierParallel, Copies: 3, CopiesMode: mode, Service: app.ServiceSpec{Image: "nginx", Ports: []app.PortSpec{{ContainerPort: 80}}, Command: []string{}, Environment: map[string]string{"MODE": "test"}}, Router: app.RouterSpec{Port: 8080, Protocol: "http"}}}
			if worker {
				input.Spec.Router = app.RouterSpec{}
				input.Spec.Service.Ports = nil
			}
			admitted, err := f.store.AdmitApp(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: admitted.OperationID, RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "app-owner", TTL: time.Minute, LeaseID: 2, LeaseRevision: 2})
			if err != nil {
				t.Fatal(err)
			}
			f.planner.config.Keyring = nil
			f.planner.config.ResolveImage = func(_ context.Context, s app.ServiceSpec) (imageinspect.Resolution, error) {
				s.Image = "nginx@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
				s.SupportedArchitectures = []string{"amd64", "arm64"}
				return imageinspect.Resolution{Service: s}, nil
			}
			resolve := f.planner.config.ResolveImage
			f.planner.config.ResolveImage = func(context.Context, app.ServiceSpec) (imageinspect.Resolution, error) {
				return imageinspect.Resolution{}, &imageinspect.Error{Code: imageinspect.Unavailable}
			}
			if err := f.planner.Plan(t.Context(), claim); err == nil {
				t.Fatal("unverified image was placed")
			}
			pending, err := f.store.GetAppStatus(t.Context(), claim.InstallationID, claim.AppID)
			if err != nil || pending.Status.State != "pending" || pending.Status.FailureCode != string(imageinspect.Unavailable) || len(pending.Status.Copies) != 0 {
				t.Fatalf("image failure status: %+v %v", pending.Status, err)
			}
			f.planner.config.ResolveImage = resolve
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			copies, err := f.store.GetAppCopies(t.Context(), claim.InstallationID, claim.AppID)
			if err != nil {
				t.Fatal(err)
			}
			want := 3
			if mode == app.CopiesModeAllCapable {
				want = 2
			}
			if len(copies) != want {
				t.Fatalf("copies=%d want %d", len(copies), want)
			}
			nodes := map[uuid.UUID]bool{}
			for _, copy := range copies {
				nodes[copy.NodeID] = true
				if (copy.HostPort == 0) != worker || copy.ObservedState != "pending" {
					t.Fatalf("copy %+v", copy)
				}
			}
			if len(nodes) != 2 {
				t.Fatalf("mixed architecture spread: %v", nodes)
			}
			state, err := f.store.LoadExecutionState(t.Context(), claim)
			if err != nil {
				t.Fatal(err)
			}
			if len(state.Plan.Steps) != 2*want {
				t.Fatalf("steps=%d", len(state.Plan.Steps))
			}
			for _, step := range state.Plan.Steps {
				if ensure := step.Command.Input.GetEnsureContainer(); ensure != nil {
					if ensure.Generic.Image == "nginx" || !ensure.Generic.CommandOverride || ensure.MemoryBytes != 256<<20 || ensure.CpuMillis != 500 {
						t.Fatalf("container %+v", ensure)
					}
				}
			}
			record, err := f.store.ReadPlacement(t.Context(), claim.InstallationID, claim.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			ingressCount := 2
			if worker {
				ingressCount = 0
			}
			if len(record.Allocations) != want+ingressCount {
				t.Fatalf("reservations=%d", len(record.Allocations))
			}
			// A new store can replay the installed transaction without fresh probe evidence.
			restarted := postgres.New(f.pool)
			replay, err := restarted.InstallPlacementPlan(t.Context(), claim, record.Request, record.Decision, state.Plan)
			if err != nil {
				t.Fatal(err)
			}
			if replay.PlanHash != record.PlanHash {
				t.Fatal("replay changed plan")
			}
			again, err := restarted.LoadExecutionState(t.Context(), claim)
			if err != nil || !proto.Equal(again.Plan, state.Plan) {
				t.Fatalf("restart changed commands: %v", err)
			}
			// Only current observations from the assigned node can add backends.
			for _, copy := range copies {
				presence, err := f.store.GetAgentPresence(t.Context(), claim.InstallationID, copy.NodeID)
				if err != nil {
					t.Fatal(err)
				}
				heartbeat := &agentv1.Heartbeat{Sequence: uint64(copy.CopyIndex + 1), ObservedAt: timestamppb.Now(), Observations: []*agentv1.Observation{{ResourceId: copy.ID.String(), Lifecycle: "running", Health: "healthy", ObservedAt: timestamppb.Now(), Ports: []*agentv1.PortBinding{{HostAddress: "0.0.0.0", HostPort: uint32(copy.HostPort), ContainerPort: 80, Transport: "tcp"}}}}}
				if worker {
					heartbeat.Observations[0].Ports = nil
				}
				if err := f.store.RecordAgentHeartbeat(t.Context(), claim.InstallationID, copy.NodeID, presence.SessionID, heartbeat); err != nil {
					t.Fatal(err)
				}
			}
			targets, err := f.store.AppRouteTargets(t.Context(), claim.InstallationID)
			if err != nil {
				t.Fatal(err)
			}
			if worker {
				for _, allocation := range record.Allocations {
					if len(allocation.Ports) != 0 {
						t.Fatal("worker reserved ports")
					}
				}
				for _, target := range targets {
					if len(target.Table.Routes) != 0 {
						t.Fatal("worker created routes")
					}
				}
				if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='succeeded' WHERE id=$1`, admitted.OperationID); err != nil {
					t.Fatal(err)
				}
				current, err := f.store.GetAppStatus(t.Context(), claim.InstallationID, claim.AppID)
				if err != nil || current.Status.State != "running" || len(current.Status.Endpoints) != 0 {
					t.Fatalf("worker status: %+v %v", current.Status, err)
				}
				if _, err := f.pool.Exec(t.Context(), `UPDATE agent_presence SET last_seen_at=clock_timestamp()-interval '1 hour' WHERE node_id=$1`, arm); err != nil {
					t.Fatal(err)
				}
				current, err = f.store.GetAppStatus(t.Context(), claim.InstallationID, claim.AppID)
				if err != nil || current.Status.State == "running" {
					t.Fatalf("stale worker stayed running: %+v %v", current.Status, err)
				}
				return
			}
			for node, target := range targets {
				if len(target.Table.Routes) != 1 || len(target.Table.Routes[0].Backends) != want {
					t.Fatalf("routes: %v", target.Table)
				}
				table, acked, err := f.store.PrepareAppRoutes(t.Context(), claim.InstallationID, node, target.Table)
				if err != nil || acked {
					t.Fatalf("prepare: %v %v", acked, err)
				}
				retry, acked, err := f.store.PrepareAppRoutes(t.Context(), claim.InstallationID, node, target.Table)
				if err != nil || acked || !proto.Equal(table, retry) {
					t.Fatalf("route retry: %v", err)
				}
				ack := &agentv1.RouteAck{SessionId: table.SessionId, Revision: table.Revision, FailureCode: "route_port_conflict"}
				if err := f.store.RecordAppRouteAck(t.Context(), claim.InstallationID, node, uuid.MustParse(table.SessionId), ack); err != nil {
					t.Fatal(err)
				}
				failed, err := f.store.GetAppStatus(t.Context(), claim.InstallationID, claim.AppID)
				if err != nil {
					t.Fatal(err)
				}
				for _, endpoint := range failed.Status.Endpoints {
					if endpoint.NodeID == node && (endpoint.Ready || endpoint.FailureCode != "route_port_conflict") {
						t.Fatalf("false ready: %+v", endpoint)
					}
				}
				ack.FailureCode = ""
				if err := f.store.RecordAppRouteAck(t.Context(), claim.InstallationID, node, uuid.MustParse(table.SessionId), ack); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE agent_presence SET last_seen_at=clock_timestamp()-interval '1 hour' WHERE node_id=$1`, arm); err != nil {
				t.Fatal(err)
			}
			targets, err = f.store.AppRouteTargets(t.Context(), claim.InstallationID)
			if err != nil {
				t.Fatal(err)
			}
			if target := targets[arm]; len(target.Table.Routes) != 0 {
				t.Fatal("lost node kept ingress")
			}
			for _, target := range targets {
				if len(target.Table.Routes) > 0 && len(target.Table.Routes[0].Backends) >= want {
					t.Fatal("lost node retained backend")
				}
			}
			policy, err := f.store.ReadNodePlacementPolicy(t.Context(), claim.InstallationID, f.node)
			if err != nil {
				t.Fatal(err)
			}
			policy.Maintenance = true
			if _, err := f.store.PutNodePlacementPolicy(t.Context(), policy, policy.Revision); err != nil {
				t.Fatal(err)
			}
			targets, err = f.store.AppRouteTargets(t.Context(), claim.InstallationID)
			if err != nil {
				t.Fatal(err)
			}
			target := targets[f.node]
			if len(target.Table.Routes) != 0 {
				t.Fatal("maintenance kept ingress")
			}
			table, acknowledged, err := f.store.PrepareAppRoutes(t.Context(), claim.InstallationID, f.node, target.Table)
			if err != nil || acknowledged || table.Revision < 2 {
				t.Fatalf("clear route snapshot: %v %v %v", table, acknowledged, err)
			}
			if err := f.store.RecordAppRouteAck(t.Context(), claim.InstallationID, f.node, uuid.MustParse(table.SessionId), &agentv1.RouteAck{SessionId: table.SessionId, Revision: table.Revision - 1}); err != nil {
				t.Fatal(err)
			}
			_, acknowledged, err = f.store.PrepareAppRoutes(t.Context(), claim.InstallationID, f.node, target.Table)
			if err != nil || acknowledged {
				t.Fatalf("old ack accepted for new routes: %v", err)
			}
			// A different plan cannot replace committed identities.
			changed := proto.Clone(state.Plan).(*agentv1.ExecutionPlan)
			changed.Steps[0].Command.CommandId = uuid.NewString()
			hash, err := operation.CommandHash(changed.Steps[0].Command)
			if err != nil {
				t.Fatal(err)
			}
			changed.Steps[0].Command.InputHash = hash[:]
			if _, err := restarted.InstallPlacementPlan(t.Context(), claim, record.Request, record.Decision, changed); !errors.Is(err, operation.ErrCommandConflict) {
				t.Fatalf("changed replay: %v", err)
			}
		})
	}
}
