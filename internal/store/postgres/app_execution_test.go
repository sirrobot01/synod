package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestAppPlanRollsBackCopiesAndReservationsTogether(t *testing.T) {
	h := placementFixture(t)
	admitted, err := h.store.AdmitApp(t.Context(), CreateAppRequest{Identity: RequestIdentity{InstallationID: h.id, ActorID: "app-test", APIOperation: CreateAppAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: "test", Spec: app.AppSpec{SchemaVersion: 1, Name: "web", Tier: app.TierParallel, Copies: 1, Service: app.ServiceSpec{Image: "nginx", Ports: []app.PortSpec{{ContainerPort: 80}}}, Router: app.RouterSpec{Port: 8080, Protocol: "http"}}})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.AcquireClaim(t.Context(), ClaimRequest{ClaimID: uuid.New(), InstallationID: h.id, OperationID: admitted.OperationID, RecoveryGeneration: h.generation, OwnerID: "app-test", TTL: time.Minute, LeaseID: 1, LeaseRevision: 1})
	if err != nil {
		t.Fatal(err)
	}
	request := placementRequest(h.id, claim.ClusterID, claim.OperationID, h.policy, []placement.Port{{Name: "backend", Address: "0.0.0.0", Number: 23000, Transport: "tcp"}})
	resource := &request.Resources[0]
	resource.Kind = "service"
	resource.Role = ""
	resource.Engine = "generic"
	resource.Version = ""
	resource.Architecture = ""
	resource.SupportedArchitectures = []string{h.policy.Architecture}
	snapshot, err := h.store.ReadPlacementSnapshot(t.Context(), h.id)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := placement.Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	plan := planFor(t, claim, h.node, resource.ID, 1)
	image := "nginx@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	input := &agentv1.EnsureContainer{ImageDigest: image, Engine: "generic", NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, Generic: &agentv1.GenericContainerConfig{Image: image, AppName: "web", CopyIndex: 1}, Ports: []*agentv1.PortBinding{{HostAddress: "0.0.0.0", HostPort: 23000, ContainerPort: 80, Transport: "tcp"}}}
	plan.Steps[0].Command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: input}
	plan.Steps[0].Postcondition = agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS
	hash, err := operation.CommandHash(plan.Steps[0].Command)
	if err != nil {
		t.Fatal(err)
	}
	plan.Steps[0].Command.InputHash = hash[:]
	if _, err := h.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, nil); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("invalid copy index: %v", err)
	}
	if h.count(t, `SELECT count(*) FROM resource_allocations`) != 0 || h.count(t, `SELECT count(*) FROM app_copies`) != 0 || h.count(t, `SELECT count(*) FROM operations WHERE plan_version<>0`) != 0 {
		t.Fatal("failed plan left partial state")
	}
	input.Generic.CopyIndex = 0
	hash, err = operation.CommandHash(plan.Steps[0].Command)
	if err != nil {
		t.Fatal(err)
	}
	plan.Steps[0].Command.InputHash = hash[:]
	if _, err := h.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, nil); err != nil {
		t.Fatal(err)
	}
	_, err = h.store.AdmitAppAction(t.Context(), AppActionRequest{Identity: RequestIdentity{InstallationID: h.id, ActorID: "operator", APIOperation: DeleteAppAPIOperation, IdempotencyKey: "delete"}, RequestID: "delete", AppID: admitted.AppID, ExpectedRevision: 1})
	requireCode(t, err, OperationConflict)
	// A new owner keeps the immutable plan and copy identities after a lost controller.
	if _, err := h.store.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, claim.OperationID); err != nil {
		t.Fatal(err)
	}
	restarted := New(h.store.pool)
	next, err := restarted.AcquireClaim(t.Context(), ClaimRequest{ClaimID: uuid.New(), InstallationID: h.id, OperationID: claim.OperationID, RecoveryGeneration: h.generation, OwnerID: "restarted", TTL: time.Minute, LeaseID: 2, LeaseRevision: 2})
	if err != nil {
		t.Fatal(err)
	}
	if next.ExecutionEpoch != claim.ExecutionEpoch+1 {
		t.Fatal("takeover kept the old epoch")
	}
	recovered, err := restarted.ReadPlan(t.Context(), next)
	if err != nil || !proto.Equal(plan, recovered) {
		t.Fatalf("takeover changed commands: %v", err)
	}
	if h.count(t, `SELECT count(*) FROM app_copies`) != 1 || h.count(t, `SELECT count(*) FROM resource_allocations`) != 1 {
		t.Fatal("takeover duplicated resources")
	}
}
