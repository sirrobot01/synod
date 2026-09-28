package provision

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	imageinspect "github.com/sirrobot01/synod/internal/runtime/image"
	"github.com/sirrobot01/synod/internal/store/postgres"
	"google.golang.org/protobuf/proto"
)

func TestAppLifecycleKeepsPinsAndRetriesFailedCleanup(t *testing.T) {
	f := newFixture(t)
	f.planner.config.Keyring = nil
	f.planner.config.ResolveImage = func(_ context.Context, s app.ServiceSpec) (imageinspect.Resolution, error) {
		s.Image = "nginx@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		s.SupportedArchitectures = []string{"amd64"}
		return imageinspect.Resolution{Service: s}, nil
	}
	created, err := f.store.AdmitApp(t.Context(), postgres.CreateAppRequest{Identity: postgres.RequestIdentity{InstallationID: f.claim.InstallationID, ActorID: "operator", APIOperation: postgres.CreateAppAPIOperation, IdempotencyKey: "create"}, RequestID: "create", Spec: app.AppSpec{SchemaVersion: 1, Name: "web", Tier: app.TierParallel, Copies: 2, Service: app.ServiceSpec{Image: "nginx", Ports: []app.PortSpec{{ContainerPort: 80}}, Command: []string{}}, Router: app.RouterSpec{Protocol: "http", Port: 8080}}})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: created.OperationID, RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "app", TTL: time.Minute, LeaseID: 2, LeaseRevision: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	original, err := f.store.ReadPlan(t.Context(), claim)
	if err != nil {
		t.Fatal(err)
	}
	f.planner.config.ResolveImage = func(context.Context, app.ServiceSpec) (imageinspect.Resolution, error) {
		t.Fatal("lifecycle resolved the moving image tag again")
		return imageinspect.Resolution{}, nil
	}
	prior := created.OperationID
	for index, action := range []string{"stop", "start", "delete", "delete"} {
		// Model a failed operation after its plan was installed. Its copies and holds remain.
		if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',claim_active=false,finished_at=clock_timestamp() WHERE id=$1`, prior); err != nil {
			t.Fatal(err)
		}
		accepted, err := f.store.AdmitAppAction(t.Context(), postgres.AppActionRequest{Identity: postgres.RequestIdentity{InstallationID: claim.InstallationID, ActorID: "operator", APIOperation: "app." + action, IdempotencyKey: uuid.NewString()}, RequestID: "action", AppID: created.AppID, ExpectedRevision: int64(index + 1)})
		if err != nil {
			t.Fatal(err)
		}
		claim, err = f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: claim.InstallationID, OperationID: accepted.OperationID, RecoveryGeneration: claim.RecoveryGeneration, OwnerID: "app", TTL: time.Minute, LeaseID: int64(index + 3), LeaseRevision: int64(index + 3)})
		if err != nil {
			t.Fatal(err)
		}
		if err := f.planner.Plan(t.Context(), claim); err != nil {
			t.Fatal(err)
		}
		plan, err := f.store.ReadPlan(t.Context(), claim)
		if err != nil {
			t.Fatal(err)
		}
		want := 4
		if action == "stop" {
			want = 2
		}
		if len(plan.Steps) != want {
			t.Fatalf("%s commands: %d", action, len(plan.Steps))
		}
		if action == "start" {
			for i := 0; i < len(plan.Steps); i += 2 {
				if plan.Steps[i].Command.Input.ResourceId != original.Steps[i].Command.Input.ResourceId || !proto.Equal(plan.Steps[i].Command.Input.GetEnsureContainer(), original.Steps[i].Command.Input.GetEnsureContainer()) {
					t.Fatal("start changed copy or configuration")
				}
			}
		}
		prior = accepted.OperationID
	}
}
