package provision

import (
	"errors"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestPostgresReplicaPlanBindsSourceStorageAndReadiness(t *testing.T) {
	f := provisioned(t)
	original := installedPlanFor(t, f, f.claim.OperationID)
	primaryConfig := original.Steps[1].Command.Input.GetEnsureContainer().Postgres
	before, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(before) != 1 {
		t.Fatalf("primary: %+v %v", before, err)
	}
	holds, err := f.store.ReadPlacementHolds(t.Context(), f.claim.InstallationID)
	if err != nil || len(holds) != 1 {
		t.Fatalf("holds: %+v %v", holds, err)
	}
	claim := f.act(t, postgres.ScaleClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	plan := installedPlanFor(t, f, claim.OperationID)
	op, err := f.store.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := operation.ValidatePlan(plan, op); err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 9 {
		t.Fatalf("steps=%d", len(plan.Steps))
	}
	source, volume, container, prepare, clone, stop, start, native, app := plan.Steps[0], plan.Steps[1], plan.Steps[2], plan.Steps[3], plan.Steps[4], plan.Steps[5], plan.Steps[6], plan.Steps[7], plan.Steps[8]
	if source.Command.Input.ResourceId != before[0].ResourceID.String() || source.Command.Input.GetCheckReadiness().PostgresRole != "primary" || source.Command.Input.GetCheckReadiness().PostgresReplication == nil || source.Command.Input.GetCheckReadiness().Role != "synod_admin" {
		t.Fatal("copy omitted primary identity proof")
	}
	config := container.Command.Input.GetEnsureContainer()
	if volume.Command.Input.GetEnsureVolume() == nil || config == nil || config.Postgres.ReplicaSource == nil || prepare.Command.Input.GetPreparePostgresReplica() == nil || clone.Command.Input.GetClonePostgresReplica() == nil || stop.Command.Input.GetStopContainer() == nil || start.Command.Input.GetStartContainer() == nil || native.Command.Input.GetCheckReadiness() == nil || app.Command.Input.GetCheckReadiness() == nil {
		t.Fatal("copy omitted a required native phase")
	}
	peer := config.Postgres.ReplicaSource
	if peer.Address != "192.0.2.10" || peer.Port != primaryConfig.Port || peer.ResourceId != before[0].ResourceID.String() {
		t.Fatal("replica lost the exact source")
	}
	if config.Postgres.SuperuserCredentialId != primaryConfig.SuperuserCredentialId || config.Postgres.ApplicationCredentialId != primaryConfig.ApplicationCredentialId || config.Postgres.ReplicationCredentialId != primaryConfig.ReplicationCredentialId {
		t.Fatal("replica replaced cluster credentials")
	}
	for _, step := range []*agentv1.PlanStep{container, clone} {
		if len(step.Command.Input.Secrets) != 2 || step.Command.Input.Secrets[0].Purpose != postgres.ApplicationCredential || step.Command.Input.Secrets[1].Purpose != postgres.ReplicationCredential {
			t.Fatal("copy delivers administrative material")
		}
	}
	if start.Command.Input.GetStartContainer().AllowInitialization {
		t.Fatal("replica start can initialize missing data")
	}
	if !proto.Equal(clone.Command.Input.GetClonePostgresReplica().Postgres, config.Postgres) {
		t.Fatal("clone differs from stored configuration")
	}
	policy := native.Command.Input.GetCheckReadiness().PostgresReplication
	if policy == nil || !proto.Equal(policy.Source, peer) || native.Command.Input.GetCheckReadiness().PostgresRole != "replica" || app.Command.Input.GetCheckReadiness().PostgresRole != "replica" || app.Command.Input.GetCheckReadiness().Role != "synod_app" {
		t.Fatal("final readiness omits replication or app access")
	}
	for i, step := range plan.Steps {
		if i > 0 && (len(step.DependsOn) != 1 || step.DependsOn[0] != plan.Steps[i-1].Command.StepId) {
			t.Fatal("copy effects can race")
		}
		if i > 1 && step.Command.Input.ResourceId == before[0].ResourceID.String() {
			t.Fatal("copy mutates the primary")
		}
	}
	disk := 3 * f.planner.config.Budget.DiskBytes
	if clone.Command.Input.GetClonePostgresReplica().RequiredFreeBytes != disk || f.decider.request.Resources[0].Budgets.Backup.DiskBytes != 2*f.planner.config.Budget.DiskBytes {
		t.Fatal("copy working space was not reserved")
	}
	identity, err := f.store.ReadVolumeIdentity(t.Context(), claim.InstallationID, uuid.MustParse(volume.Command.Input.ResourceId))
	if err != nil || identity.CapacityRequestBytes != disk || identity.Retention != postgres.Retained {
		t.Fatalf("volume capacity: %+v %v", identity, err)
	}
	after, err := f.store.ReadPlacementHolds(t.Context(), claim.InstallationID)
	if err != nil || len(after) != 2 {
		t.Fatalf("holds: %+v %v", after, err)
	}
	for _, hold := range after {
		if hold.ResourceID == holds[0].ResourceID && (hold.Budget != holds[0].Budget || hold.ReservationID != holds[0].ReservationID || hold.NodeID != holds[0].NodeID) {
			t.Fatal("copy replaced the primary allocation")
		}
		if hold.ResourceID == uuid.MustParse(container.Command.Input.ResourceId) && hold.Budget.DiskBytes != disk {
			t.Fatal("stored allocation lost copy space")
		}
	}
}

func TestPostgresReplicaRejectsCapacityBeforePlacement(t *testing.T) {
	for _, bytes := range []int64{0, 1 << 50, 1 << 62} {
		t.Run(strconv.FormatInt(bytes, 10), func(t *testing.T) {
			f := provisioned(t)
			f.planner.config.Budget.DiskBytes = bytes
			prior := f.decider.request.OperationID
			claim := f.act(t, postgres.ScaleClusterAPIOperation)
			if err := f.planner.Plan(t.Context(), claim); !errors.Is(err, ErrUnplannable) {
				t.Fatalf("invalid copy budget: %v", err)
			}
			if f.decider.request.OperationID != prior {
				t.Fatal("invalid budget reached placement")
			}
			volumes, err := f.store.ReadNodeVolumeIdentities(t.Context(), claim.InstallationID, f.node)
			if err != nil || len(volumes) != 1 {
				t.Fatalf("invalid budget allocated volume metadata: %+v %v", volumes, err)
			}
		})
	}
}

func TestPostgresReplicaRejectsAnUntestedVersion(t *testing.T) {
	spec := postgresSpec()
	spec.PostgreSQL.Version = "18.6"
	f := newFixture(t, spec)
	claim := f.act(t, postgres.ScaleClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); !errors.Is(err, ErrUnplannable) {
		t.Fatalf("untested replica version: %v", err)
	}
}
