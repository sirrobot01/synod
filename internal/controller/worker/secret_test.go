package worker

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// A plan may name a credential. It may never carry one.
func TestPlanAcceptsSQLReferencesAndRestrictsNativePurposes(t *testing.T) {
	op, node := testOperation(), uuid.NewString()
	resource := uuid.NewString()
	step := planStep(t, op, node, resource, agentv1.Postcondition_POSTCONDITION_ENGINE_READY)
	step.Command.Input.Secrets = []*agentv1.SecretReference{
		{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"},
	}
	rehash(t, step.Command)
	if err := operation.ValidatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{step}}, op); err != nil {
		t.Fatalf("a plan naming a credential was refused: %v", err)
	}
	for _, purpose := range []string{"root", "unknown"} {
		step.Command.Input.Secrets[0].Purpose = purpose
		rehash(t, step.Command)
		if err := operation.ValidatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{step}}, op); !errors.Is(err, operation.ErrUnsupportedPlan) {
			t.Fatalf("purpose=%s err=%v, want refusal of an unknown purpose", purpose, err)
		}
	}
}

// A controller with no keyring refuses to dispatch a command that needs a credential
// rather than sending one the agent cannot run.
func TestDispatchRefusesWhatItCannotResolve(t *testing.T) {
	w := &Worker{config: Config{}}
	command := &agentv1.Command{
		Epoch: &agentv1.Epoch{ClusterId: uuid.NewString()},
		Input: &agentv1.CommandInput{Secrets: []*agentv1.SecretReference{
			{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"},
		}},
	}
	if err := w.pushSecrets(t.Context(), uuid.New(), "session", command); !errors.Is(err, ErrSecretUnresolvable) {
		t.Fatalf("err=%v, want ErrSecretUnresolvable", err)
	}
	// A command that needs nothing is sent by a controller with no keyring at all.
	plain := &agentv1.Command{Epoch: &agentv1.Epoch{ClusterId: uuid.NewString()}, Input: &agentv1.CommandInput{}}
	if err := w.pushSecrets(t.Context(), uuid.New(), "session", plain); err != nil {
		t.Fatalf("a command needing no credential was refused: %v", err)
	}
}

// A credential is bound to its owner, so a worker cannot open one that belongs to
// another cluster even by accident.
func TestACredentialIsBoundToItsCluster(t *testing.T) {
	active := uuid.New()
	ring, err := secret.NewKeyring(active, map[uuid.UUID][]byte{active: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	binding := secret.Binding{
		InstallationID: uuid.New(), OwnerType: "cluster", OwnerID: uuid.New(),
		CredentialID: uuid.New(), Version: 1,
	}
	envelope, err := ring.Seal(binding, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	moved := binding
	moved.OwnerID = uuid.New()
	if _, err := ring.Open(moved, envelope); err == nil {
		t.Fatal("a credential opened under another cluster's binding")
	}
}

// The worker must be able to verify every postcondition a plan may declare. A
// postcondition it does not know refuses the receipt forever: the result is never
// acknowledged and the agent replays it without end.
func TestEveryPostconditionCanBeProved(t *testing.T) {
	op, node := testOperation(), uuid.NewString()
	for postcondition, state := range map[agentv1.Postcondition]string{
		agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS:            "present",
		agentv1.Postcondition_POSTCONDITION_NETWORK_EXISTS:           "present",
		agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS:         "created",
		agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING:        "running",
		agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED:        "exited",
		agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT:         "absent",
		agentv1.Postcondition_POSTCONDITION_CONTAINER_INSPECTED:      "running",
		agentv1.Postcondition_POSTCONDITION_ENGINE_READY:             "ready",
		agentv1.Postcondition_POSTCONDITION_POSTGRES_SETUP_RECOVERED: "setup_recovered",
		agentv1.Postcondition_POSTCONDITION_APP_SNAPSHOT_TRANSFERRED: "snapshot_transferred",
		agentv1.Postcondition_POSTCONDITION_APP_DATA_ABSENT:          "app_data_absent",
	} {
		t.Run(postcondition.String(), func(t *testing.T) {
			step := planStep(t, op, node, uuid.NewString(), postcondition)
			// A readiness step names its effect differently, because it is an
			// observation over the network rather than a container action.
			native := "docker-command:" + step.Command.CommandId
			if postcondition == agentv1.Postcondition_POSTCONDITION_ENGINE_READY {
				native = "readiness:" + step.Command.CommandId
			}
			effect := &agentv1.Effect{CommandId: step.Command.CommandId, ResourceId: step.Command.Input.ResourceId, NativeOperationId: native, State: state}
			if postcondition == agentv1.Postcondition_POSTCONDITION_APP_SNAPSHOT_TRANSFERRED {
				effect.NativeOperationId = "app-data:" + step.Command.CommandId
				effect.AppSnapshot = &agentv1.AppSnapshotReceipt{WriterEpoch: 1, Sequence: 1, TargetNodeId: step.Command.Input.GetFinalAppSnapshot().TargetNodeId, ManifestHash: make([]byte, 32)}
			}
			if postcondition == agentv1.Postcondition_POSTCONDITION_APP_DATA_ABSENT {
				effect.NativeOperationId = "app-data:" + step.Command.CommandId
			}
			if err := operation.CheckStepEffects(step, []*agentv1.Effect{effect}); err != nil {
				t.Fatalf("a %s postcondition cannot be proved: %v", postcondition, err)
			}
		})
	}
	// Every postcondition in the enum is covered above, so a new one added without a
	// rule here fails this test rather than a four-minute replay loop in production.
	highest := agentv1.Postcondition_POSTCONDITION_APP_DATA_ABSENT
	if _, known := agentv1.Postcondition_name[int32(highest)+1]; known {
		t.Fatal("a postcondition was added to the protocol without a rule that proves it")
	}
}

func TestWorkerDeliversExactCredentialVersion(t *testing.T) {
	h := newHarness(t, func(*agentv1.ControllerMessage) []*agentv1.AgentMessage { return nil })
	key := uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	h.worker.config.Keyring = ring
	credential := postgres.Credential{CredentialID: uuid.New(), Version: 1, Purpose: postgres.SuperuserCredential, RoleName: "synod_admin"}
	for version := int64(1); version <= 2; version++ {
		credential.Version = version
		credential.Envelope, err = ring.Seal(secret.Binding{InstallationID: h.claim.InstallationID, OwnerType: "cluster", OwnerID: h.claim.ClusterID, CredentialID: credential.CredentialID, Version: version}, []byte(fmt.Sprintf("material-%d", version)))
		if err != nil {
			t.Fatal(err)
		}
		if err := h.store.RecordClusterIntent(t.Context(), postgres.ClusterIntent{InstallationID: h.claim.InstallationID, ClusterID: h.claim.ClusterID, Credentials: []postgres.Credential{credential}}); err != nil {
			t.Fatal(err)
		}
	}
	peer, ok := h.transport.Session(h.node)
	if !ok {
		t.Fatal("agent disconnected")
	}
	command := proto.Clone(h.plan.Steps[1].Command).(*agentv1.Command)
	command.Input.Secrets = []*agentv1.SecretReference{{CredentialId: credential.CredentialID.String(), Version: 1, Purpose: credential.Purpose}}
	if err := h.worker.pushSecrets(t.Context(), h.node, peer.SessionID, command); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-h.received:
		material := message.GetSecret()
		if material == nil || material.Version != 1 || material.CredentialId != credential.CredentialID.String() || string(material.Material) != "material-1" {
			t.Fatal("worker substituted a newer credential")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("credential did not reach the authenticated session")
	}
	for name, change := range map[string]func(*agentv1.Command){
		"other cluster":    func(c *agentv1.Command) { c.Epoch.ClusterId = uuid.NewString() },
		"missing identity": func(c *agentv1.Command) { c.Input.Secrets[0].CredentialId = uuid.NewString() },
		"missing version":  func(c *agentv1.Command) { c.Input.Secrets[0].Version = 3 },
		"wrong purpose":    func(c *agentv1.Command) { c.Input.Secrets[0].Purpose = postgres.ApplicationCredential },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(command).(*agentv1.Command)
			change(changed)
			if err := h.worker.pushSecrets(t.Context(), h.node, peer.SessionID, changed); !errors.Is(err, ErrSecretUnresolvable) {
				t.Fatalf("err=%v, want refusal", err)
			}
		})
	}
	select {
	case <-h.received:
		t.Fatal("a refused credential reached the agent")
	default:
	}
}
