package executor

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestGenericContainerUsesDurableAdmission(t *testing.T) {
	var f *fixture
	var command *agentv1.Command
	f = newFixture(t, func(request *http.Request, base http.RoundTripper) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "/containers/create") || strings.HasSuffix(request.URL.Path, "/start") {
			record, err := f.e.journal.Get(command.CommandId)
			if err != nil || record.State != journal.Running {
				t.Error("generic effect preceded journal start", err)
			}
		}
		return base.RoundTrip(request)
	})
	ctx, _ := localSession(t, f.e)
	activateLocal(t, f.e, ctx, f.epoch)
	image := "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3"
	command = volumeCommand(t, f.epoch)
	command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{ImageDigest: image, Engine: "generic", NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, Generic: &agentv1.GenericContainerConfig{Image: image, AppName: "journal-app", Mounts: []*agentv1.Mount{{Type: "tmpfs", Target: "/data"}}}}}
	for step := range 2 {
		if step == 1 {
			command.CommandId = uuid.NewString()
			command.StepId = uuid.NewString()
			command.Input.Payload = &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}
		}
		hash, err := operation.CommandHash(command)
		if err != nil {
			t.Fatal(err)
		}
		command.InputHash = hash[:]
		ack, err := f.e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}})
		if err != nil || ack.GetAdmissionAck().State != "queued" {
			t.Fatalf("admission: %v %v", ack, err)
		}
		f.e.execute(t.Context(), job{cluster: f.epoch.ClusterId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.ClusterId]})
		record, err := f.e.journal.Get(command.CommandId)
		if err != nil || record.State != journal.Terminal || record.Result.State != "succeeded" {
			t.Fatalf("step %d journal: %+v %v", step, record, err)
		}
	}
	f.e.options.InspectionTimeout = 10 * time.Second
	f.e.collectObservations(t.Context())
	observed := f.e.Observations()
	if len(observed) != 1 || observed[0].ResourceId != command.Input.ResourceId || observed[0].Health != "healthy" {
		t.Fatalf("generic observation: %v", observed)
	}
}
