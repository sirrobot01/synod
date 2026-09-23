package docker

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestPatroniPhaseRequiresCurrentNativeProof(t *testing.T) {
	p := &agentv1.EnsureContainer{Engine: "postgres", Postgres: &agentv1.PostgresContainerConfig{Patroni: &agentv1.PatroniContainerConfig{}}}
	encoded, err := proto.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(encoded)
	started := time.Now().Add(-time.Minute).UTC()
	for name, change := range map[string]func(*container.InspectResponse){
		"valid":           func(*container.InspectResponse) {},
		"prior_start":     func(i *container.InspectResponse) { i.State.Health.Log[0].Start = started.Add(-time.Second) },
		"unknown":         func(i *container.InspectResponse) { i.State.Health.Log[0].Output = "patroni_startup_unknown" },
		"extra_output":    func(i *container.InspectResponse) { i.State.Health.Log[0].Output = "patroni_ready\n" },
		"failed_exit":     func(i *container.InspectResponse) { i.State.Health.Log[0].ExitCode = 1 },
		"unhealthy":       func(i *container.InspectResponse) { i.State.Health.Status = container.Unhealthy },
		"restart":         func(i *container.InspectResponse) { i.State.Restarting = true },
		"missing_health":  func(i *container.InspectResponse) { i.State.Health = nil },
		"changed_health":  func(i *container.InspectResponse) { i.Config.Healthcheck.Test = []string{"CMD", "true"} },
		"changed_command": func(i *container.InspectResponse) { i.Config.Cmd = []string{"postgres"} },
		"changed_tmpfs":   func(i *container.InspectResponse) { i.HostConfig.Tmpfs = nil },
		"changed_hash":    func(i *container.InspectResponse) { i.Config.Labels[labelHash] = "invalid" },
	} {
		t.Run(name, func(t *testing.T) {
			item := container.InspectResponse{
				Config:     &container.Config{Labels: map[string]string{labelInstallation: uuid.NewString(), labelCluster: uuid.NewString(), labelResource: uuid.NewString(), labelConfig: base64.RawStdEncoding.EncodeToString(encoded), labelHash: hex.EncodeToString(hash[:])}, Cmd: postgres.PatroniCommand(), Healthcheck: patroniHealthcheck()},
				HostConfig: &container.HostConfig{Tmpfs: map[string]string{postgres.PatroniDirectory: patroniTmpfs}},
				State:      &container.State{Running: true, StartedAt: started.Format(time.RFC3339Nano), Health: &container.Health{Status: container.Healthy, Log: []*container.HealthcheckResult{{Start: started.Add(time.Second), End: started.Add(2 * time.Second), Output: "patroni_ready"}}}},
			}
			change(&item)
			phase, err := postgresPhase(item)
			if (err == nil) != (name == "valid") || err == nil && phase != "patroni_ready" {
				t.Fatal("incorrect native startup proof", phase, err)
			}
		})
	}
}
