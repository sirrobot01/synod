package docker

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

const postgresTmpfs = "rw,noexec,nosuid,nodev,size=1m"

func patroniHealthcheck() *container.HealthConfig {
	return &container.HealthConfig{Test: append([]string{"CMD"}, postgres.PatroniHealthCommand()...), Interval: 2 * time.Second, Timeout: 25 * time.Second, Retries: 1}
}

func postgresHealthcheck(replica bool) *container.HealthConfig {
	phases := ""
	if replica {
		phases = `clone_pending) printf clone_pending; exit 0;; cloned) printf cloned; exit 0;; `
	}
	return &container.HealthConfig{
		Test:     []string{"CMD-SHELL", `phase=$(cat /run/synod-postgres/bootstrap-complete 2>/dev/null) || exit 1; case "$phase" in ` + phases + `ready) printf ready; exit 0;; recovered) printf recovered; exit 0;; failed) printf failed; exit 1;; *) exit 1;; esac`},
		Interval: time.Second, Timeout: time.Second, Retries: 1,
	}
}

// postgresPhase proves that startup SQL stopped before a running container can drain.
func postgresPhase(item container.InspectResponse) (string, error) {
	if item.Config == nil || item.State == nil {
		return "bootstrap_unknown", runtime.ErrOutcomeUnknown
	}
	encoded, err := base64.RawStdEncoding.DecodeString(item.Config.Labels[labelConfig])
	if err != nil {
		return "bootstrap_unknown", &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	var stored agentv1.EnsureContainer
	if err := proto.Unmarshal(encoded, &stored); err != nil {
		return "bootstrap_unknown", &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if stored.Engine != "postgres" {
		return "", nil
	}
	if !item.State.Running && !item.State.Restarting {
		return "", nil
	}
	if stored.Postgres == nil {
		return "bootstrap_unknown", runtime.ErrOutcomeUnknown
	}
	health := postgresHealthcheck(stored.Postgres.ReplicaSource != nil)
	tmpfs := map[string]string{postgres.ConfigDirectory: postgresTmpfs}
	if stored.Postgres.Patroni != nil {
		health = patroniHealthcheck()
		tmpfs = map[string]string{postgres.PatroniDirectory: patroniTmpfs}
	}
	program, programErr := postgresCommand(stored.Postgres, item.Config.Labels[labelInstallation], item.Config.Labels[labelCluster], item.Config.Labels[labelResource])
	hash := sha256.Sum256(encoded)
	if programErr != nil || stored.Postgres == nil || item.HostConfig == nil || item.Config.Labels[labelHash] != hex.EncodeToString(hash[:]) ||
		!reflect.DeepEqual(item.Config.Healthcheck, health) ||
		!maps.Equal(item.HostConfig.Tmpfs, tmpfs) ||
		!slices.Equal(item.Config.Cmd, program) {
		return "bootstrap_unknown", runtime.ErrOutcomeUnknown
	}
	if item.State.Restarting || item.State.Dead || item.State.Health == nil || len(item.State.Health.Log) == 0 {
		return "bootstrap_pending", runtime.ErrOutcomeUnknown
	}
	started, err := time.Parse(time.RFC3339Nano, item.State.StartedAt)
	if err != nil {
		return "bootstrap_unknown", &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if started.IsZero() {
		return "bootstrap_unknown", runtime.ErrOutcomeUnknown
	}
	latest := item.State.Health.Log[len(item.State.Health.Log)-1]
	if latest == nil || latest.Start.Before(started) || latest.End.Before(latest.Start) {
		return "bootstrap_unknown", runtime.ErrOutcomeUnknown
	}
	if stored.Postgres.Patroni != nil {
		if item.State.Health.Status == container.Healthy && latest.ExitCode == 0 && latest.Output == "patroni_ready" {
			return "patroni_ready", nil
		}
		return "patroni_startup_unknown", runtime.ErrOutcomeUnknown
	}
	switch {
	case stored.Postgres.ReplicaSource != nil && item.State.Health.Status == container.Healthy && latest.ExitCode == 0 && latest.Output == "clone_pending":
		return "replica_prepared", nil
	case stored.Postgres.ReplicaSource != nil && item.State.Health.Status == container.Healthy && latest.ExitCode == 0 && latest.Output == "cloned":
		return "replica_cloned", nil
	case item.State.Health.Status == container.Healthy && latest.ExitCode == 0 && latest.Output == "ready":
		return "bootstrap_ready", nil
	case item.State.Health.Status == container.Healthy && latest.ExitCode == 0 && latest.Output == "recovered":
		return "bootstrap_recovered", nil
	case item.State.Health.Status == container.Unhealthy && latest.ExitCode == 1 && latest.Output == "failed":
		return "bootstrap_failed", nil
	default:
		return "bootstrap_pending", runtime.ErrOutcomeUnknown
	}
}

// postgresCommand selects the fixed startup program from the persisted topology.
func postgresCommand(config *agentv1.PostgresContainerConfig, installation, cluster, resource string) ([]string, error) {
	if config == nil {
		return nil, runtime.ErrConfigurationConflict
	}
	if config.Patroni != nil {
		if config.ReplicaSource != nil {
			return nil, runtime.ErrConfigurationConflict
		}
		return postgres.PatroniCommand(), nil
	}
	if source := config.ReplicaSource; source != nil {
		if source.Port > 65535 {
			return nil, runtime.ErrConfigurationConflict
		}
		return postgres.ReplicaCommand(config, postgres.ReplicaSource{Address: source.Address, Port: uint16(source.Port), ResourceID: source.ResourceId}, installation, cluster, resource)
	}
	return postgres.ContainerCommand(config, installation, cluster, resource), nil
}
