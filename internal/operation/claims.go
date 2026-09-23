package operation

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"time"

	"github.com/google/uuid"
	postgresengine "github.com/sirrobot01/synod/internal/engine/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

const ProtocolVersion uint32 = 1
const PayloadVersion uint32 = 1

var ErrClaimConflict = errors.New("claim_conflict")

var ErrStaleOwner = errors.New("stale_owner")
var ErrBarrierPending = errors.New("barrier_pending")
var ErrCommandConflict = errors.New("command_conflict")
var ErrIncompatibleProtocol = errors.New("incompatible_protocol")

// NegotiateVersion chooses the highest shared version without changing either range.
func NegotiateVersion(controller, agent *agentv1.VersionRange) (uint32, error) {
	if controller == nil || agent == nil || controller.Minimum == 0 || agent.Minimum == 0 || controller.Maximum < controller.Minimum || agent.Maximum < agent.Minimum {
		return 0, ErrIncompatibleProtocol
	}
	version := min(controller.Maximum, agent.Maximum)
	if version < max(controller.Minimum, agent.Minimum) {
		return 0, ErrIncompatibleProtocol
	}
	return version, nil
}

// NegotiateHello rejects journal formats that the controller cannot recover.
func NegotiateHello(hello *agentv1.Hello, supportedCapabilities []string) ([]string, error) {
	if hello == nil {
		return nil, ErrIncompatibleProtocol
	}
	for _, id := range []string{hello.InstallationId, hello.NodeId, hello.BootId, hello.JournalGeneration} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil {
			return nil, ErrIncompatibleProtocol
		}
	}
	if _, err := NegotiateVersion(&agentv1.VersionRange{Minimum: ProtocolVersion, Maximum: ProtocolVersion}, hello.Protocol); err != nil {
		return nil, err
	}
	for _, version := range hello.ActivePayloadVersions {
		if version != PayloadVersion {
			return nil, ErrIncompatibleProtocol
		}
	}
	capabilities := make([]string, 0, len(hello.Capabilities))
	for _, capability := range hello.Capabilities {
		if slices.Contains(supportedCapabilities, capability) && !slices.Contains(capabilities, capability) {
			capabilities = append(capabilities, capability)
		}
	}
	slices.Sort(capabilities)
	return capabilities, nil
}

var imageDigestPattern = regexp.MustCompile(`^.+@sha256:[0-9a-f]{64}$`)

// CommandHash covers immutable identities and typed input. It excludes authority and tracing.
// New ownership reuses command IDs and input hashes with a higher execution epoch.
func CommandHash(command *agentv1.Command) ([32]byte, error) {
	if command == nil || command.Epoch == nil || command.Input == nil {
		return [32]byte{}, fmt.Errorf("command envelope is incomplete")
	}
	for _, id := range []string{command.Epoch.InstallationId, command.Epoch.NodeId, command.Epoch.ClusterId, command.Epoch.RecoveryGeneration, command.OperationId, command.StepId, command.CommandId, command.Input.ResourceId} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil {
			return [32]byte{}, fmt.Errorf("command has an invalid identity")
		}
		if parsed.String() != id {
			return [32]byte{}, fmt.Errorf("command identities must use canonical UUID strings")
		}
	}
	if command.Epoch.ExecutionEpoch < 1 || command.DesiredRevision < 1 || command.PlanVersion < 1 || command.Input.PayloadVersion != PayloadVersion {
		return [32]byte{}, fmt.Errorf("command has an unsupported revision or version")
	}
	if command.Input.Timeout == nil || command.Input.Timeout.CheckValid() != nil || command.Input.Timeout.AsDuration() < time.Second || command.Input.Timeout.AsDuration() > time.Hour {
		return [32]byte{}, fmt.Errorf("command timeout must be between one second and one hour")
	}
	for _, secret := range command.Input.Secrets {
		if secret == nil {
			return [32]byte{}, fmt.Errorf("secret reference is absent")
		}
		id, err := uuid.Parse(secret.CredentialId)
		if err != nil || id == uuid.Nil || secret.Version < 1 || secret.Purpose == "" {
			return [32]byte{}, fmt.Errorf("secret reference is invalid")
		}
	}
	switch payload := command.Input.Payload.(type) {
	case *agentv1.CommandInput_EnsureVolume:
		if payload.EnsureVolume == nil || payload.EnsureVolume.VolumeId != command.Input.ResourceId {
			return [32]byte{}, fmt.Errorf("volume identity must match resource")
		}
	case *agentv1.CommandInput_EnsureNetwork:
		if payload.EnsureNetwork == nil || payload.EnsureNetwork.NetworkId != command.Input.ResourceId {
			return [32]byte{}, fmt.Errorf("network identity must match resource")
		}
	case *agentv1.CommandInput_EnsureContainer:
		p := payload.EnsureContainer
		if p == nil || !imageDigestPattern.MatchString(p.ImageDigest) || (p.Engine != "postgres" && p.Engine != "redis") || p.MemoryBytes < 0 || p.CpuMillis < 0 {
			return [32]byte{}, fmt.Errorf("container input is invalid")
		}
		if p.Engine == "redis" && p.Redis == nil {
			return [32]byte{}, fmt.Errorf("redis container settings are required")
		}
		if p.Engine == "postgres" && p.Postgres == nil {
			return [32]byte{}, fmt.Errorf("postgres container settings are required")
		}
		if config := p.Postgres; config != nil {
			if p.Engine != "postgres" || len(p.PlainEnv) != 0 {
				return [32]byte{}, fmt.Errorf("postgres container settings are invalid")
			}
			if err := validatePostgresConfiguration(config, command.Input.Secrets); err != nil {
				return [32]byte{}, err
			}
			if config.Patroni != nil {
				if len(p.SecretEnv) != 0 {
					return [32]byte{}, fmt.Errorf("Patroni cannot use credential environment entries")
				}
				if _, err := postgresengine.PatroniMemberSettings(p, command.Epoch.InstallationId, command.Epoch.ClusterId, command.Input.ResourceId); err != nil {
					return [32]byte{}, err
				}
			} else {
				passwordName, passwordPurpose := "POSTGRES_PASSWORD", "superuser"
				if config.ReplicaSource != nil {
					if config.ReplicaSource.ResourceId == command.Input.ResourceId {
						return [32]byte{}, fmt.Errorf("postgres replica source equals target")
					}
					passwordName, passwordPurpose = "SYNOD_REPL_PASSWORD", "replication"
				}
				if len(p.SecretEnv) != 3 || !slices.ContainsFunc(p.SecretEnv, func(env *agentv1.SecretEnv) bool {
					return env != nil && env.Name == passwordName && env.Purpose == passwordPurpose
				}) || !slices.ContainsFunc(p.SecretEnv, func(env *agentv1.SecretEnv) bool {
					return env != nil && env.Name == "SYNOD_APP_VERIFIER" && env.Purpose == "application"
				}) || !slices.ContainsFunc(p.SecretEnv, func(env *agentv1.SecretEnv) bool {
					return env != nil && env.Name == "SYNOD_REPL_VERIFIER" && env.Purpose == "replication"
				}) {
					return [32]byte{}, fmt.Errorf("postgres credential environment is invalid")
				}
			}
		}
		if p.Redis != nil {
			if err := ValidateRedisContainer(p, command.Input.Secrets); err != nil {
				return [32]byte{}, err
			}
		}
		volumeID, err := uuid.Parse(p.VolumeId)
		if err != nil || volumeID == uuid.Nil {
			return [32]byte{}, fmt.Errorf("container volume reference is invalid")
		}
		switch p.NetworkMode {
		case agentv1.NetworkMode_NETWORK_MODE_HOST:
			if p.NetworkId != "" || len(p.Ports) != 0 {
				return [32]byte{}, fmt.Errorf("host networking requires no network ID or published ports")
			}
		case agentv1.NetworkMode_NETWORK_MODE_BRIDGE:
			networkID, err := uuid.Parse(p.NetworkId)
			if err != nil || networkID == uuid.Nil {
				return [32]byte{}, fmt.Errorf("bridge network reference is invalid")
			}
		default:
			return [32]byte{}, fmt.Errorf("container network mode is required")
		}
		for _, port := range p.Ports {
			if port == nil || port.HostPort < 1 || port.HostPort > 65535 || port.ContainerPort < 1 || port.ContainerPort > 65535 || (port.Transport != "tcp" && port.Transport != "udp") {
				return [32]byte{}, fmt.Errorf("container port is invalid")
			}
		}
	case *agentv1.CommandInput_RecoverPostgresSetup:
		if payload.RecoverPostgresSetup == nil || payload.RecoverPostgresSetup.Postgres == nil || payload.RecoverPostgresSetup.Postgres.ReplicaSource != nil || payload.RecoverPostgresSetup.Postgres.Patroni != nil {
			return [32]byte{}, fmt.Errorf("postgres recovery input is absent")
		}
		if err := validatePostgresConfiguration(payload.RecoverPostgresSetup.Postgres, command.Input.Secrets); err != nil {
			return [32]byte{}, err
		}
	case *agentv1.CommandInput_PreparePostgresReplica:
		if payload.PreparePostgresReplica == nil || len(command.Input.Secrets) != 0 {
			return [32]byte{}, fmt.Errorf("postgres replica preparation input is invalid")
		}
	case *agentv1.CommandInput_ClonePostgresReplica:
		p := payload.ClonePostgresReplica
		if p == nil || p.Postgres == nil || p.Postgres.ReplicaSource == nil || p.Postgres.ReplicaSource.ResourceId == command.Input.ResourceId || p.RequiredFreeBytes < 1 || p.RequiredFreeBytes > 1<<50 {
			return [32]byte{}, fmt.Errorf("postgres clone input is invalid")
		}
		if err := validatePostgresConfiguration(p.Postgres, command.Input.Secrets); err != nil {
			return [32]byte{}, err
		}
	case *agentv1.CommandInput_VerifyPostgresReplicaClone:
		p := payload.VerifyPostgresReplicaClone
		if err := validatePostgresCloneVerification(command, p.GetPostgres(), p.GetCloneCommandId()); err != nil {
			return [32]byte{}, err
		}
	case *agentv1.CommandInput_VerifyPostgresReplicaFailure:
		p := payload.VerifyPostgresReplicaFailure
		if err := validatePostgresCloneVerification(command, p.GetPostgres(), p.GetCloneCommandId()); err != nil {
			return [32]byte{}, err
		}
	case *agentv1.CommandInput_StartPatroniMember:
		p := payload.StartPatroniMember
		if p == nil || p.Container == nil || p.Container.Postgres == nil || p.Container.Postgres.Patroni == nil || len(command.Input.Secrets) != 0 || p.AllowInitialization && !p.Container.Postgres.Patroni.AllowInitialization {
			return [32]byte{}, fmt.Errorf("Patroni start input is invalid")
		}
		validation := proto.Clone(command).(*agentv1.Command)
		validation.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: p.Container}
		validation.Input.Secrets = p.Container.Postgres.Patroni.Credentials
		if _, err := CommandHash(validation); err != nil {
			return [32]byte{}, err
		}
	case *agentv1.CommandInput_StartContainer:
		if payload.StartContainer == nil {
			return [32]byte{}, fmt.Errorf("start input is absent")
		}
	case *agentv1.CommandInput_StopContainer:
		if payload.StopContainer == nil || payload.StopContainer.GracePeriod == nil || payload.StopContainer.GracePeriod.CheckValid() != nil || payload.StopContainer.GracePeriod.AsDuration() < 0 || payload.StopContainer.GracePeriod.AsDuration() > command.Input.Timeout.AsDuration() {
			return [32]byte{}, fmt.Errorf("stop grace period is invalid")
		}
	case *agentv1.CommandInput_RemoveContainer:
		if payload.RemoveContainer == nil {
			return [32]byte{}, fmt.Errorf("remove input is absent")
		}
	case *agentv1.CommandInput_VerifyContainerAbsent:
		if payload.VerifyContainerAbsent == nil || len(command.Input.Secrets) != 0 {
			return [32]byte{}, fmt.Errorf("container absence input is invalid")
		}
	case *agentv1.CommandInput_InspectContainer:
		if payload.InspectContainer == nil {
			return [32]byte{}, fmt.Errorf("inspect input is absent")
		}
	case *agentv1.CommandInput_CheckReadiness:
		p := payload.CheckReadiness
		// A loopback address is what this profile publishes. A readiness check that
		// can be pointed anywhere is a request to connect to anywhere.
		if p == nil || p.Port == 0 || p.Port > 65535 || p.Database == "" || p.Role == "" ||
			(p.Engine != "postgres" && p.Engine != "redis") {
			return [32]byte{}, fmt.Errorf("readiness input is invalid")
		}
		if (p.Engine == "postgres" && p.PostgresRole != "primary" && p.PostgresRole != "replica") || (p.Engine != "postgres" && p.PostgresRole != "") {
			return [32]byte{}, fmt.Errorf("postgres readiness requires its native role only")
		}
		address, err := netip.ParseAddr(p.Address)
		if err != nil || !address.IsLoopback() {
			return [32]byte{}, fmt.Errorf("readiness address must be loopback")
		}
		if settings := p.PostgresTls; settings != nil {
			if p.Engine != "postgres" {
				return [32]byte{}, fmt.Errorf("TLS readiness settings require postgres")
			}
			if _, err := postgresengine.ReadinessTLSConfig(settings.RootCaPem, settings.ServerName); err != nil {
				return [32]byte{}, err
			}
		}
		if check := p.PostgresReplication; check != nil {
			if p.Engine != "postgres" || p.Database != "postgres" || p.Role != "synod_admin" || p.RedisReplication != nil || len(command.Input.Secrets) != 1 || command.Input.Secrets[0].Purpose != "superuser" {
				return [32]byte{}, fmt.Errorf("postgres replication check requires the admin credential")
			}
			if p.PostgresRole == "primary" {
				if check.Source != nil || check.MaxReplayLagBytes != 0 || check.MaxMessageAge != nil {
					return [32]byte{}, fmt.Errorf("primary identity check has replica settings")
				}
			} else {
				source := check.Source
				if source == nil || source.Port == 0 || source.Port > 65535 || source.ResourceId == command.Input.ResourceId || check.MaxReplayLagBytes < 1 || check.MaxReplayLagBytes > 1<<30 || check.MaxMessageAge == nil || check.MaxMessageAge.CheckValid() != nil || check.MaxMessageAge.AsDuration() < time.Second || check.MaxMessageAge.AsDuration() > 120*time.Second {
					return [32]byte{}, fmt.Errorf("postgres replication check is invalid")
				}
				address, err := netip.ParseAddr(source.Address)
				id, idErr := uuid.Parse(source.ResourceId)
				if err != nil || address.String() != source.Address || address.Zone() != "" || (!address.IsGlobalUnicast() && !address.IsLoopback()) || idErr != nil || id == uuid.Nil || id.String() != source.ResourceId {
					return [32]byte{}, fmt.Errorf("postgres replication source is invalid")
				}
			}
		}
		if replication := p.RedisReplication; replication != nil {
			if p.Engine != "redis" || p.Database != "0" || p.Role != "synod_admin" ||
				len(command.Input.Secrets) != 1 || command.Input.Secrets[0].Purpose != "superuser" {
				return [32]byte{}, fmt.Errorf("redis replication check requires the admin credential")
			}
			switch replication.ExpectedRole {
			case "primary":
				if replication.Upstream != nil {
					return [32]byte{}, fmt.Errorf("redis primary check must not name an upstream")
				}
			case "replica":
				if !validRedisUpstream(replication.Upstream) {
					return [32]byte{}, fmt.Errorf("redis replica check requires a valid upstream")
				}
			default:
				return [32]byte{}, fmt.Errorf("redis replication check requires an expected role")
			}
		}
	default:
		return [32]byte{}, fmt.Errorf("command payload is unsupported")
	}
	immutable := proto.Clone(command).(*agentv1.Command)
	immutable.Epoch.RecoveryGeneration = ""
	immutable.Epoch.ExecutionEpoch = 0
	immutable.InputHash = nil
	immutable.TraceId = ""
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(immutable)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

// ValidateCommand checks the hash before an agent writes durable admission.
func ValidateCommand(command *agentv1.Command) error {
	hash, err := CommandHash(command)
	if err != nil {
		return err
	}
	if !bytes.Equal(command.InputHash, hash[:]) {
		return ErrCommandConflict
	}
	return nil
}

// CheckEpoch rejects delayed commands after local admission has closed.
// The journal owner must persist the prepared epoch before calling this for new work.
func CheckEpoch(command, activated *agentv1.Epoch) error {
	if command == nil || activated == nil || command.ExecutionEpoch < 1 || !proto.Equal(command, activated) {
		return ErrStaleOwner
	}
	return nil
}

// BarrierDrained does not claim that reported residual effects are reconciled.
func BarrierDrained(result *agentv1.BarrierResult) error {
	if result == nil || result.Epoch == nil || !result.AdmissionsClosed || !result.Drained || result.FailureCode != "" {
		return ErrBarrierPending
	}
	for _, effect := range result.Effects {
		if effect == nil || effect.Active || effect.OutcomeUnknown {
			return ErrBarrierPending
		}
	}
	return nil
}

// Claim is durable ownership of one operation by one worker.
// ExecutionEpoch increases on every takeover, and every later update must match it.
type Claim struct {
	ID                 uuid.UUID
	InstallationID     uuid.UUID
	ClusterID          uuid.UUID
	OperationID        uuid.UUID
	RecoveryGeneration uuid.UUID
	OwnerID            string
	ExecutionEpoch     int64
	ExpiresAt          time.Time
	Active             bool
	AffectedNodes      []uuid.UUID
}
