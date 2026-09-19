package operation

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/google/uuid"
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
	case *agentv1.CommandInput_InspectContainer:
		if payload.InspectContainer == nil {
			return [32]byte{}, fmt.Errorf("inspect input is absent")
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
