package operation

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strings"
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

// PatroniMemberContainer returns the stored container configuration that a typed Patroni
// member command repeats, or nil for any other payload.
func PatroniMemberContainer(input *agentv1.CommandInput) *agentv1.EnsureContainer {
	switch payload := input.GetPayload().(type) {
	case *agentv1.CommandInput_StartPatroniMember:
		return payload.StartPatroniMember.GetContainer()
	case *agentv1.CommandInput_VerifyPatroniStartFailure:
		return payload.VerifyPatroniStartFailure.GetContainer()
	case *agentv1.CommandInput_VerifyPatroniMemberNotServing:
		return payload.VerifyPatroniMemberNotServing.GetContainer()
	case *agentv1.CommandInput_VerifyPatroniMemberFailed:
		return payload.VerifyPatroniMemberFailed.GetContainer()
	case *agentv1.CommandInput_RecreatePatroniMember:
		return payload.RecreatePatroniMember.GetNext()
	}
	return nil
}

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
		if p.Route != nil {
			if err := validateRouteContainer(p, command.Input.Secrets); err != nil {
				return [32]byte{}, err
			}
			break
		}
		if p.Etcd != nil {
			if err := validateEtcdContainer(p, command.Input.Secrets); err != nil {
				return [32]byte{}, err
			}
			break
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
				if len(p.SecretEnv) != 2 || !slices.ContainsFunc(p.SecretEnv, func(env *agentv1.SecretEnv) bool {
					return env != nil && env.Name == "POSTGRES_PASSWORD" && env.Purpose == "superuser"
				}) || !slices.ContainsFunc(p.SecretEnv, func(env *agentv1.SecretEnv) bool {
					return env != nil && env.Name == "SYNOD_APP_VERIFIER" && env.Purpose == "application"
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
		if payload.RecoverPostgresSetup == nil || payload.RecoverPostgresSetup.Postgres == nil || payload.RecoverPostgresSetup.Postgres.Patroni != nil {
			return [32]byte{}, fmt.Errorf("postgres recovery input is absent")
		}
		if err := validatePostgresConfiguration(payload.RecoverPostgresSetup.Postgres, command.Input.Secrets); err != nil {
			return [32]byte{}, err
		}
	case *agentv1.CommandInput_StartPatroniMember, *agentv1.CommandInput_VerifyPatroniStartFailure,
		*agentv1.CommandInput_VerifyPatroniMemberNotServing, *agentv1.CommandInput_VerifyPatroniMemberFailed:
		start := command.Input.GetStartPatroniMember()
		stored := PatroniMemberContainer(command.Input)
		if stored.GetPostgres().GetPatroni() == nil || len(command.Input.Secrets) != 0 || start.GetAllowInitialization() && !stored.Postgres.Patroni.AllowInitialization {
			return [32]byte{}, fmt.Errorf("Patroni member input is invalid")
		}
		validation := proto.Clone(command).(*agentv1.Command)
		validation.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: stored}
		validation.Input.Secrets = stored.Postgres.Patroni.Credentials
		if _, err := CommandHash(validation); err != nil {
			return [32]byte{}, err
		}
	case *agentv1.CommandInput_RecreatePatroniMember:
		if err := validatePatroniRecreate(command); err != nil {
			return [32]byte{}, err
		}
	case *agentv1.CommandInput_SwitchoverPatroni:
		if err := validateSwitchover(payload.SwitchoverPatroni, command.Input.Secrets); err != nil {
			return [32]byte{}, err
		}
	case *agentv1.CommandInput_EtcdMembership:
		if err := validateEtcdMembership(payload.EtcdMembership, command.Input.Secrets); err != nil {
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
		if (p.Engine == "postgres" && p.PostgresRole != "primary" && p.PostgresRole != "replica" && p.PostgresRole != "any") || (p.Engine != "postgres" && p.PostgresRole != "") {
			return [32]byte{}, fmt.Errorf("postgres readiness requires its native role only")
		}
		// Role any serves Patroni, whose members elect their own roles over TLS.
		if p.PostgresRole == "any" && p.PostgresTls == nil {
			return [32]byte{}, fmt.Errorf("postgres readiness for any role requires TLS")
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

// renewedPurposes are the Patroni credentials whose certificates a recreate may renew.
var renewedPurposes = []string{"patroni_member_tls", "patroni_dcs_tls"}

// validatePatroniRecreate allows a recreate to change only the versions of the two
// certificate credentials and to append peer networks for an added member. Each credential
// keeps its ID and moves to a higher version. The command carries the next credential
// references, and both configurations must be valid member configurations on their own.
func validatePatroniRecreate(command *agentv1.Command) error {
	p := command.Input.GetRecreatePatroniMember()
	previous, next := p.GetPrevious(), p.GetNext()
	if previous.GetPostgres().GetPatroni() == nil || next.GetPostgres().GetPatroni() == nil {
		return fmt.Errorf("Patroni recreate input is invalid")
	}
	before, after := previous.Postgres.Patroni.Credentials, next.Postgres.Patroni.Credentials
	if len(before) != len(after) || !slices.EqualFunc(command.Input.Secrets, after, func(a, b *agentv1.SecretReference) bool { return proto.Equal(a, b) }) {
		return fmt.Errorf("Patroni recreate credentials are invalid")
	}
	renewed := 0
	for index, old := range before {
		current := after[index]
		if old.Purpose != current.Purpose || old.CredentialId != current.CredentialId {
			return fmt.Errorf("Patroni recreate credentials are invalid")
		}
		if old.Version == current.Version {
			continue
		}
		if !slices.Contains(renewedPurposes, current.Purpose) || current.Version < old.Version {
			return fmt.Errorf("Patroni recreate changes a credential it cannot renew")
		}
		renewed++
	}
	unchanged := proto.Clone(next).(*agentv1.EnsureContainer)
	unchanged.Postgres.Patroni.Credentials = before
	peers := previous.Postgres.Patroni.PeerNetworks
	if len(unchanged.Postgres.Patroni.PeerNetworks) < len(peers) || !slices.Equal(unchanged.Postgres.Patroni.PeerNetworks[:len(peers)], peers) {
		return fmt.Errorf("Patroni recreate removes or reorders peer networks")
	}
	unchanged.Postgres.Patroni.PeerNetworks = peers
	if renewed == 0 || !proto.Equal(unchanged, previous) {
		return fmt.Errorf("Patroni recreate changes more than certificate versions")
	}
	for _, stored := range []*agentv1.EnsureContainer{previous, next} {
		validation := proto.Clone(command).(*agentv1.Command)
		validation.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: stored}
		validation.Input.Secrets = stored.Postgres.Patroni.Credentials
		if _, err := CommandHash(validation); err != nil {
			return err
		}
	}
	return nil
}

// validateSwitchover checks a switchover. The agent dials the member REST API on the member
// address, so the address is a canonical unicast IP. It needs the REST password and the
// member certificate, and nothing else.
func validateSwitchover(p *agentv1.SwitchoverPatroni, secrets []*agentv1.SecretReference) error {
	address, err := netip.ParseAddr(p.GetAddress())
	if err != nil || address.String() != p.Address || address.Zone() != "" || !address.IsGlobalUnicast() || p.RestPort == 0 || p.RestPort > 65535 {
		return fmt.Errorf("Patroni switchover target is invalid")
	}
	purposes := make([]string, 0, len(secrets))
	for _, reference := range secrets {
		purposes = append(purposes, reference.Purpose)
	}
	slices.Sort(purposes)
	if !slices.Equal(purposes, []string{"patroni_member_tls", "patroni_rest"}) {
		return fmt.Errorf("Patroni switchover credentials are invalid")
	}
	return nil
}

// validateEtcdMembership checks an etcd membership change. The agent dials the local etcd
// member on its canonical unicast address with that member's leaf and nothing else. A new
// learner names an HTTPS peer URL on a canonical unicast address. The other actions name no URL.
func validateEtcdMembership(p *agentv1.EtcdMembership, secrets []*agentv1.SecretReference) error {
	address, err := netip.ParseAddr(p.GetAddress())
	if err != nil || address.String() != p.Address || address.Zone() != "" || !address.IsGlobalUnicast() || p.ClientPort == 0 || p.ClientPort > 65535 ||
		p.Name == "" || len(p.Name) > 128 || strings.ContainsFunc(p.Name, func(r rune) bool { return r < '!' || r > '~' }) {
		return fmt.Errorf("etcd membership target is invalid")
	}
	switch p.Action {
	case "add_learner":
		peer, err := url.Parse(p.PeerUrl)
		if err != nil || peer.Scheme != "https" || peer.User != nil || peer.Path != "" || peer.RawQuery != "" || peer.Fragment != "" {
			return fmt.Errorf("etcd learner peer URL is invalid")
		}
		host, err := netip.ParseAddrPort(peer.Host)
		if err != nil || !host.Addr().IsGlobalUnicast() || host.Addr().Zone() != "" || host.Port() == 0 || peer.Host != host.String() {
			return fmt.Errorf("etcd learner peer URL is invalid")
		}
	case "promote", "remove":
		if p.PeerUrl != "" {
			return fmt.Errorf("etcd %s names no peer URL", p.Action)
		}
	default:
		return fmt.Errorf("etcd membership action is invalid")
	}
	if len(secrets) != 1 || secrets[0].Purpose != "patroni_member_tls" {
		return fmt.Errorf("etcd membership credentials are invalid")
	}
	return nil
}

// validateRouteContainer checks a pooler or router beside a Patroni member. It runs on host
// networking with no volume and no environment. Its secrets are exactly the credentials its
// kind needs, and the command carries the same references.
func validateRouteContainer(p *agentv1.EnsureContainer, secrets []*agentv1.SecretReference) error {
	if p.Engine != "postgres" || p.Postgres != nil || p.Redis != nil || p.VolumeId != "" || p.NetworkId != "" ||
		p.NetworkMode != agentv1.NetworkMode_NETWORK_MODE_HOST || len(p.Ports) != 0 || len(p.SecretEnv) != 0 || len(p.PlainEnv) != 0 {
		return fmt.Errorf("route container shape is invalid")
	}
	_, _, purposes, err := postgresengine.RouteSettings(p.Route)
	if err != nil {
		return err
	}
	credentials := p.Route.Credentials
	if len(credentials) != len(purposes) || !slices.EqualFunc(credentials, secrets, func(a, b *agentv1.SecretReference) bool { return proto.Equal(a, b) }) {
		return fmt.Errorf("route container credentials are invalid")
	}
	for _, purpose := range purposes {
		if !slices.ContainsFunc(credentials, func(reference *agentv1.SecretReference) bool { return reference.GetPurpose() == purpose }) {
			return fmt.Errorf("route container needs a %s credential", purpose)
		}
	}
	return nil
}

// validateEtcdContainer checks an etcd member beside a Patroni member. It runs on host
// networking with an isolated volume and no environment. It requires patroni_member_tls.
func validateEtcdContainer(p *agentv1.EnsureContainer, secrets []*agentv1.SecretReference) error {
	if p.Engine != "postgres" || p.Postgres != nil || p.Redis != nil || p.Route != nil || p.VolumeId == "" || p.NetworkId != "" ||
		p.NetworkMode != agentv1.NetworkMode_NETWORK_MODE_HOST || len(p.Ports) != 0 || len(p.SecretEnv) != 0 || len(p.PlainEnv) != 0 {
		return fmt.Errorf("etcd container shape is invalid")
	}
	volumeID, err := uuid.Parse(p.VolumeId)
	if err != nil || volumeID == uuid.Nil {
		return fmt.Errorf("etcd container volume reference is invalid")
	}
	c := p.Etcd
	if c == nil || c.Name == "" || c.InitialCluster == "" {
		return fmt.Errorf("etcd container configuration is incomplete")
	}
	if c.InitialClusterState != "" && c.InitialClusterState != "new" && c.InitialClusterState != "existing" {
		return fmt.Errorf("etcd initial cluster state is invalid")
	}
	clusterID, err := uuid.Parse(c.ClusterId)
	if err != nil || clusterID == uuid.Nil {
		return fmt.Errorf("etcd cluster identity is invalid")
	}
	address, err := netip.ParseAddr(c.Address)
	if err != nil || !address.IsGlobalUnicast() || address.Zone() != "" || c.ClientPort == 0 || c.ClientPort > 65535 || c.PeerPort == 0 || c.PeerPort > 65535 || c.ClientPort == c.PeerPort {
		return fmt.Errorf("etcd container endpoints are invalid")
	}
	if len(c.Credentials) != 1 || len(secrets) != 1 || !proto.Equal(c.Credentials[0], secrets[0]) || c.Credentials[0].Purpose != "patroni_member_tls" {
		return fmt.Errorf("etcd container credentials are invalid")
	}
	return nil
}
