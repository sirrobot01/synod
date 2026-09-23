// Package provision turns an admitted cluster into a plan a node can run. It produces
// and installs a plan and executes nothing: specification 06 keeps planning separate
// from side-effect execution, and the worker owns dispatch.
package provision

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/engine"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	// ErrNoImage reports a combination with no pinned image. Specification 36 forbids
	// a moving latest tag, so it is refused rather than resolved at run time.
	ErrNoImage = errors.New("no_pinned_image")
)

// Decider refreshes capacity evidence and plans on what nodes actually answered.
// *capacity.Driver satisfies it.
type Decider interface {
	Decide(ctx context.Context, request placement.Request) (placement.Decision, placement.Snapshot, error)
}

// Image pins one engine version to one digest on one architecture.
type Image struct {
	Engine       cluster.Engine
	Version      string
	Architecture string
	Digest       string
}

// Profile is the node shape this installation places onto. A cluster spec names none
// of it and placement requires all three to match a node's policy, so it is
// configuration rather than a guess taken from whichever nodes happen to be up.
type Profile struct {
	Architecture   string
	RuntimeVersion string
	StorageID      string
}

// Config binds one planner.
type Config struct {
	Store   *postgres.Store
	Decider Decider
	Catalog *engine.Catalog
	Keyring *secret.Keyring
	Profile Profile
	Images  []Image
	// Budget is the resource amount one instance reserves.
	Budget placement.Vector
	// StepTimeout bounds one command on a node.
	StepTimeout time.Duration
	// StopGrace is how long an engine is given to shut down cleanly before the
	// runtime stops waiting. A database killed part-way leaves recovery work.
	StopGrace time.Duration
	// EndpointAddress is where a client reaches the engine, and EndpointPorts is the
	// span of host ports placement chooses one from. The readiness check connects to
	// exactly the chosen binding rather than to the container.
	EndpointAddress string
	EndpointPorts   placement.PortRange
}

// Planner produces and installs provisioning plans.
type Planner struct{ config Config }

// New checks the planner inputs.
func New(config Config) (*Planner, error) {
	if config.Store == nil || config.Decider == nil || config.Catalog == nil || config.Keyring == nil {
		return nil, errors.New("provision planner requires a store, a decider, a catalog, and a keyring")
	}
	if config.Profile.Architecture == "" || config.Profile.RuntimeVersion == "" || config.Profile.StorageID == "" {
		return nil, errors.New("provision planner requires a complete node profile")
	}
	if len(config.Images) == 0 {
		return nil, errors.New("provision planner requires at least one pinned image")
	}
	if config.StepTimeout == 0 {
		config.StepTimeout = 5 * time.Minute
	}
	if config.StepTimeout < time.Second || config.StepTimeout > time.Hour {
		return nil, errors.New("invalid provision step timeout")
	}
	if config.StopGrace == 0 {
		config.StopGrace = 30 * time.Second
	}
	if config.StopGrace < time.Second || config.StopGrace > 10*time.Minute {
		return nil, errors.New("invalid provision stop grace period")
	}
	if config.EndpointAddress == "" {
		config.EndpointAddress = "127.0.0.1"
	}
	if config.EndpointPorts.First == 0 || config.EndpointPorts.First > config.EndpointPorts.Last {
		return nil, errors.New("provision planner requires the port range it publishes on")
	}
	if !config.Budget.Valid() || config.Budget.CPUMillis == 0 || config.Budget.MemoryBytes == 0 || config.Budget.DiskBytes == 0 {
		return nil, errors.New("provision planner requires a complete budget")
	}
	return &Planner{config: config}, nil
}

// provision plans one admitted cluster and installs the plan with its reservations.
//
// The order is the contract: credentials first, because specification 07 requires the
// intent persisted before dispatch; then placement, because a plan naming a node is
// worthless without the reservation holding it; then both in one transaction.
func (p *Planner) provision(ctx context.Context, claim operation.Claim) (postgres.PlacementRecord, error) {
	found, err := p.config.Store.GetCluster(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return postgres.PlacementRecord{}, err
	}
	capabilities, err := p.config.Catalog.Resolve(found.Spec)
	if err != nil {
		return postgres.PlacementRecord{}, err
	}
	if found.Spec.Engine == cluster.Redis && found.Spec.Redis.MaxmemoryBytes > p.config.Budget.MemoryBytes/4*3 {
		return postgres.PlacementRecord{}, errors.New("redis maxmemory exceeds three quarters of the container memory")
	}
	digest, err := p.image(capabilities.Combination)
	if err != nil {
		return postgres.PlacementRecord{}, err
	}
	intent, err := p.intent(ctx, found)
	if err != nil {
		return postgres.PlacementRecord{}, err
	}
	// Before dispatch, and before anything that could dispatch. The store enforces
	// the same rule, so this is the order rather than the only guard.
	if err := p.config.Store.RecordClusterIntent(ctx, intent); err != nil {
		return postgres.PlacementRecord{}, err
	}
	resources := resources{instance: uuid.New(), volume: uuid.New(), reservationID: uuid.New()}
	request := p.request(claim, found, capabilities.Combination, resources)
	decision, _, err := p.config.Decider.Decide(ctx, request)
	if err != nil {
		return postgres.PlacementRecord{}, err
	}
	if len(decision.Assignments) != 1 {
		return postgres.PlacementRecord{}, fmt.Errorf("%w: placement returned %d assignments", placement.ErrUnsatisfied, len(decision.Assignments))
	}
	nodeID := decision.Assignments[0].NodeID
	bound := decision.Assignments[0].Ports
	if len(bound) != 1 {
		return postgres.PlacementRecord{}, fmt.Errorf("%w: placement bound %d ports", placement.ErrUnsatisfied, len(bound))
	}
	// The storage is declared before the plan that creates it. Specification 33 needs
	// this to verify the volume before an engine starts, and a delete needs it to
	// count what it retained. Declaring it after the install would leave a plan whose
	// storage nothing can account for if the process stopped in between.
	if err := p.config.Store.RecordVolumeIdentity(ctx, postgres.VolumeIdentity{
		InstallationID: claim.InstallationID, NodeID: nodeID, ClusterID: claim.ClusterID,
		ResourceID: resources.volume, Engine: string(capabilities.Combination.Engine),
		EngineVersion: capabilities.Combination.Version, DataPath: dataPath(found.Spec.Engine),
		CapacityRequestBytes: p.config.Budget.DiskBytes, Retention: postgres.Retained,
	}); err != nil {
		return postgres.PlacementRecord{}, err
	}
	plan, err := p.plan(claim, found, resources, nodeID, bound[0].Number, digest, intent, nil)
	if err != nil {
		return postgres.PlacementRecord{}, err
	}
	return p.config.Store.InstallPlacementPlan(ctx, claim, request, decision, plan, nil)
}

// resources are generated once and reused by the request and the plan, because a plan
// naming a resource the reservation does not cover is one nothing accounted for.
type resources struct {
	instance      uuid.UUID
	volume        uuid.UUID
	reservationID uuid.UUID
}

// request asks placement for one instance and the storage it owns.
func (p *Planner) request(claim operation.Claim, found cluster.Cluster, combination engine.Combination, r resources) placement.Request {
	budget := p.config.Budget
	empty := placement.Vector{}
	return placement.Request{
		Version: 1, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID,
		OperationID: claim.OperationID, DesiredRevision: found.DesiredRevision, Profile: "compact",
		PortRange: p.config.EndpointPorts,
		Resources: []placement.Resource{{
			ID: r.instance, ReservationID: r.reservationID,
			// The volume is owned by the instance's reservation. Storage that is
			// reserved separately can outlive the thing that needs it.
			OwnedResourceIDs: []uuid.UUID{r.instance, r.volume},
			Kind:             "instance",
			// A create makes the cluster's first member, which is its primary.
			Role:           "primary",
			Engine:         string(combination.Engine),
			Version:        combination.Version,
			Architecture:   p.config.Profile.Architecture,
			RuntimeVersion: p.config.Profile.RuntimeVersion,
			StorageID:      p.config.Profile.StorageID,
			Budgets: placement.Budgets{
				Workload: &budget, Supporting: &empty, Backup: &empty,
			},
			// This profile publishes on the host's own network, so the engine's port
			// is a host resource and has to be reserved like one. The number is left
			// to placement, which knows every port already held on every node.
			Ports: []placement.Port{{Address: wildcardAddress, Transport: "tcp"}},
		}},
	}
}

// plan builds the typed, versioned steps that create the cluster. Each step names what
// it depends on, so the ordering is declared rather than implied by list position.
// An upstream makes the new instance a replica of that peer. A nil upstream builds the
// cluster's primary, which is what a create does.
func (p *Planner) plan(claim operation.Claim, found cluster.Cluster, r resources, nodeID uuid.UUID, port uint16, digest string, intent postgres.ClusterIntent, upstream *agentv1.RedisUpstream) (*agentv1.ExecutionPlan, error) {
	epoch := &agentv1.Epoch{
		InstallationId: claim.InstallationID.String(), NodeId: nodeID.String(),
		ClusterId: claim.ClusterID.String(), RecoveryGeneration: claim.RecoveryGeneration.String(),
		ExecutionEpoch: claim.ExecutionEpoch,
	}
	volume, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
		ResourceId: r.volume.String(),
		Payload:    &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: r.volume.String()}},
	}, agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS, nil, beforeExecution)
	if err != nil {
		return nil, err
	}
	if len(intent.Credentials) != 3 || intent.Credentials[0].Purpose != postgres.SuperuserCredential || intent.Credentials[1].Purpose != postgres.ApplicationCredential {
		return nil, errors.New("the recorded intent does not contain the required credentials")
	}
	superuser, application := intent.Credentials[0], intent.Credentials[1]
	credentials := []*agentv1.SecretReference{
		{CredentialId: superuser.CredentialID.String(), Version: superuser.Version, Purpose: superuser.Purpose},
		{CredentialId: application.CredentialID.String(), Version: application.Version, Purpose: application.Purpose},
	}
	replication := intent.Credentials[2]
	if replication.Purpose != postgres.ReplicationCredential {
		return nil, errors.New("the recorded intent does not contain the replication credential")
	}
	credentials = append(credentials, &agentv1.SecretReference{
		CredentialId: replication.CredentialID.String(), Version: replication.Version, Purpose: replication.Purpose,
	})
	configuration := &agentv1.EnsureContainer{
		ImageDigest: digest, Engine: string(found.Spec.Engine), VolumeId: r.volume.String(),
		NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST,
		MemoryBytes: p.config.Budget.MemoryBytes, CpuMillis: p.config.Budget.CPUMillis,
		SecretEnv: []*agentv1.SecretEnv{
			{Name: "POSTGRES_PASSWORD", Purpose: superuser.Purpose},
			{Name: "SYNOD_APP_VERIFIER", Purpose: application.Purpose},
			{Name: "SYNOD_REPL_VERIFIER", Purpose: replication.Purpose},
		},
	}
	probeCredential, database := application, defaultDatabase
	if found.Spec.Engine == cluster.Redis {
		configuration.SecretEnv = []*agentv1.SecretEnv{
			{Name: "REDIS_ADMIN_PASSWORD", Purpose: superuser.Purpose},
			{Name: "REDIS_APP_PASSWORD", Purpose: application.Purpose},
			{Name: "REDIS_REPLICATION_PASSWORD", Purpose: replication.Purpose},
		}
		maxmemory := found.Spec.Redis.MaxmemoryBytes
		if maxmemory == 0 {
			maxmemory = p.config.Budget.MemoryBytes / 4 * 3
		}
		policy := found.Spec.Redis.EvictionPolicy
		if policy == "" {
			policy = "noeviction"
		}
		configuration.Redis = &agentv1.RedisContainerConfig{Port: uint32(port), Persistence: found.Spec.Redis.Persistence, MaxmemoryBytes: maxmemory, EvictionPolicy: policy, Credentials: credentials, Upstream: upstream}
		probeCredential, database = application, "0"
		if upstream != nil {
			// Reading the replication role and link is an administrative check. The
			// application role cannot run it, so the replica's readiness uses the
			// superuser credential instead.
			probeCredential = superuser
		}
	} else {
		configuration.Postgres = &agentv1.PostgresContainerConfig{
			SynchronousReplication: found.Spec.PostgreSQL.ReplicationPolicy == "synchronous",
			Port:                   uint32(port),
			SuperuserCredentialId:  superuser.CredentialID.String(), SuperuserCredentialVersion: superuser.Version,
			ApplicationCredentialId: application.CredentialID.String(), ApplicationCredentialVersion: application.Version,
			ReplicationCredentialId: replication.CredentialID.String(), ReplicationCredentialVersion: replication.Version,
		}
	}
	container, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
		ResourceId: r.instance.String(),
		Secrets:    credentials,
		Payload:    &agentv1.CommandInput_EnsureContainer{EnsureContainer: configuration},
	}, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, []string{volume.Command.StepId}, beforeExecution)
	if err != nil {
		return nil, err
	}
	start, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
		ResourceId: r.instance.String(),
		Payload:    &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}},
	}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, []string{container.Command.StepId}, beforeExecution)
	if err != nil {
		return nil, err
	}
	// A running container is not a ready database. Specification 07 requires checking
	// access through the published endpoint before success.
	postgresRole := ""
	if found.Spec.Engine == cluster.PostgreSQL {
		postgresRole = "primary"
	}
	ready, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
		ResourceId: r.instance.String(),
		Secrets: []*agentv1.SecretReference{{
			CredentialId: probeCredential.CredentialID.String(), Version: probeCredential.Version,
			Purpose: probeCredential.Purpose,
		}},
		Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{
			Address: p.config.EndpointAddress, Port: uint32(port),
			Database: database, Role: probeCredential.RoleName, Engine: string(found.Spec.Engine), PostgresRole: postgresRole,
			// A replica is not ready when it answers. It is ready when it holds the
			// declared role, is linked to its declared upstream, and has finished its
			// first sync. Without this a fresh empty replica reads as healthy.
			RedisReplication: replicationCheck(upstream),
		}},
	}, agentv1.Postcondition_POSTCONDITION_ENGINE_READY, []string{start.Command.StepId}, beforeExecution)
	if err != nil {
		return nil, err
	}
	return &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{volume, container, start, ready}}, nil
}

// replicationCheck asks the prober to prove a replica's role and its link. A primary
// carries no upstream and needs no such proof in this slice.
func replicationCheck(upstream *agentv1.RedisUpstream) *agentv1.RedisReplicationCheck {
	if upstream == nil {
		return nil
	}
	return &agentv1.RedisReplicationCheck{ExpectedRole: "replica", Upstream: upstream}
}

// step builds one plan step and seals its input hash.
func (p *Planner) step(claim operation.Claim, epoch *agentv1.Epoch, revision int64,
	input *agentv1.CommandInput, postcondition agentv1.Postcondition, dependsOn []string,
	boundary agentv1.CancellationBoundary) (*agentv1.PlanStep, error) {
	input.PayloadVersion = operation.PayloadVersion
	input.Timeout = durationpb.New(p.config.StepTimeout)
	command := &agentv1.Command{
		Epoch: epoch, OperationId: claim.OperationID.String(),
		StepId: uuid.NewString(), CommandId: uuid.NewString(),
		DesiredRevision: revision, PlanVersion: 1, Input: input,
	}
	hash, err := operation.CommandHash(command)
	if err != nil {
		return nil, err
	}
	command.InputHash = hash[:]
	return &agentv1.PlanStep{
		Command: command, DependsOn: dependsOn, MaxAttempts: 3,
		CancellationBoundary: boundary, Postcondition: postcondition,
	}, nil
}

// intent is the credentials and databases this cluster is meant to have. One already
// recorded is reused, never replaced: planning is retried whenever an earlier attempt
// stopped before the plan was installed, and a fresh password would both configure the
// engine differently and collide with what the first attempt recorded.
func (p *Planner) intent(ctx context.Context, found cluster.Cluster) (postgres.ClusterIntent, error) {
	intent := postgres.ClusterIntent{InstallationID: found.InstallationID, ClusterID: found.ID}
	purposes := []string{postgres.SuperuserCredential, postgres.ApplicationCredential, postgres.ReplicationCredential}
	for _, purpose := range purposes {
		existing, err := p.config.Store.ReadClusterCredential(ctx, found.InstallationID, found.ID, purpose)
		if err == nil {
			intent.Credentials = append(intent.Credentials, existing)
			continue
		}
		if missing, ok := errors.AsType[*postgres.AdmissionError](err); !ok || missing.Code != postgres.NotFound {
			return postgres.ClusterIntent{}, err
		}
		password, err := secret.NewPassword()
		if err != nil {
			return postgres.ClusterIntent{}, err
		}
		credential := postgres.Credential{
			CredentialID: uuid.New(), Version: 1, Purpose: purpose, RoleName: superuserRole,
		}
		switch purpose {
		case postgres.ApplicationCredential:
			credential.RoleName = "synod_app"
		case postgres.ReplicationCredential:
			credential.RoleName = "synod_repl"
		}
		binding := secret.Binding{
			InstallationID: found.InstallationID, OwnerType: "cluster", OwnerID: found.ID,
			CredentialID: credential.CredentialID, Version: credential.Version,
		}
		envelope, err := p.config.Keyring.Seal(binding, []byte(password))
		if err != nil {
			return postgres.ClusterIntent{}, err
		}
		credential.Envelope = envelope
		intent.Credentials = append(intent.Credentials, credential)
	}
	if found.Spec.Engine == cluster.PostgreSQL {
		intent.Databases = []postgres.Database{{Name: defaultDatabase, OwnerRole: "synod_app"}}
	}
	return intent, nil
}

// image answers the pinned digest for one combination.
func (p *Planner) image(combination engine.Combination) (string, error) {
	for _, pinned := range p.config.Images {
		if pinned.Engine == combination.Engine && pinned.Version == combination.Version &&
			pinned.Architecture == p.config.Profile.Architecture {
			return pinned.Digest, nil
		}
	}
	return "", fmt.Errorf("%w: %s on %s", ErrNoImage, combination, p.config.Profile.Architecture)
}

// beforeExecution is the boundary for a step that creates or starts something.
// Nothing there is destructive, so nothing needs a later one.
const beforeExecution = agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION

// The role and database a standalone cluster is created with.
const (
	superuserRole   = "synod_admin"
	defaultDatabase = "app"
)

// wildcardAddress is what a host-networked engine binds. The reservation names the
// wildcard because that is what the engine actually takes, and a reservation narrower
// than the binding would let something else claim the same port.
const wildcardAddress = "0.0.0.0"

// dataPath is where each engine reads inside its container. The Docker driver mounts
// the volume at the same path, and the two must agree for the engine to find its data.
func dataPath(found cluster.Engine) string {
	if found == cluster.Redis {
		return "/data"
	}
	return "/var/lib/postgresql/data"
}

// addReplica places one more member for the cluster and plans its creation.
//
// It reuses the provisioning request and plan. The difference is the member's declared
// role and its upstream: the replica is created, started, and then proved to hold the
// replica role on a connected, synced link to the primary.
//
// The primary is not named in the request, so its reservation is untouched. Placement
// reads it from the snapshot as held capacity and places the replica around it.
func (p *Planner) addReplica(ctx context.Context, claim operation.Claim, found cluster.Cluster, primary member, upstream *agentv1.RedisUpstream, prefix []*agentv1.PlanStep) error {
	capabilities, err := p.config.Catalog.Resolve(found.Spec)
	if err != nil {
		return err
	}
	digest, err := p.image(capabilities.Combination)
	if err != nil {
		return err
	}
	// The replica reuses the cluster's credentials. Its own intent would give the
	// cluster a second superuser and a second application role.
	intent, err := p.intent(ctx, found)
	if err != nil {
		return err
	}
	added := resources{instance: uuid.New(), volume: uuid.New(), reservationID: uuid.New()}
	request := p.request(claim, found, capabilities.Combination, added)
	request.Resources[0].Role = "replica"
	diskBytes := p.config.Budget.DiskBytes
	if found.Spec.Engine == cluster.PostgreSQL {
		if diskBytes < 1 || diskBytes > (1<<50)/3 {
			return fmt.Errorf("%w: PostgreSQL clone capacity exceeds its supported bound", ErrUnplannable)
		}
		request.Resources[0].Budgets.Backup = &placement.Vector{DiskBytes: 2 * diskBytes}
		diskBytes *= 3
	}
	decision, _, err := p.config.Decider.Decide(ctx, request)
	if err != nil {
		return err
	}
	if len(decision.Assignments) != 1 {
		return fmt.Errorf("%w: placement returned %d assignments", placement.ErrUnsatisfied, len(decision.Assignments))
	}
	nodeID, bound := decision.Assignments[0].NodeID, decision.Assignments[0].Ports
	if len(bound) != 1 {
		return fmt.Errorf("%w: placement bound %d ports", placement.ErrUnsatisfied, len(bound))
	}
	var plan *agentv1.ExecutionPlan
	if found.Spec.Engine == cluster.PostgreSQL {
		plan, err = p.postgresReplicaPlan(claim, found, added, nodeID, bound[0].Number, digest, intent, primary, upstream, diskBytes)
	} else {
		plan, err = p.plan(claim, found, added, nodeID, bound[0].Number, digest, intent, upstream)
	}
	if err != nil {
		return err
	}
	if len(prefix) > 0 {
		plan.Steps[0].DependsOn = []string{prefix[len(prefix)-1].Command.StepId}
		plan.Steps = append(prefix, plan.Steps...)
	}
	if err := p.config.Store.RecordVolumeIdentity(ctx, postgres.VolumeIdentity{
		InstallationID: claim.InstallationID, NodeID: nodeID, ClusterID: claim.ClusterID,
		ResourceID: added.volume, Engine: string(capabilities.Combination.Engine),
		EngineVersion: capabilities.Combination.Version, DataPath: dataPath(found.Spec.Engine),
		CapacityRequestBytes: diskBytes, Retention: postgres.Retained,
	}); err != nil {
		return err
	}
	_, err = p.config.Store.InstallPlacementPlan(ctx, claim, request, decision, plan, nil)
	return err
}
