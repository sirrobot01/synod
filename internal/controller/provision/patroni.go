package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/coordinator"
	"github.com/sirrobot01/synod/internal/engine"
	enginepostgres "github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// PatroniConfig holds what a Patroni create needs beyond a standalone one.
type PatroniConfig struct {
	Accounts *coordinator.PatroniAccounts
	CA       *pki.CA
	// EtcdTrust is the CA bundle members use to verify the etcd server.
	EtcdTrust    []byte
	DCSEndpoints []string
	// Image is the Patroni member image. With more than one placement architecture, it is
	// an image index that covers each one.
	Image string
}

// patroniMembers is fixed for topology patroni: one elected primary and two replicas.
const patroniMembers = 3

// patroniLeafLifetime is the longest leaf the native CA issues. TASK-162 renews it.
const patroniLeafLifetime = 30 * 24 * time.Hour

// patroniMember is one member's identity and placement.
type patroniMember struct {
	resources
	node     uuid.UUID
	address  netip.Addr
	database uint16
	rest     uint16
	// The pooler and router ports. Routing child 4 starts them.
	pooler, writer, reader uint16
}

// routePorts are the named ports each member reserves beside its database and REST ports.
var routePorts = []string{"pooler", "writer", "reader"}

// provisionPatroni plans three Patroni members and installs the plan with its native credentials.
//
// The DCS account password changes on every planning attempt. A plan that is already
// installed holds the password it was created with, so a repeat must not reach etcd.
func (p *Planner) provisionPatroni(ctx context.Context, claim operation.Claim, found cluster.Cluster, combination engine.Combination) (postgres.PlacementRecord, error) {
	native := p.config.Patroni
	if native == nil {
		return postgres.PlacementRecord{}, errors.New("Patroni HA is not configured on this controller")
	}
	op, err := p.config.Store.GetOperation(ctx, claim.InstallationID, claim.OperationID)
	if err != nil {
		return postgres.PlacementRecord{}, err
	}
	if op.PlanVersion != 0 {
		return p.config.Store.ReadPlacement(ctx, claim.InstallationID, claim.OperationID)
	}
	members := make([]patroniMember, patroniMembers)
	for index := range members {
		members[index].resources = resources{instance: uuid.New(), volume: uuid.New(), reservationID: uuid.New()}
	}
	request, decision, snapshot, err := p.decide(ctx, p.config.Profile.Architectures, func(architecture string) placement.Request {
		return p.patroniRequest(claim, found, combination, members, architecture)
	})
	if err != nil {
		return postgres.PlacementRecord{}, err
	}
	if err := bindPatroniMembers(members, decision, snapshot); err != nil {
		return postgres.PlacementRecord{}, err
	}
	for _, member := range members {
		if err := p.config.Store.RecordVolumeIdentity(ctx, postgres.VolumeIdentity{
			InstallationID: claim.InstallationID, NodeID: member.node, ClusterID: claim.ClusterID,
			ResourceID: member.volume, Engine: string(combination.Engine), EngineVersion: combination.Version,
			DataPath: dataPath(found.Spec.Engine), CapacityRequestBytes: p.config.Budget.DiskBytes, Retention: postgres.Retained,
		}); err != nil {
			return postgres.PlacementRecord{}, err
		}
	}
	intent, dcsPassword, err := p.patroniIntent(found, members)
	if err != nil {
		return postgres.PlacementRecord{}, err
	}
	plan, err := p.patroniPlan(claim, found, members, intent)
	if err != nil {
		return postgres.PlacementRecord{}, err
	}
	if err := native.Accounts.EnsureAccount(ctx, claim.ClusterID, dcsPassword); err != nil {
		return postgres.PlacementRecord{}, err
	}
	return p.config.Store.InstallPlacementPlan(ctx, claim, request, decision, plan, &intent)
}

// patroniRequest asks for three members on distinct host failure domains.
func (p *Planner) patroniRequest(claim operation.Claim, found cluster.Cluster, combination engine.Combination, members []patroniMember, architecture string) placement.Request {
	budget := p.config.Budget
	// A replica copy keeps its archives beside the extracted data, so each member reserves
	// one more disk budget as the copy allowance.
	allowance := placement.Vector{DiskBytes: budget.DiskBytes}
	empty := placement.Vector{}
	request := placement.Request{
		Version: 1, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID,
		OperationID: claim.OperationID, DesiredRevision: found.DesiredRevision, Profile: "ha",
		PortRange: p.config.EndpointPorts,
	}
	group := placement.Group{ID: "patroni", DomainKey: "host"}
	for _, member := range members {
		request.Resources = append(request.Resources, placement.Resource{
			ID: member.instance, ReservationID: member.reservationID,
			// The member reservation owns its pooler and router, so a member credential serves them.
			OwnedResourceIDs: []uuid.UUID{member.instance, member.volume, routeID(member.instance, "pooler"), routeID(member.instance, "router")},
			Kind:             "instance", Role: "patroni",
			Engine: string(combination.Engine), Version: combination.Version,
			Architecture: architecture, RuntimeVersion: p.config.Profile.RuntimeVersion, StorageID: p.config.Profile.StorageID,
			Budgets: placement.Budgets{Workload: &budget, Supporting: &allowance, Backup: &empty},
			Ports: []placement.Port{
				{Address: wildcardAddress, Transport: "tcp", Name: "database"},
				{Address: wildcardAddress, Transport: "tcp", Name: "rest"},
				{Address: wildcardAddress, Transport: "tcp", Name: "pooler"},
				{Address: wildcardAddress, Transport: "tcp", Name: "writer"},
				{Address: wildcardAddress, Transport: "tcp", Name: "reader"},
			},
		})
		group.Members = append(group.Members, member.instance)
	}
	request.Groups = []placement.Group{group}
	return request
}

// bindPatroniMembers reads each member's node, client address, and named ports from the decision.
func bindPatroniMembers(members []patroniMember, decision placement.Decision, snapshot placement.Snapshot) error {
	if len(decision.Assignments) != len(members) {
		return fmt.Errorf("%w: placement returned %d assignments", placement.ErrUnsatisfied, len(decision.Assignments))
	}
	addresses := make(map[uuid.UUID]string, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		if node.Policy != nil {
			addresses[node.ID] = node.Policy.ClientAddress
		}
	}
	for index := range members {
		member := &members[index]
		for _, assignment := range decision.Assignments {
			if assignment.ResourceID != member.instance {
				continue
			}
			member.node = assignment.NodeID
			for _, port := range assignment.Ports {
				switch port.Name {
				case "database":
					member.database = port.Number
				case "rest":
					member.rest = port.Number
				case "pooler":
					member.pooler = port.Number
				case "writer":
					member.writer = port.Number
				case "reader":
					member.reader = port.Number
				}
			}
		}
		address, err := netip.ParseAddr(addresses[member.node])
		if err != nil || !address.IsGlobalUnicast() || address.Zone() != "" {
			return fmt.Errorf("%w: Patroni node %s declares no routable client address", placement.ErrUnsatisfied, member.node)
		}
		if member.database == 0 || member.rest == 0 || member.pooler == 0 || member.writer == 0 || member.reader == 0 {
			return fmt.Errorf("%w: placement bound no database, REST, pooler, writer, and reader ports", placement.ErrUnsatisfied)
		}
		member.address = address
	}
	return nil
}

// patroniIntent creates the eight native credential purposes and seals them. It also
// returns the DCS password, which the controller writes to etcd.
func (p *Planner) patroniIntent(found cluster.Cluster, members []patroniMember) (postgres.ClusterIntent, string, error) {
	native := p.config.Patroni
	intent := postgres.ClusterIntent{InstallationID: found.InstallationID, ClusterID: found.ID,
		Databases: []postgres.Database{{Name: defaultDatabase, OwnerRole: "synod_app"}}}
	seal := func(purpose, role string, member uuid.UUID, value []byte, expires time.Time) error {
		credential := postgres.Credential{CredentialID: uuid.New(), MemberID: member, Version: 1, Purpose: purpose, RoleName: role, ExpiresAt: expires}
		envelope, err := p.config.Keyring.Seal(secret.Binding{
			InstallationID: found.InstallationID, OwnerType: "cluster", OwnerID: found.ID,
			CredentialID: credential.CredentialID, Version: credential.Version,
		}, value)
		if err != nil {
			return err
		}
		credential.Envelope = envelope
		intent.Credentials = append(intent.Credentials, credential)
		return nil
	}
	var dcsPassword string
	for _, password := range []struct{ purpose, role string }{
		{postgres.SuperuserCredential, superuserRole}, {postgres.ApplicationCredential, "synod_app"},
		{postgres.ReplicationCredential, "synod_repl"}, {postgres.PatroniRewindCredential, "synod_rewind"},
		{postgres.PatroniDCSCredential, "synod_patroni_" + found.ID.String()}, {postgres.PatroniRESTCredential, "synod_patroni"},
	} {
		value, err := secret.NewPassword()
		if err != nil {
			return postgres.ClusterIntent{}, "", err
		}
		if password.purpose == postgres.PatroniDCSCredential {
			dcsPassword = value
		}
		if err := seal(password.purpose, password.role, uuid.Nil, []byte(value), time.Time{}); err != nil {
			return postgres.ClusterIntent{}, "", err
		}
	}
	now := time.Now()
	dcs, err := enginepostgres.IssuePatroniDCSMaterial(native.CA, pki.PatroniIdentity{InstallationID: found.InstallationID, ClusterID: found.ID}, native.EtcdTrust, now, patroniLeafLifetime)
	if err != nil {
		return postgres.ClusterIntent{}, "", err
	}
	encoded, err := json.Marshal(dcs)
	if err != nil {
		return postgres.ClusterIntent{}, "", err
	}
	expires, err := dcs.Expiry()
	if err != nil {
		return postgres.ClusterIntent{}, "", err
	}
	if err := seal(postgres.PatroniDCSTLSCredential, "", uuid.Nil, encoded, expires); err != nil {
		return postgres.ClusterIntent{}, "", err
	}
	addresses := make([]netip.Addr, len(members))
	for index, member := range members {
		addresses[index] = member.address
	}
	for _, member := range members {
		endpoints, err := patroniEndpoints(found, member.address, addresses)
		if err != nil {
			return postgres.ClusterIntent{}, "", err
		}
		material, err := enginepostgres.IssuePatroniMemberMaterial(native.CA, pki.PatroniIdentity{InstallationID: found.InstallationID, ClusterID: found.ID, ResourceID: member.instance}, member.address, now, patroniLeafLifetime, endpoints...)
		if err != nil {
			return postgres.ClusterIntent{}, "", err
		}
		encoded, err := json.Marshal(material)
		if err != nil {
			return postgres.ClusterIntent{}, "", err
		}
		expires, err := material.Expiry()
		if err != nil {
			return postgres.ClusterIntent{}, "", err
		}
		if err := seal(postgres.PatroniMemberTLSCredential, "", member.instance, encoded, expires); err != nil {
			return postgres.ClusterIntent{}, "", err
		}
	}
	return intent, dcsPassword, nil
}

// patroniPlan starts member A with initialization and proves it primary. Members B and C
// start only after that proof, so Patroni finds a leader and clones instead of racing.
func (p *Planner) patroniPlan(claim operation.Claim, found cluster.Cluster, members []patroniMember, intent postgres.ClusterIntent) (*agentv1.ExecutionPlan, error) {
	native := p.config.Patroni
	references := func(member uuid.UUID) []*agentv1.SecretReference {
		var refs []*agentv1.SecretReference
		for _, credential := range intent.Credentials {
			if credential.MemberID != uuid.Nil && credential.MemberID != member {
				continue
			}
			refs = append(refs, &agentv1.SecretReference{CredentialId: credential.CredentialID.String(), Version: credential.Version, Purpose: credential.Purpose})
		}
		return refs
	}
	credential := func(purpose string) postgres.Credential {
		for _, found := range intent.Credentials {
			if found.Purpose == purpose {
				return found
			}
		}
		return postgres.Credential{}
	}
	superuser, application, replication := credential(postgres.SuperuserCredential), credential(postgres.ApplicationCredential), credential(postgres.ReplicationCredential)
	var peers []string
	for _, member := range members {
		peers = append(peers, netip.PrefixFrom(member.address, member.address.BitLen()).String())
	}
	var steps []*agentv1.PlanStep
	var primaryReady string
	var readySteps []string
	for index, member := range members {
		epoch := &agentv1.Epoch{
			InstallationId: claim.InstallationID.String(), NodeId: member.node.String(),
			ClusterId: claim.ClusterID.String(), RecoveryGeneration: claim.RecoveryGeneration.String(),
			ExecutionEpoch: claim.ExecutionEpoch,
		}
		volume, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
			ResourceId: member.volume.String(),
			Payload:    &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: member.volume.String()}},
		}, agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS, nil, beforeExecution)
		if err != nil {
			return nil, err
		}
		refs := references(member.instance)
		configuration := &agentv1.EnsureContainer{
			ImageDigest: native.Image, Engine: string(found.Spec.Engine), VolumeId: member.volume.String(),
			NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST,
			MemoryBytes: p.config.Budget.MemoryBytes, CpuMillis: p.config.Budget.CPUMillis,
			Postgres: &agentv1.PostgresContainerConfig{
				SynchronousReplication: found.Spec.PostgreSQL.ReplicationPolicy == "synchronous",
				Port:                   uint32(member.database),
				SuperuserCredentialId:  superuser.CredentialID.String(), SuperuserCredentialVersion: superuser.Version,
				ApplicationCredentialId: application.CredentialID.String(), ApplicationCredentialVersion: application.Version,
				ReplicationCredentialId: replication.CredentialID.String(), ReplicationCredentialVersion: replication.Version,
				Patroni: &agentv1.PatroniContainerConfig{
					Address: member.address.String(), RestPort: uint32(member.rest), DcsEndpoints: native.DCSEndpoints,
					PeerNetworks: peers, ClientNetworks: []string{"0.0.0.0/0", "::/0"},
					ProvisionId: uuid.NewString(), AllowInitialization: true,
					RequiredFreeBytes: p.config.Budget.DiskBytes, Credentials: refs,
				},
			},
		}
		container, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
			ResourceId: member.instance.String(), Secrets: refs,
			Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: configuration},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, []string{volume.Command.StepId}, beforeExecution)
		if err != nil {
			return nil, err
		}
		after := []string{container.Command.StepId}
		role := "primary"
		if index > 0 {
			after, role = append(after, primaryReady), "replica"
		}
		start, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
			ResourceId: member.instance.String(),
			Payload:    &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: configuration, AllowInitialization: true}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, after, beforeExecution)
		if err != nil {
			return nil, err
		}
		ready, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
			ResourceId: member.instance.String(),
			Secrets:    []*agentv1.SecretReference{{CredentialId: application.CredentialID.String(), Version: application.Version, Purpose: application.Purpose}},
			Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{
				Address: enginepostgres.ReadinessLoopback(member.address).Addr().String(), Port: uint32(member.database),
				Database: defaultDatabase, Role: application.RoleName, Engine: string(found.Spec.Engine), PostgresRole: role,
				PostgresTls: &agentv1.PostgresTLS{RootCaPem: native.CA.CertificatePEM(), ServerName: member.address.String()},
			}},
		}, agentv1.Postcondition_POSTCONDITION_ENGINE_READY, []string{start.Command.StepId}, beforeExecution)
		if err != nil {
			return nil, err
		}
		if index == 0 {
			primaryReady = ready.Command.StepId
		}
		steps = append(steps, volume, container, start, ready)
		readySteps = append(readySteps, ready.Command.StepId)
	}
	// Every member's pooler and router start after all members are ready.
	routes := make([]routeMember, len(members))
	for index, member := range members {
		routes[index] = routeMember{node: member.node, resource: member.instance, address: member.address,
			database: member.database, rest: member.rest, pooler: member.pooler, writer: member.writer, reader: member.reader}
		for _, reference := range references(member.instance) {
			if reference.Purpose == postgres.PatroniMemberTLSCredential {
				routes[index].certificate = reference
			}
		}
	}
	deploy, started, err := p.routeDeploySteps(claim, operation.Operation{DesiredRevision: found.DesiredRevision}, routes, application, readySteps)
	if err != nil {
		return nil, err
	}
	checks, err := p.routeCheckSteps(claim, operation.Operation{DesiredRevision: found.DesiredRevision}, routes, application, started)
	if err != nil {
		return nil, err
	}
	steps = append(append(steps, deploy...), checks...)
	return &agentv1.ExecutionPlan{Version: 1, Steps: steps}, nil
}

// patroniStartSteps starts every member at once with its stored configuration, so Patroni
// elects the member with the newest data. Starting one member alone could let a lagging
// member win. Each readiness step waits for every start and accepts either role.
// Completion then requires exactly one primary.
func (p *Planner) patroniStartSteps(ctx context.Context, claim operation.Claim, op operation.Operation, running []member) ([]*agentv1.PlanStep, error) {
	native := p.config.Patroni
	if native == nil {
		return nil, errors.New("Patroni HA is not configured on this controller")
	}
	application, err := p.config.Store.ReadClusterCredential(ctx, claim.InstallationID, claim.ClusterID, postgres.ApplicationCredential)
	if err != nil {
		return nil, err
	}
	containers := make([]*agentv1.EnsureContainer, len(running))
	var starts []*agentv1.PlanStep
	var started []string
	for index, each := range running {
		if each.Role != "patroni" {
			return nil, fmt.Errorf("%w: member %s has role %q in a Patroni cluster", ErrUnplannable, each.MemberID, each.Role)
		}
		container, err := p.config.Store.ReadMemberContainer(ctx, claim.InstallationID, claim.ClusterID, each.ResourceID)
		if err != nil {
			return nil, err
		}
		containers[index] = container
		start, err := p.step(claim, epochFor(claim, each.NodeID), op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(),
			Payload:    &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: container}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, nil, beforeExecution)
		if err != nil {
			return nil, err
		}
		starts = append(starts, start)
		started = append(started, start.Command.StepId)
	}
	steps := starts
	var readySteps []string
	for index, each := range running {
		ready, err := p.patroniReadiness(claim, op, application, each, containers[index], "any", started)
		if err != nil {
			return nil, err
		}
		steps = append(steps, ready)
		readySteps = append(readySteps, ready.Command.StepId)
	}
	if !routed(running) {
		return steps, nil
	}
	// The routes start after the members are ready, then prove both routes on every node.
	routes := make([]routeMember, len(running))
	for index, each := range running {
		if routes[index], err = routeMemberFrom(each, containers[index]); err != nil {
			return nil, err
		}
	}
	var routeStarts []string
	for _, each := range routes {
		previous := readySteps
		for _, kind := range []string{"pooler", "router"} {
			start, err := p.step(claim, epochFor(claim, each.node), op.DesiredRevision, &agentv1.CommandInput{
				ResourceId: routeID(each.resource, kind).String(), Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}},
			}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, previous, beforeExecution)
			if err != nil {
				return nil, err
			}
			steps = append(steps, start)
			previous = []string{start.Command.StepId}
		}
		routeStarts = append(routeStarts, previous...)
	}
	checks, err := p.routeCheckSteps(claim, op, routes, application, routeStarts)
	if err != nil {
		return nil, err
	}
	return append(steps, checks...), nil
}

// patroniReadiness checks one member over its loopback listener as the application role.
// Role any accepts either role, because Patroni elects the primary, and completion counts
// writers. A switchover names the role it expects.
func (p *Planner) patroniReadiness(claim operation.Claim, op operation.Operation, application postgres.Credential, each member, container *agentv1.EnsureContainer, role string, after []string) (*agentv1.PlanStep, error) {
	address, err := netip.ParseAddr(container.GetPostgres().GetPatroni().GetAddress())
	if err != nil {
		return nil, fmt.Errorf("%w: member %s stores no Patroni address", ErrUnplannable, each.MemberID)
	}
	return p.step(claim, epochFor(claim, each.NodeID), op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: each.ResourceID.String(),
		Secrets:    []*agentv1.SecretReference{{CredentialId: application.CredentialID.String(), Version: application.Version, Purpose: application.Purpose}},
		Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{
			Address: enginepostgres.ReadinessLoopback(address).Addr().String(), Port: container.Postgres.Port,
			Database: defaultDatabase, Role: application.RoleName, Engine: string(cluster.PostgreSQL), PostgresRole: role,
			PostgresTls: &agentv1.PostgresTLS{RootCaPem: p.config.Patroni.CA.CertificatePEM(), ServerName: address.String()},
		}},
	}, agentv1.Postcondition_POSTCONDITION_ENGINE_READY, after, beforeExecution)
}

// patroniEndpoints lists the other addresses a member leaf names. A client can reach the
// router on any member node, and that router can forward it to this member's pooler, so the
// leaf names every member address and the optional load balancer address.
func patroniEndpoints(found cluster.Cluster, own netip.Addr, members []netip.Addr) ([]netip.Addr, error) {
	var endpoints []netip.Addr
	for _, address := range members {
		if address != own && !slices.Contains(endpoints, address) {
			endpoints = append(endpoints, address)
		}
	}
	if endpoint := found.Spec.PostgreSQL.Endpoint; endpoint != nil {
		address, err := netip.ParseAddr(endpoint.Address)
		if err != nil {
			return nil, fmt.Errorf("%w: the load balancer address is invalid", ErrUnplannable)
		}
		if address != own && !slices.Contains(endpoints, address) {
			endpoints = append(endpoints, address)
		}
	}
	return endpoints, nil
}
