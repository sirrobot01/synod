package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/engine"
	enginepostgres "github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// PatroniConfig holds what a Patroni create needs beyond a standalone one.
type PatroniConfig struct {
	CA        *pki.CA
	Image     string
	EtcdImage string
}

// patroniLeafLifetime is the longest leaf the native CA issues. TASK-162 renews it.
const patroniLeafLifetime = 30 * 24 * time.Hour

// etcdBudget is what one etcd member reserves and runs with. The DCS holds only Patroni
// state, so a small process and data directory are enough.
var etcdBudget = placement.Vector{CPUMillis: 250, MemoryBytes: 256 << 20, DiskBytes: 1 << 30}

// patroniMember is one member's identity and placement.
type patroniMember struct {
	resources
	// etcd is the etcd service beside this member. A rebuilt member has none of its own,
	// because the etcd service on its node stays.
	etcd               resources
	node               uuid.UUID
	address            netip.Addr
	database           uint16
	rest               uint16
	dcsClient, dcsPeer uint16
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
	members := make([]patroniMember, operation.MinPatroniMembers)
	for index := range members {
		members[index].resources = resources{instance: uuid.New(), volume: uuid.New(), reservationID: uuid.New()}
		members[index].etcd = resources{instance: uuid.New(), volume: uuid.New(), reservationID: uuid.New()}
	}
	request, decision, snapshot, err := p.decide(ctx, p.config.Profile.Architectures, func(architecture string) placement.Request {
		request := p.patroniRequest(claim, found, combination, members, architecture)
		// Each etcd service shares its member's node and holds the DCS ports.
		for _, member := range members {
			request.Resources = append(request.Resources, placement.Resource{
				ID: member.etcd.instance, ReservationID: member.etcd.reservationID,
				OwnedResourceIDs: []uuid.UUID{member.etcd.instance, member.etcd.volume},
				Kind:             "service", Role: "etcd", Colocate: member.instance,
				Engine: string(combination.Engine), Version: combination.Version,
				Architecture: architecture, RuntimeVersion: p.config.Profile.RuntimeVersion, StorageID: p.config.Profile.StorageID,
				Budgets: placement.Budgets{Workload: &etcdBudget, Supporting: &placement.Vector{}, Backup: &placement.Vector{}},
				Ports: []placement.Port{
					{Address: wildcardAddress, Transport: "tcp", Name: "dcs_client"},
					{Address: wildcardAddress, Transport: "tcp", Name: "dcs_peer"},
				},
			})
		}
		return request
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
		if err := p.config.Store.RecordVolumeIdentity(ctx, postgres.VolumeIdentity{
			InstallationID: claim.InstallationID, NodeID: member.node, ClusterID: claim.ClusterID,
			// The etcd member belongs to the PostgreSQL cluster, so its volume carries the cluster engine.
			ResourceID: member.etcd.volume, Engine: string(combination.Engine), EngineVersion: combination.Version,
			DataPath: "/data", CapacityRequestBytes: etcdBudget.DiskBytes, Retention: postgres.Retained,
		}); err != nil {
			return postgres.PlacementRecord{}, err
		}
	}
	intent, err := p.patroniIntent(found, members)
	if err != nil {
		return postgres.PlacementRecord{}, err
	}
	plan, err := p.patroniPlan(claim, found, members, intent)
	if err != nil {
		return postgres.PlacementRecord{}, err
	}
	return p.config.Store.InstallPlacementPlan(ctx, claim, request, decision, plan, &intent)
}

// patroniRequest asks for members on distinct host failure domains. A new cluster adds
// its etcd services to this request.
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
			// The member reservation owns its pooler and router.
			OwnedResourceIDs: []uuid.UUID{
				member.instance, member.volume,
				routeID(member.instance, "pooler"), routeID(member.instance, "router"),
			},
			Kind: "instance", Role: "patroni",
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
	etcd := 0
	for _, member := range members {
		if member.etcd.instance != uuid.Nil {
			etcd++
		}
	}
	if len(decision.Assignments) != len(members)+etcd {
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
		for _, assignment := range decision.Assignments {
			if assignment.ResourceID != member.etcd.instance {
				continue
			}
			if assignment.NodeID != member.node {
				return fmt.Errorf("%w: etcd service %s left its member's node", placement.ErrUnsatisfied, member.etcd.instance)
			}
			for _, port := range assignment.Ports {
				switch port.Name {
				case "dcs_client":
					member.dcsClient = port.Number
				case "dcs_peer":
					member.dcsPeer = port.Number
				}
			}
		}
		address, err := netip.ParseAddr(addresses[member.node])
		if err != nil || !address.IsGlobalUnicast() || address.Zone() != "" {
			return fmt.Errorf("%w: Patroni node %s declares no routable client address", placement.ErrUnsatisfied, member.node)
		}
		if member.database == 0 || member.rest == 0 || member.pooler == 0 || member.writer == 0 || member.reader == 0 ||
			member.etcd.instance != uuid.Nil && (member.dcsClient == 0 || member.dcsPeer == 0) {
			return fmt.Errorf("%w: placement bound no database, REST, DCS, pooler, writer, and reader ports", placement.ErrUnsatisfied)
		}
		member.address = address
	}
	return nil
}

// patroniIntent creates the native credentials and seals them: the cluster passwords, the
// DCS client leaf, and one leaf for each member and each etcd service.
func (p *Planner) patroniIntent(found cluster.Cluster, members []patroniMember) (postgres.ClusterIntent, error) {
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
	for _, password := range []struct{ purpose, role string }{
		{postgres.SuperuserCredential, superuserRole}, {postgres.ApplicationCredential, "synod_app"},
		{postgres.ReplicationCredential, "synod_repl"}, {postgres.PatroniRewindCredential, "synod_rewind"},
		{postgres.PatroniRESTCredential, "synod_patroni"},
	} {
		value, err := secret.NewPassword()
		if err != nil {
			return postgres.ClusterIntent{}, err
		}
		if err := seal(password.purpose, password.role, uuid.Nil, []byte(value), time.Time{}); err != nil {
			return postgres.ClusterIntent{}, err
		}
	}
	now := time.Now()
	dcs, err := enginepostgres.IssuePatroniDCSMaterial(native.CA, pki.PatroniIdentity{InstallationID: found.InstallationID, ClusterID: found.ID}, native.CA.CertificatePEM(), now, patroniLeafLifetime)
	if err != nil {
		return postgres.ClusterIntent{}, err
	}
	encoded, err := json.Marshal(dcs)
	if err != nil {
		return postgres.ClusterIntent{}, err
	}
	expires, err := dcs.Expiry()
	if err != nil {
		return postgres.ClusterIntent{}, err
	}
	if err := seal(postgres.PatroniDCSTLSCredential, "", uuid.Nil, encoded, expires); err != nil {
		return postgres.ClusterIntent{}, err
	}
	addresses := make([]netip.Addr, len(members))
	for index, member := range members {
		addresses[index] = member.address
	}
	for _, member := range members {
		endpoints, err := patroniEndpoints(found, member.address, addresses)
		if err != nil {
			return postgres.ClusterIntent{}, err
		}
		material, err := enginepostgres.IssuePatroniMemberMaterial(native.CA, pki.PatroniIdentity{InstallationID: found.InstallationID, ClusterID: found.ID, ResourceID: member.instance}, member.address, now, patroniLeafLifetime, endpoints...)
		if err != nil {
			return postgres.ClusterIntent{}, err
		}
		encoded, err := json.Marshal(material)
		if err != nil {
			return postgres.ClusterIntent{}, err
		}
		expires, err := material.Expiry()
		if err != nil {
			return postgres.ClusterIntent{}, err
		}
		if err := seal(postgres.PatroniMemberTLSCredential, "", member.instance, encoded, expires); err != nil {
			return postgres.ClusterIntent{}, err
		}
		// The etcd service has its own leaf, so a member rebuild can retire the member leaf.
		etcd, err := enginepostgres.IssuePatroniMemberMaterial(native.CA, pki.PatroniIdentity{InstallationID: found.InstallationID, ClusterID: found.ID, ResourceID: member.etcd.instance}, member.address, now, patroniLeafLifetime)
		if err != nil {
			return postgres.ClusterIntent{}, err
		}
		if encoded, err = json.Marshal(etcd); err != nil {
			return postgres.ClusterIntent{}, err
		}
		if expires, err = etcd.Expiry(); err != nil {
			return postgres.ClusterIntent{}, err
		}
		if err := seal(postgres.PatroniMemberTLSCredential, "", member.etcd.instance, encoded, expires); err != nil {
			return postgres.ClusterIntent{}, err
		}
	}
	return intent, nil
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
	dcsEndpoints := make([]string, len(members))
	initialMembers := make([]string, len(members))
	for index, member := range members {
		peers = append(peers, netip.PrefixFrom(member.address, member.address.BitLen()).String())
		name := fmt.Sprintf("etcd-%d", index)
		dcsEndpoints[index] = net.JoinHostPort(member.address.String(), strconv.Itoa(int(member.dcsClient)))
		initialMembers[index] = name + "=https://" + net.JoinHostPort(member.address.String(), strconv.Itoa(int(member.dcsPeer)))
	}
	initialCluster := strings.Join(initialMembers, ",")
	var steps []*agentv1.PlanStep
	var primaryReady string
	var readySteps []string
	var startEtcdSteps []string
	type memberPlan struct {
		volume, container *agentv1.PlanStep
		configuration     *agentv1.EnsureContainer
		epoch             *agentv1.Epoch
	}
	memberPlans := make([]memberPlan, len(members))
	for index, member := range members {
		epoch := &agentv1.Epoch{
			InstallationId: claim.InstallationID.String(), NodeId: member.node.String(),
			ClusterId: claim.ClusterID.String(), RecoveryGeneration: claim.RecoveryGeneration.String(),
			ExecutionEpoch: claim.ExecutionEpoch,
		}
		volumeEtcd, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
			ResourceId: member.etcd.volume.String(),
			Payload:    &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: member.etcd.volume.String()}},
		}, agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS, nil, beforeExecution)
		if err != nil {
			return nil, err
		}
		// The etcd service presents only its own leaf.
		var etcdRefs []*agentv1.SecretReference
		for _, credential := range intent.Credentials {
			if credential.MemberID == member.etcd.instance {
				etcdRefs = append(etcdRefs, &agentv1.SecretReference{CredentialId: credential.CredentialID.String(), Version: credential.Version, Purpose: credential.Purpose})
			}
		}
		etcdConfiguration := &agentv1.EnsureContainer{
			ImageDigest: native.EtcdImage, Engine: string(cluster.PostgreSQL), VolumeId: member.etcd.volume.String(),
			NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST,
			MemoryBytes: etcdBudget.MemoryBytes, CpuMillis: etcdBudget.CPUMillis,
			Etcd: &agentv1.EtcdContainerConfig{
				Name:                fmt.Sprintf("etcd-%d", index),
				ClusterId:           claim.ClusterID.String(),
				Address:             member.address.String(),
				ClientPort:          uint32(member.dcsClient),
				PeerPort:            uint32(member.dcsPeer),
				InitialCluster:      initialCluster,
				InitialClusterState: "new",
				Credentials:         etcdRefs,
			},
		}
		containerEtcd, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
			ResourceId: member.etcd.instance.String(), Secrets: etcdRefs,
			Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: etcdConfiguration},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, []string{volumeEtcd.Command.StepId}, beforeExecution)
		if err != nil {
			return nil, err
		}
		startEtcd, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
			ResourceId: member.etcd.instance.String(),
			Payload:    &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, []string{containerEtcd.Command.StepId}, beforeExecution)
		if err != nil {
			return nil, err
		}
		startEtcdSteps = append(startEtcdSteps, startEtcd.Command.StepId)

		volume, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
			ResourceId: member.volume.String(),
			Payload:    &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: member.volume.String()}},
		}, agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS, []string{startEtcd.Command.StepId}, beforeExecution)
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
					Address: member.address.String(), RestPort: uint32(member.rest), DcsEndpoints: dcsEndpoints,
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
		memberPlans[index] = memberPlan{volume: volume, container: container, configuration: configuration, epoch: epoch}
		steps = append(steps, volumeEtcd, containerEtcd, startEtcd, volume, container)
	}

	for index, member := range members {
		mp := memberPlans[index]
		after := []string{mp.container.Command.StepId}
		role := "primary"
		if index == 0 {
			for _, stepID := range startEtcdSteps {
				if !slices.Contains(after, stepID) {
					after = append(after, stepID)
				}
			}
		} else {
			after, role = append(after, primaryReady), "replica"
		}
		start, err := p.step(claim, mp.epoch, found.DesiredRevision, &agentv1.CommandInput{
			ResourceId: member.instance.String(),
			Payload:    &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: mp.configuration, AllowInitialization: true}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, after, beforeExecution)
		if err != nil {
			return nil, err
		}
		ready, err := p.step(claim, mp.epoch, found.DesiredRevision, &agentv1.CommandInput{
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
		steps = append(steps, start, ready)
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
	// Each member starts after the etcd service on its node, so Patroni finds its DCS.
	etcd, err := p.config.Store.ReadEtcdMembers(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return nil, err
	}
	var starts []*agentv1.PlanStep
	etcdStarted := map[uuid.UUID]string{}
	for _, each := range etcd {
		start, err := p.step(claim, epochFor(claim, each.NodeID), op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(),
			Payload:    &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, nil, beforeExecution)
		if err != nil {
			return nil, err
		}
		starts, etcdStarted[each.NodeID] = append(starts, start), start.Command.StepId
	}
	containers := make([]*agentv1.EnsureContainer, len(running))
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
		var after []string
		if step, found := etcdStarted[each.NodeID]; found {
			after = []string{step}
		}
		start, err := p.step(claim, epochFor(claim, each.NodeID), op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(),
			Payload:    &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: container}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, after, beforeExecution)
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

// dcsEndpoints lists the client endpoints of the cluster's live etcd services for a new member
// configuration. Endpoints of stored that are still live keep their order, and live endpoints
// that stored lacks follow, so a configuration changes only when an etcd service moved.
func (p *Planner) dcsEndpoints(ctx context.Context, claim operation.Claim, stored []string) ([]string, error) {
	etcd, err := p.config.Store.ReadEtcdMembers(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return nil, err
	}
	var live []string
	for _, each := range etcd {
		container, err := p.config.Store.ReadEtcdContainer(ctx, claim.InstallationID, claim.ClusterID, each.ResourceID)
		if err != nil {
			return nil, err
		}
		live = append(live, net.JoinHostPort(container.Etcd.Address, strconv.Itoa(int(container.Etcd.ClientPort))))
	}
	var endpoints []string
	for _, endpoint := range stored {
		if slices.Contains(live, endpoint) {
			endpoints = append(endpoints, endpoint)
		}
	}
	for _, endpoint := range live {
		if !slices.Contains(endpoints, endpoint) {
			endpoints = append(endpoints, endpoint)
		}
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%w: the cluster holds no etcd service", ErrUnplannable)
	}
	return endpoints, nil
}

// etcdStopSteps stops each etcd service after the last step on its node. A delete also
// removes the container. The volume stays, as a member volume does.
func (p *Planner) etcdStopSteps(ctx context.Context, claim operation.Claim, op operation.Operation, remove bool, last map[uuid.UUID]string) ([]*agentv1.PlanStep, error) {
	etcd, err := p.config.Store.ReadEtcdMembers(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return nil, err
	}
	var steps []*agentv1.PlanStep
	for _, each := range etcd {
		epoch := epochFor(claim, each.NodeID)
		var after []string
		if step, found := last[each.NodeID]; found {
			after = []string{step}
		}
		stop, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(),
			Payload:    &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(p.config.StopGrace)}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, after, agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP)
		if err != nil {
			return nil, err
		}
		steps = append(steps, stop)
		if !remove {
			continue
		}
		removed, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(),
			Payload:    &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, []string{stop.Command.StepId}, beforeExecution)
		if err != nil {
			return nil, err
		}
		steps = append(steps, removed)
	}
	return steps, nil
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
