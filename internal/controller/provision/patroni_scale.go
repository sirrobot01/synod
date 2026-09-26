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
	enginepostgres "github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// addPatroniMember plans one new Patroni member on a host that no member uses.
//
// Each member admits replication only from the peer networks in its own configuration, and
// its leaf names the other member addresses that routers forward from. So each current member
// is recreated in turn with the new address and a renewed leaf, as a renewal does. Then the
// new member starts with initialization allowed, and Patroni clones it from the leader. Every
// pooler and router is replaced last, so each route reaches every member. The etcd services
// stay as they are.
func (p *Planner) addPatroniMember(ctx context.Context, claim operation.Claim, op operation.Operation, found cluster.Cluster) error {
	native := p.config.Patroni
	if native == nil {
		return errors.New("Patroni HA is not configured on this controller")
	}
	// An installed plan holds its reservation and certificates. A repeat must not make new ones.
	if op.PlanVersion != 0 {
		return nil
	}
	running, err := p.members(ctx, claim)
	if err != nil {
		return err
	}
	if len(running) != found.DesiredReplicas || len(running) < operation.MinPatroniMembers || len(running) >= operation.MaxPatroniMembers {
		return fmt.Errorf("%w: a cluster of %d members cannot grow to %d", ErrUnplannable, len(running), found.DesiredReplicas+1)
	}
	for _, each := range running {
		if each.Role != "patroni" || each.State != "active" {
			return fmt.Errorf("%w: member %s is %s %s", ErrUnplannable, each.MemberID, each.State, each.Role)
		}
	}
	application, err := p.config.Store.ReadClusterCredential(ctx, claim.InstallationID, claim.ClusterID, postgres.ApplicationCredential)
	if err != nil {
		return err
	}
	stored := make([]*agentv1.EnsureContainer, len(running))
	addresses := make([]netip.Addr, len(running))
	for index, each := range running {
		if stored[index], err = p.config.Store.ReadMemberContainer(ctx, claim.InstallationID, claim.ClusterID, each.ResourceID); err != nil {
			return err
		}
		if addresses[index], err = netip.ParseAddr(stored[index].GetPostgres().GetPatroni().GetAddress()); err != nil {
			return fmt.Errorf("%w: member %s stores no Patroni address", ErrUnplannable, each.MemberID)
		}
	}

	// The new member joins the host group beside every current member, on their architecture.
	capabilities, err := p.config.Catalog.Resolve(found.Spec)
	if err != nil {
		return err
	}
	architecture, err := p.nodeArchitecture(ctx, claim.InstallationID, running[0].NodeID)
	if err != nil {
		return err
	}
	fresh := []patroniMember{{resources: resources{instance: uuid.New(), volume: uuid.New(), reservationID: uuid.New()}}}
	request := p.patroniRequest(claim, found, capabilities.Combination, fresh, architecture)
	request.DesiredRevision = op.DesiredRevision
	for _, each := range running {
		request.Groups[0].Members = append(request.Groups[0].Members, each.ResourceID)
	}
	decision, snapshot, err := p.config.Decider.Decide(ctx, request)
	if err != nil {
		return err
	}
	if err := bindPatroniMembers(fresh, decision, snapshot); err != nil {
		return err
	}
	created := fresh[0]
	all := append(slices.Clone(addresses), created.address)

	// Each current member admits the new address and gets a leaf that names it.
	now := time.Now()
	intent := postgres.ClusterIntent{InstallationID: claim.InstallationID, ClusterID: claim.ClusterID}
	peer := netip.PrefixFrom(created.address, created.address.BitLen()).String()
	next := make([]*agentv1.EnsureContainer, len(running))
	for index, each := range running {
		endpoints, err := patroniEndpoints(found, addresses[index], all)
		if err != nil {
			return err
		}
		material, err := enginepostgres.IssuePatroniMemberMaterial(native.CA, pki.PatroniIdentity{InstallationID: claim.InstallationID, ClusterID: claim.ClusterID, ResourceID: each.ResourceID}, addresses[index], now, patroniLeafLifetime, endpoints...)
		if err != nil {
			return err
		}
		next[index] = proto.Clone(stored[index]).(*agentv1.EnsureContainer)
		patroni := next[index].Postgres.Patroni
		// A removed member's address stays among the peers, so a host that returns is not added twice.
		if !slices.Contains(patroni.PeerNetworks, peer) {
			patroni.PeerNetworks = append(patroni.PeerNetworks, peer)
		}
		for position, reference := range patroni.Credentials {
			if reference.Purpose != postgres.PatroniMemberTLSCredential {
				continue
			}
			if patroni.Credentials[position], err = p.sealVersion(ctx, claim, &intent, reference, each.ResourceID, material); err != nil {
				return err
			}
		}
	}

	// The new member has its own leaf. The rest of its configuration is the cluster's.
	endpoints, err := patroniEndpoints(found, created.address, all)
	if err != nil {
		return err
	}
	material, err := enginepostgres.IssuePatroniMemberMaterial(native.CA, pki.PatroniIdentity{InstallationID: claim.InstallationID, ClusterID: claim.ClusterID, ResourceID: created.instance}, created.address, now, patroniLeafLifetime, endpoints...)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(material)
	if err != nil {
		return err
	}
	expires, err := material.Expiry()
	if err != nil {
		return err
	}
	certificate := postgres.Credential{CredentialID: uuid.New(), MemberID: created.instance, Version: 1, Purpose: postgres.PatroniMemberTLSCredential, ExpiresAt: expires}
	certificate.Envelope, err = p.config.Keyring.Seal(secret.Binding{
		InstallationID: claim.InstallationID, OwnerType: "cluster", OwnerID: claim.ClusterID,
		CredentialID: certificate.CredentialID, Version: certificate.Version,
	}, encoded)
	if err != nil {
		return err
	}
	intent.Credentials = append(intent.Credentials, certificate)
	leaf := &agentv1.SecretReference{CredentialId: certificate.CredentialID.String(), Version: certificate.Version, Purpose: certificate.Purpose}
	configuration := proto.Clone(next[0]).(*agentv1.EnsureContainer)
	configuration.VolumeId = created.volume.String()
	configuration.Postgres.Port = uint32(created.database)
	patroni := configuration.Postgres.Patroni
	patroni.Address, patroni.RestPort, patroni.ProvisionId, patroni.AllowInitialization = created.address.String(), uint32(created.rest), uuid.NewString(), true
	if patroni.DcsEndpoints, err = p.dcsEndpoints(ctx, claim, patroni.DcsEndpoints); err != nil {
		return err
	}
	for position, reference := range patroni.Credentials {
		if reference.Purpose == postgres.PatroniMemberTLSCredential {
			patroni.Credentials[position] = leaf
		}
	}
	if err := p.config.Store.RecordVolumeIdentity(ctx, postgres.VolumeIdentity{
		InstallationID: claim.InstallationID, NodeID: created.node, ClusterID: claim.ClusterID,
		ResourceID: created.volume, Engine: string(capabilities.Combination.Engine), EngineVersion: capabilities.Combination.Version,
		DataPath: dataPath(found.Spec.Engine), CapacityRequestBytes: p.config.Budget.DiskBytes, Retention: postgres.Retained,
	}); err != nil {
		return err
	}

	steps, err := p.recreateBlocks(claim, op, application, running, stored, next)
	if err != nil {
		return err
	}
	added := member{MemberID: created.instance, NodeID: created.node, ResourceID: created.instance}
	epoch := epochFor(claim, created.node)
	volume, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: configuration.VolumeId,
		Payload:    &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: configuration.VolumeId}},
	}, agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS, []string{steps[len(steps)-1].Command.StepId}, beforeExecution)
	if err != nil {
		return err
	}
	container, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: created.instance.String(), Secrets: patroni.Credentials,
		Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: configuration},
	}, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, []string{volume.Command.StepId}, beforeExecution)
	if err != nil {
		return err
	}
	start, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: created.instance.String(),
		Payload:    &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: configuration, AllowInitialization: true}},
	}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, []string{container.Command.StepId}, beforeExecution)
	if err != nil {
		return err
	}
	ready, err := p.patroniReadiness(claim, op, application, added, configuration, "any", []string{start.Command.StepId})
	if err != nil {
		return err
	}
	steps = append(steps, volume, container, start, ready)
	// A final check of every member proves one writer among them.
	previous := []string{ready.Command.StepId}
	for index, each := range append(slices.Clone(running), added) {
		config := configuration
		if index < len(next) {
			config = next[index]
		}
		check, err := p.patroniReadiness(claim, op, application, each, config, "any", previous)
		if err != nil {
			return err
		}
		steps, previous = append(steps, check), []string{check.Command.StepId}
	}
	if routed(running) {
		routes := make([]routeMember, 0, len(running)+1)
		for index, each := range running {
			route, err := routeMemberFrom(each, next[index])
			if err != nil {
				return err
			}
			routes = append(routes, route)
		}
		routes = append(routes, routeMember{node: created.node, resource: created.instance, address: created.address,
			database: created.database, rest: created.rest, pooler: created.pooler, writer: created.writer, reader: created.reader, certificate: leaf})
		tail, err := p.replaceRoutes(claim, op, routes, application, true)
		if err != nil {
			return err
		}
		steps = append(steps, chainAfter(previous[0], tail)...)
	}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: steps}
	if err := operation.ValidatePlan(plan, op); err != nil {
		return err
	}
	_, err = p.config.Store.InstallPlacementPlan(ctx, claim, request, decision, plan, &intent)
	return err
}

// removePatroniMember plans the removal of the member that the reviewed plan names.
//
// The kept members prove readiness, then the named member proves it is a replica, so the
// leader is never removed. It stops and its container is removed. Its volume stays. A final
// check of each kept member proves one writer. Its pooler and router leave, and every other
// router is replaced so that it no longer forwards to the removed member.
func (p *Planner) removePatroniMember(ctx context.Context, claim operation.Claim, op operation.Operation, found cluster.Cluster, running []member) error {
	// An installed plan is the reviewed removal. A repeat must not plan another.
	if op.PlanVersion != 0 {
		return nil
	}
	reviewed, action, err := p.config.Store.ReadReplicaActionTarget(ctx, claim.InstallationID, claim.OperationID)
	if err != nil {
		return err
	}
	if action != "scale_down" || len(running) <= operation.MinPatroniMembers || len(running) > operation.MaxPatroniMembers {
		return ErrUnplannable
	}
	var target member
	var kept []member
	for _, each := range running {
		if each.Role != "patroni" || each.State != "active" {
			return fmt.Errorf("%w: member %s is %s %s", ErrUnplannable, each.MemberID, each.State, each.Role)
		}
		if each.MemberID == reviewed {
			target = each
		} else {
			kept = append(kept, each)
		}
	}
	if target.MemberID == uuid.Nil {
		return operation.ErrCommandConflict
	}
	application, err := p.config.Store.ReadClusterCredential(ctx, claim.InstallationID, claim.ClusterID, postgres.ApplicationCredential)
	if err != nil {
		return err
	}
	stored := make([]*agentv1.EnsureContainer, len(kept))
	var steps []*agentv1.PlanStep
	var checks []string
	for index, each := range kept {
		if stored[index], err = p.config.Store.ReadMemberContainer(ctx, claim.InstallationID, claim.ClusterID, each.ResourceID); err != nil {
			return err
		}
		check, err := p.patroniReadiness(claim, op, application, each, stored[index], "any", nil)
		if err != nil {
			return err
		}
		steps, checks = append(steps, check), append(checks, check.Command.StepId)
	}
	container, err := p.config.Store.ReadMemberContainer(ctx, claim.InstallationID, claim.ClusterID, target.ResourceID)
	if err != nil {
		return err
	}
	replica, err := p.patroniReadiness(claim, op, application, target, container, "replica", checks)
	if err != nil {
		return err
	}
	epoch := epochFor(claim, target.NodeID)
	stop, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: target.ResourceID.String(),
		Payload:    &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(p.config.StopGrace)}},
	}, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, []string{replica.Command.StepId}, agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP)
	if err != nil {
		return err
	}
	remove, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: target.ResourceID.String(),
		Payload:    &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}},
	}, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, []string{stop.Command.StepId}, beforeExecution)
	if err != nil {
		return err
	}
	steps = append(steps, replica)
	// An etcd service on the member's node moves to a kept member's node first.
	move, request, decision, intent, err := p.moveEtcd(ctx, claim, op, found, target, kept, stored, replica.Command.StepId)
	if err != nil {
		return err
	}
	if len(move) > 0 {
		stop.DependsOn = []string{move[len(move)-1].Command.StepId}
		steps = append(steps, move...)
	}
	steps = append(steps, stop, remove)
	previous := remove.Command.StepId
	for index, each := range kept {
		check, err := p.patroniReadiness(claim, op, application, each, stored[index], "any", []string{previous})
		if err != nil {
			return err
		}
		steps, previous = append(steps, check), check.Command.StepId
	}
	if routed(running) {
		routes := make([]routeMember, len(kept))
		for index, each := range kept {
			if routes[index], err = routeMemberFrom(each, stored[index]); err != nil {
				return err
			}
		}
		tail, err := p.rebuildRouteTail(claim, op, []member{target}, routes, uuid.Nil, application, previous)
		if err != nil {
			return err
		}
		steps = append(steps, tail...)
	}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: steps}
	if err := operation.ValidatePlan(plan, op); err != nil {
		return err
	}
	// A new etcd service commits its reservation and leaf with the plan. A resumed move has both.
	if intent != nil {
		_, err := p.config.Store.InstallPlacementPlan(ctx, claim, request, decision, plan, intent)
		return err
	}
	return p.config.Store.InstallPlan(ctx, claim, plan)
}

// moveEtcd plans the move of the etcd service on the target's node, if there is one, to the
// node of a kept member that holds none. The steps start after the step named after. A cluster
// with more than three etcd services resumes the move with its newest service, which an
// earlier failed removal placed, and needs no placement. Otherwise the move places a new
// service, and the returned request, decision, and intent commit it with the plan.
func (p *Planner) moveEtcd(ctx context.Context, claim operation.Claim, op operation.Operation, found cluster.Cluster, target member, kept []member, stored []*agentv1.EnsureContainer, after string) ([]*agentv1.PlanStep, placement.Request, placement.Decision, *postgres.ClusterIntent, error) {
	var none placement.Request
	etcd, err := p.config.Store.ReadEtcdMembers(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return nil, none, placement.Decision{}, nil, err
	}
	source := slices.IndexFunc(etcd, func(each postgres.EtcdMember) bool { return each.NodeID == target.NodeID })
	if source < 0 {
		return nil, none, placement.Decision{}, nil, nil
	}
	old := etcd[source]
	previous, err := p.config.Store.ReadEtcdContainer(ctx, claim.InstallationID, claim.ClusterID, old.ResourceID)
	if err != nil {
		return nil, none, placement.Decision{}, nil, err
	}
	var request placement.Request
	var decision placement.Decision
	var intent *postgres.ClusterIntent
	var fresh postgres.EtcdMember
	var container *agentv1.EnsureContainer
	if len(etcd) > operation.MinPatroniMembers {
		fresh = etcd[len(etcd)-1]
		if fresh.ResourceID == old.ResourceID || !slices.ContainsFunc(kept, func(each member) bool { return each.NodeID == fresh.NodeID }) {
			return nil, none, placement.Decision{}, nil, fmt.Errorf("%w: the cluster runs %d etcd services, and the newest is not on a kept member's node", ErrUnplannable, len(etcd))
		}
		if container, err = p.config.Store.ReadEtcdContainer(ctx, claim.InstallationID, claim.ClusterID, fresh.ResourceID); err != nil {
			return nil, none, placement.Decision{}, nil, err
		}
	} else {
		// The first kept member whose node holds no etcd service receives the new one.
		index := slices.IndexFunc(kept, func(each member) bool {
			return !slices.ContainsFunc(etcd, func(service postgres.EtcdMember) bool { return service.NodeID == each.NodeID })
		})
		if index < 0 {
			return nil, none, placement.Decision{}, nil, fmt.Errorf("%w: no kept member's node is free for the etcd service", ErrUnplannable)
		}
		address, err := netip.ParseAddr(stored[index].GetPostgres().GetPatroni().GetAddress())
		if err != nil {
			return nil, none, placement.Decision{}, nil, fmt.Errorf("%w: member %s stores no Patroni address", ErrUnplannable, kept[index].MemberID)
		}
		capabilities, err := p.config.Catalog.Resolve(found.Spec)
		if err != nil {
			return nil, none, placement.Decision{}, nil, err
		}
		architecture, err := p.nodeArchitecture(ctx, claim.InstallationID, kept[index].NodeID)
		if err != nil {
			return nil, none, placement.Decision{}, nil, err
		}
		service := resources{instance: uuid.New(), volume: uuid.New(), reservationID: uuid.New()}
		group := placement.Group{ID: "etcd", DomainKey: "host", Members: []uuid.UUID{service.instance}}
		for _, each := range etcd {
			group.Members = append(group.Members, each.ResourceID)
		}
		request = placement.Request{
			Version: 1, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID,
			OperationID: claim.OperationID, DesiredRevision: op.DesiredRevision, Profile: "ha", PortRange: p.config.EndpointPorts,
			Resources: []placement.Resource{{
				ID: service.instance, ReservationID: service.reservationID, OwnedResourceIDs: []uuid.UUID{service.instance, service.volume},
				Kind: "service", Role: "etcd", NodeAffinity: kept[index].NodeID,
				Engine: string(capabilities.Combination.Engine), Version: capabilities.Combination.Version,
				Architecture: architecture, RuntimeVersion: p.config.Profile.RuntimeVersion, StorageID: p.config.Profile.StorageID,
				Budgets: placement.Budgets{Workload: &etcdBudget, Supporting: &placement.Vector{}, Backup: &placement.Vector{}},
				Ports: []placement.Port{
					{Address: wildcardAddress, Transport: "tcp", Name: "dcs_client"},
					{Address: wildcardAddress, Transport: "tcp", Name: "dcs_peer"},
				},
			}},
			Groups: []placement.Group{group},
		}
		if decision, _, err = p.config.Decider.Decide(ctx, request); err != nil {
			return nil, none, placement.Decision{}, nil, err
		}
		if len(decision.Assignments) != 1 || decision.Assignments[0].NodeID != kept[index].NodeID {
			return nil, none, placement.Decision{}, nil, fmt.Errorf("%w: placement did not keep the etcd service on node %s", placement.ErrUnsatisfied, kept[index].NodeID)
		}
		var client, peer uint16
		for _, port := range decision.Assignments[0].Ports {
			switch port.Name {
			case "dcs_client":
				client = port.Number
			case "dcs_peer":
				peer = port.Number
			}
		}
		native := p.config.Patroni
		material, err := enginepostgres.IssuePatroniMemberMaterial(native.CA, pki.PatroniIdentity{InstallationID: claim.InstallationID, ClusterID: claim.ClusterID, ResourceID: service.instance}, address, time.Now(), patroniLeafLifetime)
		if err != nil {
			return nil, none, placement.Decision{}, nil, err
		}
		encoded, err := json.Marshal(material)
		if err != nil {
			return nil, none, placement.Decision{}, nil, err
		}
		expires, err := material.Expiry()
		if err != nil {
			return nil, none, placement.Decision{}, nil, err
		}
		leaf := postgres.Credential{CredentialID: uuid.New(), MemberID: service.instance, Version: 1, Purpose: postgres.PatroniMemberTLSCredential, ExpiresAt: expires}
		leaf.Envelope, err = p.config.Keyring.Seal(secret.Binding{
			InstallationID: claim.InstallationID, OwnerType: "cluster", OwnerID: claim.ClusterID,
			CredentialID: leaf.CredentialID, Version: leaf.Version,
		}, encoded)
		if err != nil {
			return nil, none, placement.Decision{}, nil, err
		}
		intent = &postgres.ClusterIntent{InstallationID: claim.InstallationID, ClusterID: claim.ClusterID, Credentials: []postgres.Credential{leaf}}
		if err := p.config.Store.RecordVolumeIdentity(ctx, postgres.VolumeIdentity{
			InstallationID: claim.InstallationID, NodeID: kept[index].NodeID, ClusterID: claim.ClusterID,
			ResourceID: service.volume, Engine: string(capabilities.Combination.Engine), EngineVersion: capabilities.Combination.Version,
			DataPath: "/data", CapacityRequestBytes: etcdBudget.DiskBytes, Retention: postgres.Retained,
		}); err != nil {
			return nil, none, placement.Decision{}, nil, err
		}
		fresh = postgres.EtcdMember{ResourceID: service.instance, NodeID: kept[index].NodeID}
		container = proto.Clone(previous).(*agentv1.EnsureContainer)
		container.VolumeId = service.volume.String()
		container.Etcd.Name, container.Etcd.Address = "etcd-"+service.instance.String(), address.String()
		container.Etcd.ClientPort, container.Etcd.PeerPort, container.Etcd.InitialClusterState = uint32(client), uint32(peer), "existing"
		container.Etcd.Credentials = []*agentv1.SecretReference{{CredentialId: leaf.CredentialID.String(), Version: leaf.Version, Purpose: leaf.Purpose}}
		// The new service joins every current member.
		peers := []string{container.Etcd.Name + "=https://" + net.JoinHostPort(address.String(), strconv.Itoa(int(peer)))}
		for _, each := range etcd {
			current, err := p.config.Store.ReadEtcdContainer(ctx, claim.InstallationID, claim.ClusterID, each.ResourceID)
			if err != nil {
				return nil, none, placement.Decision{}, nil, err
			}
			peers = append(peers, current.Etcd.Name+"=https://"+net.JoinHostPort(current.Etcd.Address, strconv.Itoa(int(current.Etcd.PeerPort))))
		}
		container.Etcd.InitialCluster = strings.Join(peers, ",")
	}
	// Membership changes go through an established service that stays.
	through := slices.IndexFunc(etcd, func(each postgres.EtcdMember) bool {
		return each.ResourceID != old.ResourceID && each.ResourceID != fresh.ResourceID
	})
	if through < 0 {
		return nil, none, placement.Decision{}, nil, fmt.Errorf("%w: no established etcd service remains", ErrUnplannable)
	}
	established, err := p.config.Store.ReadEtcdContainer(ctx, claim.InstallationID, claim.ClusterID, etcd[through].ResourceID)
	if err != nil {
		return nil, none, placement.Decision{}, nil, err
	}
	credential := established.Etcd.Credentials[0]
	id, err := uuid.Parse(credential.CredentialId)
	if err != nil {
		return nil, none, placement.Decision{}, nil, err
	}
	// The established service runs its latest leaf after a renewal.
	latest, err := p.config.Store.ReadLatestCredentialVersion(ctx, claim.InstallationID, claim.ClusterID, id)
	if err != nil {
		return nil, none, placement.Decision{}, nil, err
	}
	leaf := &agentv1.SecretReference{CredentialId: credential.CredentialId, Version: latest, Purpose: credential.Purpose}
	membership := func(action, name, url string) *agentv1.CommandInput {
		return &agentv1.CommandInput{ResourceId: etcd[through].ResourceID.String(), Secrets: []*agentv1.SecretReference{leaf},
			Payload: &agentv1.CommandInput_EtcdMembership{EtcdMembership: &agentv1.EtcdMembership{
				Address: established.Etcd.Address, ClientPort: established.Etcd.ClientPort, Action: action, Name: name, PeerUrl: url}}}
	}
	peerURL := "https://" + net.JoinHostPort(container.Etcd.Address, strconv.Itoa(int(container.Etcd.PeerPort)))
	at, via, from := epochFor(claim, fresh.NodeID), epochFor(claim, etcd[through].NodeID), epochFor(claim, old.NodeID)
	grace := durationpb.New(p.config.StopGrace)
	actions := []struct {
		epoch         *agentv1.Epoch
		input         *agentv1.CommandInput
		postcondition agentv1.Postcondition
		boundary      agentv1.CancellationBoundary
	}{
		{at, &agentv1.CommandInput{ResourceId: container.VolumeId, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: container.VolumeId}}}, agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS, beforeExecution},
		{at, &agentv1.CommandInput{ResourceId: fresh.ResourceID.String(), Secrets: container.Etcd.Credentials, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: container}}, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, beforeExecution},
		{via, membership("add_learner", container.Etcd.Name, peerURL), agentv1.Postcondition_POSTCONDITION_ETCD_MEMBERSHIP, beforeExecution},
		{at, &agentv1.CommandInput{ResourceId: fresh.ResourceID.String(), Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, beforeExecution},
		{via, membership("promote", container.Etcd.Name, ""), agentv1.Postcondition_POSTCONDITION_ETCD_MEMBERSHIP, beforeExecution},
		{via, membership("remove", previous.Etcd.Name, ""), agentv1.Postcondition_POSTCONDITION_ETCD_MEMBERSHIP, beforeExecution},
		{from, &agentv1.CommandInput{ResourceId: old.ResourceID.String(), Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: grace}}}, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP},
		{from, &agentv1.CommandInput{ResourceId: old.ResourceID.String(), Payload: &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}}, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, beforeExecution},
	}
	var steps []*agentv1.PlanStep
	for _, each := range actions {
		step, err := p.step(claim, each.epoch, op.DesiredRevision, each.input, each.postcondition, []string{after}, each.boundary)
		if err != nil {
			return nil, none, placement.Decision{}, nil, err
		}
		steps, after = append(steps, step), step.Command.StepId
	}
	return steps, request, decision, intent, nil
}
