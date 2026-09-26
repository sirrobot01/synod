package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
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

// rebuildMember replaces the reviewed Patroni member with a fresh one on the same node, or
// resumes a residual member that a failed rebuild left.
//
// The other members must prove readiness first, so the cluster keeps a writer. An
// active target must prove that it does not serve and that its storage cannot restart
// before it is removed. A residual target is resumed when its latest start never ran or
// reached running. Otherwise it must prove its failure before it is removed. The fresh
// member keeps the address, peers, and DCS. It gets new storage, ports, and a member
// certificate, and Patroni clones it from the current leader.
func (p *Planner) rebuildMember(ctx context.Context, claim operation.Claim, op operation.Operation) error {
	native := p.config.Patroni
	if native == nil {
		return errors.New("Patroni HA is not configured on this controller")
	}
	// An installed plan holds its reservation and certificate. A repeat must not make new ones.
	if op.PlanVersion != 0 {
		return nil
	}
	found, err := p.config.Store.GetCluster(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return err
	}
	if found.Spec.Engine != cluster.PostgreSQL || found.Spec.PostgreSQL == nil || found.Spec.PostgreSQL.Topology != "patroni" {
		return ErrUnplannable
	}
	running, err := p.members(ctx, claim)
	if err != nil {
		return err
	}
	reviewed, action, err := p.config.Store.ReadReplicaActionTarget(ctx, claim.InstallationID, claim.OperationID)
	if err != nil {
		return err
	}
	if action != "rebuild_member" || len(running) < operation.MinPatroniMembers || len(running) > operation.MaxPatroniMembers {
		return ErrUnplannable
	}
	var target member
	var others []member
	for _, each := range running {
		if each.MemberID == reviewed {
			target = each
		} else {
			others = append(others, each)
		}
	}
	if target.MemberID == uuid.Nil {
		return operation.ErrCommandConflict
	}
	for _, each := range running {
		if each.Role != "patroni" || each.State != "active" && (each.MemberID != target.MemberID || each.State != "residual") {
			return fmt.Errorf("%w: member %s is %s %s", ErrUnplannable, each.MemberID, each.State, each.Role)
		}
	}
	application, err := p.config.Store.ReadClusterCredential(ctx, claim.InstallationID, claim.ClusterID, postgres.ApplicationCredential)
	if err != nil {
		return err
	}
	readiness := func(each member, container *agentv1.EnsureContainer, after []string) (*agentv1.PlanStep, error) {
		return p.patroniReadiness(claim, op, application, each, container, "any", after)
	}
	var steps []*agentv1.PlanStep
	var ready []string
	// The fresh member's leaf names the other members, because their routers forward to it.
	var peers []netip.Addr
	// A routed cluster also refreshes its routes after the member steps.
	routedCluster := routed(running)
	var otherRoutes []routeMember
	for _, each := range others {
		container, err := p.config.Store.ReadMemberContainer(ctx, claim.InstallationID, claim.ClusterID, each.ResourceID)
		if err != nil {
			return err
		}
		peer, err := netip.ParseAddr(container.GetPostgres().GetPatroni().GetAddress())
		if err != nil {
			return fmt.Errorf("%w: member %s stores no Patroni address", ErrUnplannable, each.MemberID)
		}
		peers = append(peers, peer)
		if routedCluster {
			route, err := routeMemberFrom(each, container)
			if err != nil {
				return err
			}
			otherRoutes = append(otherRoutes, route)
		}
		check, err := readiness(each, container, nil)
		if err != nil {
			return err
		}
		steps, ready = append(steps, check), append(ready, check.Command.StepId)
	}
	stored, err := p.config.Store.ReadMemberContainer(ctx, claim.InstallationID, claim.ClusterID, target.ResourceID)
	if err != nil {
		return err
	}
	address, err := netip.ParseAddr(stored.GetPostgres().GetPatroni().GetAddress())
	if err != nil {
		return fmt.Errorf("%w: member %s stores no Patroni address", ErrUnplannable, target.MemberID)
	}
	epoch := epochFor(claim, target.NodeID)
	// build creates the volume, container, start, and check of one member after the given steps.
	build := func(each member, configuration *agentv1.EnsureContainer, after []string, initialize bool) ([]*agentv1.PlanStep, error) {
		epoch := epochFor(claim, each.NodeID)
		volume, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: configuration.VolumeId,
			Payload:    &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: configuration.VolumeId}},
		}, agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS, after, beforeExecution)
		if err != nil {
			return nil, err
		}
		container, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(), Secrets: configuration.Postgres.Patroni.Credentials,
			Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: configuration},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, []string{volume.Command.StepId}, beforeExecution)
		if err != nil {
			return nil, err
		}
		start, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(),
			Payload:    &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: configuration, AllowInitialization: initialize}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, []string{container.Command.StepId}, beforeExecution)
		if err != nil {
			return nil, err
		}
		check, err := readiness(each, configuration, []string{start.Command.StepId})
		if err != nil {
			return nil, err
		}
		return []*agentv1.PlanStep{volume, container, start, check}, nil
	}
	var proof *agentv1.CommandInput
	switch {
	case target.State == "active":
		proof = &agentv1.CommandInput{ResourceId: target.ResourceID.String(),
			Payload: &agentv1.CommandInput_VerifyPatroniMemberFailed{VerifyPatroniMemberFailed: &agentv1.VerifyPatroniMemberFailed{Container: stored}}}
		notServing, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: target.ResourceID.String(),
			Payload:    &agentv1.CommandInput_VerifyPatroniMemberNotServing{VerifyPatroniMemberNotServing: &agentv1.VerifyPatroniMemberNotServing{Container: stored}},
		}, agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_NOT_SERVING, ready, beforeExecution)
		if err != nil {
			return err
		}
		steps, ready = append(steps, notServing), []string{notServing.Command.StepId}
	default:
		history, err := p.config.Store.ReadPatroniStartHistory(ctx, claim.InstallationID, claim.ClusterID, target.ResourceID)
		if err != nil {
			return err
		}
		if history.Resume() {
			// The residual member keeps its reservation, storage, and certificate. A start that
			// never ran may still initialize. After a run, only a restart of completed storage is safe.
			resumed, err := build(target, stored, ready, !history.Executed)
			if err != nil {
				return err
			}
			steps = append(steps, resumed...)
			if routedCluster {
				route, err := routeMemberFrom(target, stored)
				if err != nil {
					return err
				}
				tail, err := p.rebuildRouteTail(claim, op, nil, append(otherRoutes, route), target.ResourceID, application, steps[len(steps)-1].Command.StepId)
				if err != nil {
					return err
				}
				steps = append(steps, tail...)
			}
			plan := &agentv1.ExecutionPlan{Version: 1, Steps: steps}
			if err := operation.ValidatePlan(plan, op); err != nil {
				return err
			}
			return p.config.Store.InstallPlan(ctx, claim, plan)
		}
		// Storage that completed once can only fail a restart. Storage that never completed
		// can only fail its first start.
		proof = &agentv1.CommandInput{ResourceId: target.ResourceID.String(),
			Payload: &agentv1.CommandInput_VerifyPatroniStartFailure{VerifyPatroniStartFailure: &agentv1.VerifyPatroniStartFailure{Container: stored}}}
		if history.EverRunning {
			proof.Payload = &agentv1.CommandInput_VerifyPatroniMemberFailed{VerifyPatroniMemberFailed: &agentv1.VerifyPatroniMemberFailed{Container: stored}}
		}
	}
	stop, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: target.ResourceID.String(),
		Payload:    &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(p.config.StopGrace)}},
	}, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, ready, agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP)
	if err != nil {
		return err
	}
	postcondition := agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_FAILED
	if proof.GetVerifyPatroniStartFailure() != nil {
		postcondition = agentv1.Postcondition_POSTCONDITION_PATRONI_START_FAILED
	}
	storageFailed, err := p.step(claim, epoch, op.DesiredRevision, proof, postcondition, []string{stop.Command.StepId}, beforeExecution)
	if err != nil {
		return err
	}
	remove, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: target.ResourceID.String(),
		Payload:    &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}},
	}, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, []string{stop.Command.StepId, storageFailed.Command.StepId}, beforeExecution)
	if err != nil {
		return err
	}
	steps = append(steps, stop, storageFailed, remove)

	// The fresh member stays on the target's node and host domain. The others keep
	// their reservations, and the group keeps every member on a distinct host.
	capabilities, err := p.config.Catalog.Resolve(found.Spec)
	if err != nil {
		return err
	}
	fresh := []patroniMember{{resources: resources{instance: uuid.New(), volume: uuid.New(), reservationID: uuid.New()}}}
	// The fresh member stays on the target's node, so it keeps that node's architecture.
	architecture, err := p.nodeArchitecture(ctx, claim.InstallationID, target.NodeID)
	if err != nil {
		return err
	}
	request := p.patroniRequest(claim, found, capabilities.Combination, fresh, architecture)
	request.DesiredRevision = op.DesiredRevision
	request.Resources[0].NodeAffinity = target.NodeID
	for _, each := range others {
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
	if created.node != target.NodeID || created.address != address {
		return fmt.Errorf("%w: node %s no longer declares member address %s", placement.ErrUnsatisfied, target.NodeID, address)
	}
	endpoints, err := patroniEndpoints(found, address, peers)
	if err != nil {
		return err
	}
	material, err := enginepostgres.IssuePatroniMemberMaterial(native.CA, pki.PatroniIdentity{InstallationID: claim.InstallationID, ClusterID: claim.ClusterID, ResourceID: created.instance}, address, time.Now(), patroniLeafLifetime, endpoints...)
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
	intent := postgres.ClusterIntent{InstallationID: claim.InstallationID, ClusterID: claim.ClusterID, Credentials: []postgres.Credential{certificate}}
	// The stored configuration carries the cluster settings. Only the member's own storage,
	// ports, provision, and certificate change.
	configuration := proto.Clone(stored).(*agentv1.EnsureContainer)
	configuration.VolumeId = created.volume.String()
	configuration.Postgres.Port = uint32(created.database)
	patroni := configuration.Postgres.Patroni
	patroni.RestPort, patroni.ProvisionId, patroni.AllowInitialization = uint32(created.rest), uuid.NewString(), true
	if patroni.DcsEndpoints, err = p.dcsEndpoints(ctx, claim, patroni.DcsEndpoints); err != nil {
		return err
	}
	replaced := false
	for index, reference := range patroni.Credentials {
		if reference.Purpose == postgres.PatroniMemberTLSCredential {
			patroni.Credentials[index] = &agentv1.SecretReference{CredentialId: certificate.CredentialID.String(), Version: certificate.Version, Purpose: certificate.Purpose}
			replaced = true
		}
	}
	if !replaced {
		return fmt.Errorf("%w: member %s stores no member certificate", ErrUnplannable, target.MemberID)
	}
	built, err := build(member{MemberID: created.instance, NodeID: created.node, ResourceID: created.instance}, configuration, []string{remove.Command.StepId}, true)
	if err != nil {
		return err
	}
	steps = append(steps, built...)
	if routedCluster {
		route := routeMember{node: created.node, resource: created.instance, address: address, database: created.database, rest: created.rest,
			pooler: created.pooler, writer: created.writer, reader: created.reader,
			certificate: &agentv1.SecretReference{CredentialId: certificate.CredentialID.String(), Version: certificate.Version, Purpose: certificate.Purpose}}
		tail, err := p.rebuildRouteTail(claim, op, []member{target}, append(otherRoutes, route), created.instance, application, steps[len(steps)-1].Command.StepId)
		if err != nil {
			return err
		}
		steps = append(steps, tail...)
	}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: steps}
	if err := operation.ValidatePlan(plan, op); err != nil {
		return err
	}
	if err := p.config.Store.RecordVolumeIdentity(ctx, postgres.VolumeIdentity{
		InstallationID: claim.InstallationID, NodeID: created.node, ClusterID: claim.ClusterID,
		ResourceID: created.volume, Engine: string(capabilities.Combination.Engine), EngineVersion: capabilities.Combination.Version,
		DataPath: dataPath(found.Spec.Engine), CapacityRequestBytes: p.config.Budget.DiskBytes, Retention: postgres.Retained,
	}); err != nil {
		return err
	}
	_, err = p.config.Store.InstallPlacementPlan(ctx, claim, request, decision, plan, &intent)
	return err
}
