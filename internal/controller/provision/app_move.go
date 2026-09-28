package provision

import (
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func (p *Planner) moveApp(ctx context.Context, claim operation.Claim, op operation.Operation) error {
	move, err := p.config.Store.GetAppMove(ctx, claim.InstallationID, claim.OperationID)
	if err != nil {
		return err
	}
	found, err := p.config.Store.GetApp(ctx, claim.InstallationID, claim.AppID)
	if err != nil {
		return err
	}
	deployment, err := p.config.Store.ReadAppDeployment(ctx, claim.InstallationID, claim.AppID)
	if err != nil {
		return err
	}
	if deployment == nil {
		return ErrNoWorkload
	}
	var original *agentv1.EnsureContainer
	for _, step := range deployment.Steps {
		if step.Command.Input.GetEnsureContainer().GetGeneric() != nil {
			original = step.Command.Input.GetEnsureContainer()
			break
		}
	}
	if original == nil {
		return ErrNoWorkload
	}
	service := found.Spec.Service
	service.Image = original.ImageDigest
	resolved, err := p.config.ResolveImage(ctx, service)
	if err != nil {
		return err
	}
	if resolved.LocalOnly || resolved.Service.Image != original.ImageDigest {
		return fmt.Errorf("move image differs from the deployed digest")
	}
	copies, err := p.config.Store.GetAllAppCopies(ctx, claim.InstallationID, claim.AppID)
	if err != nil {
		return err
	}
	var source, target app.Copy
	nextIndex := 0
	for _, copy := range copies {
		if copy.CopyIndex >= nextIndex {
			nextIndex = copy.CopyIndex + 1
		}
		if copy.NodeID == move.SourceNodeID && copy.Role == "active" {
			source = copy
		}
		if copy.NodeID == move.TargetNodeID {
			target = copy
		}
	}
	if source.ID == uuid.Nil {
		return ErrNoWorkload
	}
	if found.Tier == app.TierDatabase && target.ID == uuid.Nil {
		return ErrNoWorkload
	}
	if target.ID == uuid.Nil {
		target = app.Copy{ID: uuid.New(), NodeID: move.TargetNodeID, CopyIndex: nextIndex}
	}
	var secrets []*agentv1.SecretReference
	if found.Tier == app.TierDatabase {
		original = nil
		for _, step := range deployment.Steps {
			if step.Command.Input.ResourceId == target.ID.String() && step.Command.Epoch.NodeId == target.NodeID.String() && step.Command.Input.GetEnsureContainer() != nil {
				original = step.Command.Input.GetEnsureContainer()
				secrets = proto.Clone(step.Command.Input).(*agentv1.CommandInput).Secrets
				break
			}
		}
		if original == nil {
			return ErrNoWorkload
		}
	}
	snapshot, err := p.config.Store.ReadPlacementSnapshot(ctx, claim.InstallationID)
	if err != nil {
		return err
	}
	disk := service.Resources.DiskBytes
	if disk == 0 {
		disk = 1 << 30
	}
	resource := placement.Resource{ID: target.ID, ReservationID: uuid.New(), OwnedResourceIDs: []uuid.UUID{target.ID}, Kind: "service", Role: "active", Engine: "generic", NodeAffinity: target.NodeID, SupportedArchitectures: resolved.Service.SupportedArchitectures, Ports: []placement.Port{{Name: "backend", Address: "0.0.0.0", Transport: "tcp"}}, Budgets: placement.Budgets{Workload: &placement.Vector{CPUMillis: original.CpuMillis, MemoryBytes: original.MemoryBytes, DiskBytes: disk}, Supporting: &placement.Vector{}, Backup: &placement.Vector{}}}
	for _, allocation := range snapshot.Allocations {
		if allocation.ResourceID == target.ID && !allocation.Released {
			resource.ReservationID = allocation.ID
			resource.OwnedResourceIDs = allocation.OwnedResourceIDs
			resource.Ports = allocation.Ports
			resource.Budgets.Workload.DiskBytes = allocation.Budget.DiskBytes
		}
	}
	for _, mount := range service.Mounts {
		if mount.Type == "bind" {
			resource.RequiredPaths = append(resource.RequiredPaths, mount.Source)
		}
	}
	for _, node := range snapshot.Nodes {
		if node.Policy == nil {
			continue
		}
		for _, name := range found.Spec.DevicePreference.Never {
			if name == node.ID.String() || name == node.Policy.Labels["synod.name"] {
				resource.ExcludedNodes = append(resource.ExcludedNodes, node.ID)
			}
		}
		for _, name := range append(slices.Clone(found.Spec.DevicePreference.Allowed), found.Spec.DevicePreference.Preferred...) {
			if name == node.ID.String() || name == node.Policy.Labels["synod.name"] {
				resource.AllowedNodes = append(resource.AllowedNodes, node.ID)
				break
			}
		}
	}
	if len(found.Spec.DevicePreference.Allowed) == 0 || slices.Contains(found.Spec.DevicePreference.Allowed, "*") {
		resource.AllowedNodes = nil
	} else if len(resource.AllowedNodes) == 0 {
		return placement.ErrUnsatisfied
	}
	presence, err := p.config.Store.GetAgentPresence(ctx, claim.InstallationID, target.NodeID)
	if err != nil {
		return err
	}
	if !slices.Contains(presence.Hello.Capabilities, "runtime.docker") || !slices.Contains(presence.Hello.Capabilities, replication.Capability) {
		return placement.ErrUnsatisfied
	}
	nodeIndex := slices.IndexFunc(snapshot.Nodes, func(n placement.Node) bool { return n.ID == target.NodeID })
	if nodeIndex < 0 || snapshot.Nodes[nodeIndex].Policy == nil || snapshot.Nodes[nodeIndex].Policy.ClientAddress == "" {
		return placement.ErrUnsatisfied
	}
	request := placement.Request{Version: 1, InstallationID: claim.InstallationID, AppID: claim.AppID, OperationID: claim.OperationID, DesiredRevision: op.DesiredRevision, Profile: "compact", PortRange: p.config.EndpointPorts, Resources: []placement.Resource{resource}}
	decision, _, err := p.config.Decider.Decide(ctx, request)
	if err != nil {
		return err
	}
	assigned := decision.Assignments[0]
	container := proto.Clone(original).(*agentv1.EnsureContainer)
	container.Generic.CopyIndex = uint32(target.CopyIndex)
	container.Ports = []*agentv1.PortBinding{{ContainerPort: original.Ports[0].ContainerPort, HostPort: uint32(assigned.Ports[0].Number), HostAddress: assigned.Ports[0].Address, Transport: assigned.Ports[0].Transport}}
	var steps []*agentv1.PlanStep
	var final *agentv1.PlanStep
	if found.Tier == app.TierDatabase {
		final, err = p.step(claim, epochFor(claim, source.NodeID), op.DesiredRevision, &agentv1.CommandInput{ResourceId: source.ID.String(), Payload: &agentv1.CommandInput_SwitchDatabase{SwitchDatabase: &agentv1.SwitchDatabase{WriterEpoch: move.WriterEpoch, TargetNodeId: target.NodeID.String(), TargetHost: snapshot.Nodes[nodeIndex].Policy.ClientAddress, TargetPort: uint32(target.HostPort)}}}, agentv1.Postcondition_POSTCONDITION_DATABASE_SWITCHOVER_READY, nil, beforeExecution)
	} else {
		steps, err = p.stopSteps(claim, op, []member{{NodeID: source.NodeID, ResourceID: source.ID}})
		if err != nil {
			return err
		}
		final, err = p.step(claim, epochFor(claim, source.NodeID), op.DesiredRevision, &agentv1.CommandInput{ResourceId: source.ID.String(), Payload: &agentv1.CommandInput_FinalAppSnapshot{FinalAppSnapshot: &agentv1.FinalAppSnapshot{WriterEpoch: move.WriterEpoch, TargetNodeId: target.NodeID.String()}}}, agentv1.Postcondition_POSTCONDITION_APP_SNAPSHOT_TRANSFERRED, []string{steps[0].Command.StepId}, beforeExecution)
	}
	if err != nil {
		return err
	}
	ensure, err := p.step(claim, epochFor(claim, target.NodeID), op.DesiredRevision, &agentv1.CommandInput{ResourceId: target.ID.String(), Secrets: secrets, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: container}}, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, []string{final.Command.StepId}, beforeExecution)
	if err != nil {
		return err
	}
	start, err := p.step(claim, epochFor(claim, target.NodeID), op.DesiredRevision, &agentv1.CommandInput{ResourceId: target.ID.String(), Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, []string{ensure.Command.StepId}, beforeExecution)
	if err != nil {
		return err
	}
	steps = append(steps, final, ensure, start)
	_, err = p.config.Store.InstallPlacementPlan(ctx, claim, request, decision, &agentv1.ExecutionPlan{Version: 1, Steps: steps})
	return err
}
