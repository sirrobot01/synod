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

func (p *Planner) failoverApp(ctx context.Context, claim operation.Claim, op operation.Operation) error {
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
	database := found.Tier == app.TierDatabase
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
		return fmt.Errorf("failover image differs from the deployed digest")
	}
	candidates, err := p.config.Store.AppFailoverCandidates(ctx, claim.InstallationID, claim.AppID)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return fmt.Errorf("no_replica")
	}
	copies, err := p.config.Store.GetAppCopies(ctx, claim.InstallationID, claim.AppID)
	if err != nil {
		return err
	}
	snapshot, err := p.config.Store.ReadPlacementSnapshot(ctx, claim.InstallationID)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		var resource placement.Resource
		for _, allocation := range snapshot.Allocations {
			if allocation.ResourceID != candidate.CopyID || allocation.Released {
				continue
			}
			resource = placement.Resource{ID: allocation.ResourceID, ReservationID: allocation.ID, OwnedResourceIDs: allocation.OwnedResourceIDs, Kind: "service", Role: "active", Engine: "generic", SupportedArchitectures: resolved.Service.SupportedArchitectures, NodeAffinity: candidate.NodeID, Ports: allocation.Ports,
				Budgets: placement.Budgets{Workload: &placement.Vector{CPUMillis: original.CpuMillis, MemoryBytes: original.MemoryBytes, DiskBytes: allocation.Budget.DiskBytes}, Supporting: &placement.Vector{}, Backup: &placement.Vector{}}}
		}
		if resource.ID == candidate.CopyID {
			for _, mount := range service.Mounts {
				if mount.Type == "bind" {
					resource.RequiredPaths = append(resource.RequiredPaths, mount.Source)
				}
			}
			request := placement.Request{Version: 1, InstallationID: claim.InstallationID, AppID: claim.AppID, OperationID: claim.OperationID, DesiredRevision: op.DesiredRevision, Profile: "compact", Resources: []placement.Resource{resource}}
			decision, _, err := p.config.Decider.Decide(ctx, request)
			if err != nil {
				continue
			}
			writer, err := p.config.Store.GetAppWriter(ctx, claim.InstallationID, claim.AppID)
			if err != nil {
				return err
			}
			oldOnline := false
			for _, node := range snapshot.Nodes {
				if node.ID == writer.NodeID {
					oldOnline = !node.Revoked && !node.RecoveryRequired && node.Policy != nil && !node.Policy.Maintenance && node.LastSeenAt.Add(placement.PresenceAge).After(snapshot.ObservedAt)
				}
			}
			// A tier 1 old primary rejoins as a standby when it returns, so no replacement is placed.
			if !oldOnline && !database {
				standby := resource
				standby.ID, standby.ReservationID = uuid.New(), uuid.New()
				standby.OwnedResourceIDs = []uuid.UUID{standby.ID}
				standby.Role, standby.NodeAffinity = "standby", uuid.Nil
				standby.Budgets = placement.Budgets{Workload: &placement.Vector{DiskBytes: resource.Budgets.Workload.DiskBytes}, Supporting: &placement.Vector{}, Backup: &placement.Vector{}}
				standby.Ports = []placement.Port{{Name: "backend", Address: "0.0.0.0", Transport: "tcp"}}
				for _, copy := range copies {
					standby.ExcludedNodes = append(standby.ExcludedNodes, copy.NodeID)
				}
				// Replacement uses the same device policy as the admitted app.
				for _, node := range snapshot.Nodes {
					if node.Policy == nil {
						continue
					}
					for _, name := range found.Spec.DevicePreference.Never {
						if (name == node.ID.String() || name == node.Policy.Labels["synod.name"]) && !slices.Contains(standby.ExcludedNodes, node.ID) {
							standby.ExcludedNodes = append(standby.ExcludedNodes, node.ID)
						}
					}
					for _, name := range append(slices.Clone(found.Spec.DevicePreference.Allowed), found.Spec.DevicePreference.Preferred...) {
						if name == node.ID.String() || name == node.Policy.Labels["synod.name"] {
							standby.AllowedNodes = append(standby.AllowedNodes, node.ID)
							break
						}
					}
					presence, err := p.config.Store.GetAgentPresence(ctx, claim.InstallationID, node.ID)
					if err != nil {
						return err
					}
					if (node.Policy.ClientAddress == "" || !slices.Contains(presence.Hello.Capabilities, "runtime.docker") || !slices.Contains(presence.Hello.Capabilities, replication.Capability)) && !slices.Contains(standby.ExcludedNodes, node.ID) {
						standby.ExcludedNodes = append(standby.ExcludedNodes, node.ID)
					}
				}
				if len(found.Spec.DevicePreference.Allowed) == 0 || slices.Contains(found.Spec.DevicePreference.Allowed, "*") {
					standby.AllowedNodes = nil
				}
				trial := request
				trial.PortRange = p.config.EndpointPorts
				trial.Resources = append(slices.Clone(request.Resources), standby)
				trial.Groups = []placement.Group{{ID: "app-copies", DomainKey: "host", Soft: true, Members: []uuid.UUID{resource.ID, standby.ID}}}
				if chosen, _, err := p.config.Decider.Decide(ctx, trial); err == nil {
					request, decision = trial, chosen
				}
			}
			container := proto.Clone(original).(*agentv1.EnsureContainer)
			var secrets []*agentv1.SecretReference
			for _, copy := range copies {
				if copy.ID == candidate.CopyID {
					container.Generic.CopyIndex = uint32(copy.CopyIndex)
				}
			}
			container.Ports = []*agentv1.PortBinding{{ContainerPort: original.Ports[0].ContainerPort, HostPort: uint32(resource.Ports[0].Number), HostAddress: resource.Ports[0].Address, Transport: resource.Ports[0].Transport}}
			if database {
				// A tier 1 standby already runs its own engine container. Promotion keeps it.
				at := slices.IndexFunc(deployment.Steps, func(step *agentv1.PlanStep) bool {
					return step.Command.Input.ResourceId == candidate.CopyID.String() && step.Command.Input.GetEnsureContainer() != nil
				})
				if at < 0 {
					continue
				}
				container = proto.Clone(deployment.Steps[at].Command.Input.GetEnsureContainer()).(*agentv1.EnsureContainer)
				secrets = deployment.Steps[at].Command.Input.Secrets
			}
			ensure, err := p.step(claim, epochFor(claim, candidate.NodeID), op.DesiredRevision, &agentv1.CommandInput{ResourceId: candidate.CopyID.String(), Secrets: secrets, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: container}}, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, nil, beforeExecution)
			if err != nil {
				return err
			}
			start, err := p.step(claim, epochFor(claim, candidate.NodeID), op.DesiredRevision, &agentv1.CommandInput{ResourceId: candidate.CopyID.String(), Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, []string{ensure.Command.StepId}, beforeExecution)
			if err != nil {
				return err
			}
			_, err = p.config.Store.InstallPlacementPlan(ctx, claim, request, decision, &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{ensure, start}})
			return err
		}
	}
	return placement.ErrUnsatisfied
}
